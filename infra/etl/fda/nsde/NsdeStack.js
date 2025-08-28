const cdk = require("aws-cdk-lib");
const s3 = require("aws-cdk-lib/aws-s3");
const s3Deploy = require("aws-cdk-lib/aws-s3-deployment");
const lambda = require("aws-cdk-lib/aws-lambda");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const sfn = require("aws-cdk-lib/aws-stepfunctions");
const tasks = require("aws-cdk-lib/aws-stepfunctions-tasks");
const path = require("path");

class PpDwEtlStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Load warehouse and dataset configurations
    const warehouseConfig = require("../../config/warehouse.json");
    const datasetConfig = require("./config/dataset.json");
    const dataset = datasetConfig.dataset;
    
    // Construct resource names from warehouse conventions
    const resourceNames = {
      bronzeJob: `${warehouseConfig.warehouse_prefix}-bronze-${dataset}`,
      silverJob: `${warehouseConfig.warehouse_prefix}-silver-${dataset}`,
      bronzeCrawler: `${warehouseConfig.warehouse_prefix}-bronze-${dataset}-crawler`,
      fetchLambda: `${warehouseConfig.warehouse_prefix}-raw-fetch-${dataset}`,
      bucket: warehouseConfig.bucket_name_pattern.replace('{account}', this.account)
    };

    // Single data warehouse bucket with prefix-based organization
    const dataWarehouseBucket = new s3.Bucket(this, "DataWarehouseBucket", {
      bucketName: resourceNames.bucket,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          id: "temp-cleanup",
          prefix: "temp/",
          expiration: cdk.Duration.days(7),
        },
      ],
    });

    // Simple Glue service role
    const glueRole = new iam.Role(this, "GlueRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
      ],
    });

    // Grant S3 access to Glue for data warehouse bucket
    dataWarehouseBucket.grantReadWrite(glueRole);

    // Glue bronze database for Athena queries (shared across all datasets)
    const bronzeDatabase = new glue.CfnDatabase(this, "BronzeDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: warehouseConfig.bronze_database,
        description: "Bronze layer database for all datasets"
      }
    });

    // Glue silver database for Athena queries (shared across all datasets)
    const silverDatabase = new glue.CfnDatabase(this, "SilverDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: warehouseConfig.silver_database,
        description: "Silver layer database for all datasets"
      }
    });

    // Simple Lambda execution role
    const lambdaRole = new iam.Role(this, "LambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole"
        ),
      ],
    });

    // Grant S3 access to Lambda for data warehouse bucket
    dataWarehouseBucket.grantReadWrite(lambdaRole);

    // Fetch Lambda function
    const fetchLambda = new lambda.Function(this, "FetchLambda", {
      functionName: resourceNames.fetchLambda,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "app.handler",
      code: lambda.Code.fromAsset(
        path.join(__dirname, "lambdas/fetch")
      ),
      timeout: cdk.Duration.seconds(warehouseConfig.lambda_defaults.timeout_seconds),
      memorySize: warehouseConfig.lambda_defaults.memory_mb,
      role: lambdaRole,
      environment: {
        DATA_WAREHOUSE_BUCKET_NAME: dataWarehouseBucket.bucketName,
        DATASET: dataset,
      },
    });


    // Bronze Glue job
    const bronzeJob = new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${dataWarehouseBucket.bucketName}/scripts/${dataset}/bronze_job.py`,
        pythonVersion: warehouseConfig.glue_defaults.python_version,
      },
      glueVersion: warehouseConfig.glue_defaults.version,
      workerType: warehouseConfig.glue_defaults.worker_type,
      numberOfWorkers: warehouseConfig.glue_defaults.number_of_workers,
      maxRetries: warehouseConfig.glue_defaults.max_retries,
      timeout: warehouseConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
      },
    });

    // Silver Glue job
    const silverJob = new glue.CfnJob(this, "SilverJob", {
      name: resourceNames.silverJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${dataWarehouseBucket.bucketName}/scripts/${dataset}/silver_job.py`,
        pythonVersion: warehouseConfig.glue_defaults.python_version,
      },
      glueVersion: warehouseConfig.glue_defaults.version,
      workerType: warehouseConfig.glue_defaults.worker_type,
      numberOfWorkers: warehouseConfig.glue_defaults.number_of_workers,
      maxRetries: warehouseConfig.glue_defaults.max_retries,
      timeout: warehouseConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
      },
    });

    // Bronze crawler (auto-discovers schema from parquet files)
    const bronzeCrawler = new glue.CfnCrawler(this, "BronzeCrawler", {
      name: resourceNames.bronzeCrawler,
      role: glueRole.roleArn,
      databaseName: warehouseConfig.bronze_database,
      targets: {
        s3Targets: [
          {
            path: `s3://${dataWarehouseBucket.bucketName}/bronze/${dataset}/`
          }
        ]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });

    // Silver crawler (for initial table creation only)
    const silverCrawler = new glue.CfnCrawler(this, "SilverCrawler", {
      name: `${warehouseConfig.warehouse_prefix}-silver-${dataset}-crawler`,
      role: glueRole.roleArn,
      databaseName: warehouseConfig.silver_database,
      targets: {
        s3Targets: [
          {
            path: `s3://${dataWarehouseBucket.bucketName}/silver/${dataset}/`
          }
        ]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });

    // Crawlers depend on databases
    bronzeCrawler.addDependency(bronzeDatabase);
    silverCrawler.addDependency(silverDatabase);

    // Deploy Glue scripts to data warehouse bucket
    new s3Deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3Deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `scripts/${dataset}/`
    });

    // Step Functions workflow for orchestration
    const fetchTask = new tasks.LambdaInvoke(this, "FetchTask", {
      lambdaFunction: fetchLambda,
      payload: sfn.TaskInput.fromObject({
        dataset: datasetConfig.dataset,
        source_url: datasetConfig.source_url
      }),
      outputPath: "$.Payload"
    });

    const bronzeJobTask = new tasks.GlueStartJobRun(this, "BronzeJobTask", {
      glueJobName: resourceNames.bronzeJob,
      arguments: sfn.TaskInput.fromObject({
        "--raw_path": sfn.JsonPath.stringAt("$.raw_path"),
        "--bronze_path": sfn.JsonPath.stringAt("$.bronze_path"), 
        "--run_id": sfn.JsonPath.stringAt("$.run_id"),
        "--dataset": sfn.JsonPath.stringAt("$.dataset")
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      resultPath: "$.bronzeJobResult"
    });

    const silverJobTask = new tasks.GlueStartJobRun(this, "SilverJobTask", {
      glueJobName: resourceNames.silverJob,
      arguments: sfn.TaskInput.fromObject({
        "--bronze_path": sfn.JsonPath.stringAt("$.bronze_path"),
        "--silver_path": sfn.JsonPath.stringAt("$.silver_path"),
        "--run_id": sfn.JsonPath.stringAt("$.run_id"),
        "--dataset": sfn.JsonPath.stringAt("$.dataset")
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      resultPath: "$.silverJobResult"
    });

    const definition = fetchTask.next(bronzeJobTask).next(silverJobTask);

    const stateMachine = new sfn.StateMachine(this, "EtlPipeline", {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      stateMachineName: `${warehouseConfig.warehouse_prefix}-pipeline-${dataset}`,
      timeout: cdk.Duration.minutes(30)
    });


    // Outputs
    new cdk.CfnOutput(this, "DataWarehouseBucketName", {
      value: dataWarehouseBucket.bucketName,
      description: "Data warehouse bucket name",
    });

    new cdk.CfnOutput(this, "FetchLambdaArn", {
      value: fetchLambda.functionArn,
      description: "Fetch Lambda ARN",
    });

    new cdk.CfnOutput(this, "BronzeJobName", {
      value: resourceNames.bronzeJob,
      description: "Bronze Glue Job Name",
    });

    new cdk.CfnOutput(this, "SilverJobName", {
      value: resourceNames.silverJob,
      description: "Silver Glue Job Name",
    });

    new cdk.CfnOutput(this, "BronzeCrawlerName", {
      value: resourceNames.bronzeCrawler,
      description: "Bronze Crawler Name",
    });

    new cdk.CfnOutput(this, "SilverCrawlerName", {
      value: `${warehouseConfig.warehouse_prefix}-silver-${dataset}-crawler`,
      description: "Silver Crawler Name (for initial table creation)",
    });

    new cdk.CfnOutput(this, "EtlPipelineArn", {
      value: stateMachine.stateMachineArn,
      description: "ETL Pipeline State Machine ARN",
    });
  }
}

module.exports = { PpDwEtlStack };
