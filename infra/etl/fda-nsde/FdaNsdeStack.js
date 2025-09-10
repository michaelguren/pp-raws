const cdk = require("aws-cdk-lib");
const lambda = require("aws-cdk-lib/aws-lambda");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const sfn = require("aws-cdk-lib/aws-stepfunctions");
const tasks = require("aws-cdk-lib/aws-stepfunctions-tasks");
const path = require("path");

class FdaNsdeStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const bucketName = cdk.Fn.importValue("pp-dw-bucket-name");
    const glueRoleArn = cdk.Fn.importValue("pp-dw-glue-role-arn");
    const lambdaRoleArn = cdk.Fn.importValue("pp-dw-lambda-role-arn");

    // Reference existing resources
    const glueRole = iam.Role.fromRoleArn(this, "GlueRole", glueRoleArn);
    const lambdaRole = iam.Role.fromRoleArn(this, "LambdaRole", lambdaRoleArn);

    // Load warehouse and dataset configurations
    const etlConfig = require("../config.json");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;
    
    // Construct resource names from warehouse conventions
    const resourceNames = {
      bronzeJob: `${etlConfig.warehouse_prefix}-bronze-${dataset}`,
      silverJob: `${etlConfig.warehouse_prefix}-silver-${dataset}`,
      bronzeCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-crawler`,
      silverCrawler: `${etlConfig.warehouse_prefix}-silver-${dataset}-crawler`,
      fetchLambda: `${etlConfig.warehouse_prefix}-raw-fetch-${dataset}`
    };

    // Fetch Lambda function
    const fetchLambda = new lambda.Function(this, "FetchLambda", {
      functionName: resourceNames.fetchLambda,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "app.handler",
      code: lambda.Code.fromAsset(
        path.join(__dirname, "lambdas/fetch")
      ),
      timeout: cdk.Duration.seconds(etlConfig.lambda_defaults.timeout_seconds),
      memorySize: etlConfig.lambda_defaults.memory_mb,
      role: lambdaRole,
      environment: {
        DATA_WAREHOUSE_BUCKET_NAME: bucketName,
        DATASET: dataset,
      },
    });


    // Get worker config based on dataset size category
    const sizeCategory = datasetConfig.data_size_category || 'medium';
    const workerConfig = etlConfig.glue_worker_configs[sizeCategory];
    
    if (!workerConfig) {
      throw new Error(`Invalid data_size_category: ${sizeCategory}. Must be one of: small, medium, large, xlarge`);
    }

    // Build S3 paths from warehouse config patterns
    const bronzePath = etlConfig.path_patterns.bronze.replace('{dataset}', dataset);
    const silverPath = etlConfig.path_patterns.silver.replace('{dataset}', dataset);
    
    // Pre-compute simple S3 base paths for Glue jobs
    const rawBasePath = `s3://${bucketName}/raw/${dataset}/`;
    const bronzeBasePath = `s3://${bucketName}/bronze/bronze_${dataset}/`;
    const silverBasePath = `s3://${bucketName}/silver/silver_${dataset}/`;

    // Bronze Glue job
    new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/${etlConfig.path_patterns.bronze_script.replace('{dataset}', dataset)}`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": etlConfig.bronze_database,
        "--silver_database": etlConfig.silver_database,
        "--warehouse_prefix": etlConfig.warehouse_prefix,
        "--raw_base_path": rawBasePath,
        "--bronze_base_path": bronzeBasePath,
        "--silver_base_path": silverBasePath,
        "--business_key": datasetConfig.business_key,
        "--date_format": datasetConfig.date_format,
        "--bronze_crawler_name": resourceNames.bronzeCrawler,
        "--partition_key": "partition_datetime",
        "--compression_codec": "zstd",
        "--crawler_timeout_seconds": "600",
        "--crawler_check_interval": "30",
      },
    });

    // Silver Glue job
    new glue.CfnJob(this, "SilverJob", {
      name: resourceNames.silverJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/${etlConfig.path_patterns.silver_script.replace('{dataset}', dataset)}`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": etlConfig.bronze_database,
        "--silver_database": etlConfig.silver_database,
        "--warehouse_prefix": etlConfig.warehouse_prefix,
        "--raw_base_path": rawBasePath,
        "--bronze_base_path": bronzeBasePath,
        "--silver_base_path": silverBasePath,
        "--business_key": datasetConfig.business_key,
        "--date_format": datasetConfig.date_format,
        "--silver_crawler_name": resourceNames.silverCrawler,
        "--partition_key": "effective_year_month",
        "--compression_codec": "zstd",
        "--crawler_timeout_seconds": "600",
        "--crawler_check_interval": "30",
        "--scd_end_date": "9999-12-31",
        "--spark_adaptive_enabled": "true",
        "--spark_adaptive_coalesce": "true",
      },
    });

    // Bronze crawler (auto-discovers schema from parquet files)
    new glue.CfnCrawler(this, "BronzeCrawler", {
      name: resourceNames.bronzeCrawler,
      role: glueRole.roleArn,
      databaseName: etlConfig.bronze_database,
      targets: {
        s3Targets: [
          {
            path: `s3://${bucketName}/${bronzePath}`
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
    new glue.CfnCrawler(this, "SilverCrawler", {
      name: resourceNames.silverCrawler,
      role: glueRole.roleArn,
      databaseName: etlConfig.silver_database,
      targets: {
        s3Targets: [
          {
            path: `s3://${bucketName}/${silverPath}`
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

    // All configuration now passed via Glue job arguments - no S3 deployments needed

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
        "--run_id": sfn.JsonPath.stringAt("$.run_id"),
        "--bucket_name": bucketName
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      resultPath: "$.bronzeJobResult"
    });

    const silverJobTask = new tasks.GlueStartJobRun(this, "SilverJobTask", {
      glueJobName: resourceNames.silverJob,
      arguments: sfn.TaskInput.fromObject({
        "--run_id": sfn.JsonPath.stringAt("$.run_id"),
        "--bucket_name": bucketName
      }),
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      resultPath: "$.silverJobResult"
    });

    const definition = fetchTask.next(bronzeJobTask).next(silverJobTask);

    const stateMachine = new sfn.StateMachine(this, "EtlPipeline", {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      stateMachineName: `${etlConfig.warehouse_prefix}-pipeline-${dataset}`,
      timeout: cdk.Duration.minutes(30)
    });


    // Outputs
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
      value: resourceNames.silverCrawler,
      description: "Silver Crawler Name (for initial table creation)",
    });

    new cdk.CfnOutput(this, "EtlPipelineArn", {
      value: stateMachine.stateMachineArn,
      description: "ETL Pipeline State Machine ARN",
    });
  }
}

module.exports = { FdaNsdeStack };
