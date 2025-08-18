const cdk = require("aws-cdk-lib");
const s3 = require("aws-cdk-lib/aws-s3");
const s3Deploy = require("aws-cdk-lib/aws-s3-deployment");
const lambda = require("aws-cdk-lib/aws-lambda");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const sfn = require("aws-cdk-lib/aws-stepfunctions");
const sfnTasks = require("aws-cdk-lib/aws-stepfunctions-tasks");
const path = require("path");

class PpRawsEtlFdaNsdeStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Load dataset configuration
    const config = require("./config/dataset.json");
    const dataset = config.dataset;

    // Simple S3 bucket with default encryption
    const dataBucket = new s3.Bucket(this, "DataBucket", {
      bucketName: `pp-raws-${dataset}-${this.account}`,
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

    // Grant S3 access to Glue
    dataBucket.grantReadWrite(glueRole);

    // Simple Lambda execution role
    const lambdaRole = new iam.Role(this, "LambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole"
        ),
      ],
    });

    // Grant S3 access to Lambda
    dataBucket.grantReadWrite(lambdaRole);

    // Simple Lambda functions (no layers)
    const fetchAndHashLambda = new lambda.Function(this, "FetchAndHashLambda", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "app.handler",
      code: lambda.Code.fromAsset(
        path.join(__dirname, "lambdas/fetch_and_hash")
      ),
      timeout: cdk.Duration.seconds(300),
      memorySize: 1024,
      role: lambdaRole,
      environment: {
        BUCKET_NAME: dataBucket.bucketName,
        DATASET: dataset,
      },
    });

    const updateManifestLambda = new lambda.Function(
      this,
      "UpdateManifestLambda",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "app.handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "lambdas/update_manifest")
        ),
        timeout: cdk.Duration.seconds(60),
        memorySize: 256,
        role: lambdaRole,
        environment: {
          BUCKET_NAME: dataBucket.bucketName,
          DATASET: dataset,
        },
      }
    );

    // Simple Glue jobs
    const bronzeJob = new glue.CfnJob(this, "BronzeJob", {
      name: config.bronze_job_name,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${dataBucket.bucketName}/scripts/${dataset}/bronze_job.py`,
        pythonVersion: "3",
      },
      glueVersion: "4.0",
      workerType: "G.1X",
      numberOfWorkers: 5,
      maxRetries: 0,
      timeout: 60,
      defaultArguments: {
        "--dataset": dataset,
      },
    });

    const silverJob = new glue.CfnJob(this, "SilverJob", {
      name: config.silver_job_name,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${dataBucket.bucketName}/scripts/${dataset}/silver_job.py`,
        pythonVersion: "3",
      },
      glueVersion: "4.0",
      workerType: "G.1X",
      numberOfWorkers: 5,
      maxRetries: 0,
      timeout: 60,
      defaultArguments: {
        "--dataset": dataset,
      },
    });

    // Deploy Glue scripts to S3
    new s3Deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3Deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataBucket,
      destinationKeyPrefix: `scripts/${dataset}/`
    });

    // Build Step Functions definition with CDK constructs
    const generateRunId = new sfn.Pass(this, "GenerateRunId", {
      parameters: {
        "dataset.$": "$.dataset",
        "source_url.$": "$.source_url",
        "force.$": "$.force",
        "bucket.$": "$.bucket",
        "fetch_and_hash_function_name.$": "$.fetch_and_hash_function_name",
        "update_manifest_function_name.$": "$.update_manifest_function_name",
        "bronze_job_name.$": "$.bronze_job_name",
        "silver_job_name.$": "$.silver_job_name",
        "run_id.$": "States.Format('{}-{}', $.dataset, $$.State.EnteredTime)"
      }
    });

    const fetchAndHash = new sfnTasks.LambdaInvoke(this, "FetchAndHash", {
      lambdaFunction: fetchAndHashLambda,
      payloadResponseOnly: true
    });

    const checkIfChanged = new sfn.Choice(this, "CheckIfChanged");
    
    const skipProcessing = new sfn.Succeed(this, "SkipProcessing", {
      comment: "Source unchanged and not forced - skipping"
    });

    const continueProcessing = new sfn.Pass(this, "ContinueProcessing", {
      comment: "Proceeding with ETL processing"
    });

    const runBronzeJob = new sfnTasks.GlueStartJobRun(this, "RunBronzeJob", {
      glueJobName: config.bronze_job_name,
      arguments: {
        "--raw_path.$": "$.raw_path",
        "--run_id.$": "$.run_id",
        "--bronze_path.$": "$.bronze_path"
      },
      integrationPattern: sfn.IntegrationPattern.RUN_JOB
    });

    const runSilverJob = new sfnTasks.GlueStartJobRun(this, "RunSilverJob", {
      glueJobName: config.silver_job_name,
      arguments: {
        "--bronze_path.$": "$.bronze_path", 
        "--run_id.$": "$.run_id"
      },
      integrationPattern: sfn.IntegrationPattern.RUN_JOB
    });

    const updateManifest = new sfnTasks.LambdaInvoke(this, "UpdateManifest", {
      lambdaFunction: updateManifestLambda
    });

    const success = new sfn.Succeed(this, "Success", {
      comment: "ETL pipeline completed successfully"
    });

    // Wire the workflow
    const definition = generateRunId
      .next(fetchAndHash)
      .next(checkIfChanged
        .when(sfn.Condition.and(
          sfn.Condition.booleanEquals("$.changed", false),
          sfn.Condition.booleanEquals("$.force", false)
        ), skipProcessing)
        .otherwise(continueProcessing
          .next(runBronzeJob)
          .next(runSilverJob)
          .next(updateManifest)
          .next(success)
        )
      );

    const stateMachine = new sfn.StateMachine(this, "StateMachine", {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      timeout: cdk.Duration.minutes(120),
      stateMachineName: `${dataset}-etl-orchestrator`
    });


    // Manual trigger Lambda
    const triggerLambda = new lambda.Function(this, "TriggerLambda", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.handler",
      code: lambda.Code.fromInline(`
import json
import boto3

def handler(event, context):
    client = boto3.client('stepfunctions')
    
    force = event.get('force', False)
    
    response = client.start_execution(
        stateMachineArn='${stateMachine.stateMachineArn}',
        input=json.dumps({
            'dataset': '${dataset}',
            'source_url': '${config.source_url}',
            'force': force,
            'bucket': '${dataBucket.bucketName}',
            'fetch_and_hash_function_name': '${fetchAndHashLambda.functionName}',
            'update_manifest_function_name': '${updateManifestLambda.functionName}',
            'bronze_job_name': '${config.bronze_job_name}',
            'silver_job_name': '${config.silver_job_name}'
        })
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'executionArn': response['executionArn'],
            'message': f'Started ${dataset} ETL'
        })
    }
      `),
      timeout: cdk.Duration.seconds(30),
    });

    stateMachine.grantStartExecution(triggerLambda);

    // Outputs
    new cdk.CfnOutput(this, "BucketName", {
      value: dataBucket.bucketName,
      description: "Data bucket name",
    });

    new cdk.CfnOutput(this, "StateMachineArn", {
      value: stateMachine.stateMachineArn,
      description: "Step Functions ARN",
    });

    new cdk.CfnOutput(this, "TriggerLambdaArn", {
      value: triggerLambda.functionArn,
      description: "Trigger Lambda ARN",
    });
  }
}

module.exports = { PpRawsEtlFdaNsdeStack };
