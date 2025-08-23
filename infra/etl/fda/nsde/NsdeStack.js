const cdk = require("aws-cdk-lib");
const s3 = require("aws-cdk-lib/aws-s3");
const s3Deploy = require("aws-cdk-lib/aws-s3-deployment");
const lambda = require("aws-cdk-lib/aws-lambda");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const path = require("path");

class PpRawsEtlFdaNsdeStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Load dataset configuration
    const config = require("./config/dataset.json");
    const dataset = config.dataset;

    // Simple S3 bucket with default encryption
    // Create separate buckets for each data layer
    const rawBucket = new s3.Bucket(this, "RawBucket", {
      bucketName: `pp-dw-raw-${this.account}`,
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

    const bronzeBucket = new s3.Bucket(this, "BronzeBucket", {
      bucketName: `pp-dw-bronze-${this.account}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const silverBucket = new s3.Bucket(this, "SilverBucket", {
      bucketName: `pp-dw-silver-${this.account}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
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

    // Grant S3 access to Glue for all buckets
    rawBucket.grantRead(glueRole);
    bronzeBucket.grantReadWrite(glueRole);
    silverBucket.grantReadWrite(glueRole);

    // Simple Lambda execution role
    const lambdaRole = new iam.Role(this, "LambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole"
        ),
      ],
    });

    // Grant S3 access to Lambda (only needs raw bucket)
    rawBucket.grantReadWrite(lambdaRole);

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
        RAW_BUCKET_NAME: rawBucket.bucketName,
        BRONZE_BUCKET_NAME: bronzeBucket.bucketName,
        SILVER_BUCKET_NAME: silverBucket.bucketName,
        DATASET: dataset,
      },
    });


    // Simple Glue jobs
    const bronzeJob = new glue.CfnJob(this, "BronzeJob", {
      name: config.bronze_job_name,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${rawBucket.bucketName}/scripts/${dataset}/bronze_job.py`,
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
        scriptLocation: `s3://${rawBucket.bucketName}/scripts/${dataset}/silver_job.py`,
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

    // Deploy Glue scripts to raw bucket
    new s3Deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3Deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: rawBucket,
      destinationKeyPrefix: `scripts/${dataset}/`
    });


    // Outputs
    new cdk.CfnOutput(this, "RawBucketName", {
      value: rawBucket.bucketName,
      description: "Raw data bucket name",
    });

    new cdk.CfnOutput(this, "BronzeBucketName", {
      value: bronzeBucket.bucketName,
      description: "Bronze data bucket name",
    });

    new cdk.CfnOutput(this, "SilverBucketName", {
      value: silverBucket.bucketName,
      description: "Silver data bucket name",
    });

    new cdk.CfnOutput(this, "FetchLambdaArn", {
      value: fetchAndHashLambda.functionArn,
      description: "Fetch Lambda ARN",
    });

    new cdk.CfnOutput(this, "BronzeJobName", {
      value: config.bronze_job_name,
      description: "Bronze Glue Job Name",
    });

    new cdk.CfnOutput(this, "SilverJobName", {
      value: config.silver_job_name,
      description: "Silver Glue Job Name",
    });
  }
}

module.exports = { PpRawsEtlFdaNsdeStack };
