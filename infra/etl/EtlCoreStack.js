const cdk = require("aws-cdk-lib");
const s3 = require("aws-cdk-lib/aws-s3");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");

class EtlCoreStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Load warehouse configuration
    const warehouseConfig = require("./config/warehouse.json");
    
    // Single data warehouse bucket with prefix-based organization
    const dataWarehouseBucket = new s3.Bucket(this, "DataWarehouseBucket", {
      bucketName: warehouseConfig.bucket_name_pattern.replace('{account}', this.account),
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

    // Shared Glue service role for all ETL jobs
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

    // Shared Lambda execution role for all fetch functions
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

    // Store references for other stacks to import
    this.dataWarehouseBucket = dataWarehouseBucket;
    this.glueRole = glueRole;
    this.lambdaRole = lambdaRole;
    this.bronzeDatabase = bronzeDatabase;
    this.silverDatabase = silverDatabase;
    this.warehouseConfig = warehouseConfig;

    // Outputs for cross-stack references
    new cdk.CfnOutput(this, "DataWarehouseBucketName", {
      value: dataWarehouseBucket.bucketName,
      description: "Shared data warehouse bucket name",
      exportName: "pp-dw-bucket-name"
    });

    new cdk.CfnOutput(this, "GlueRoleArn", {
      value: glueRole.roleArn,
      description: "Shared Glue execution role ARN",
      exportName: "pp-dw-glue-role-arn"
    });

    new cdk.CfnOutput(this, "LambdaRoleArn", {
      value: lambdaRole.roleArn,
      description: "Shared Lambda execution role ARN", 
      exportName: "pp-dw-lambda-role-arn"
    });
  }
}

module.exports = { EtlCoreStack };