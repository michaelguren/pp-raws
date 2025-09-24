const cdk = require("aws-cdk-lib");
const s3 = require("aws-cdk-lib/aws-s3");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const path = require("path");

class EtlCoreStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Load warehouse configuration
    const warehouseConfig = require("./config.json");

    // Compute database names from prefix
    const bronzeDbName = `${warehouseConfig.database_prefix}_bronze`;
    const silverDbName = `${warehouseConfig.database_prefix}_silver`;
    const goldDbName = `${warehouseConfig.database_prefix}_gold`;

    // Single data warehouse bucket with prefix-based organization
    const bucketName = `${warehouseConfig.etl_resource_prefix}-${this.account}`;
    const dataWarehouseBucket = new s3.Bucket(this, "DataWarehouseBucket", {
      bucketName: bucketName,
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

    // Deploy shared runtime utilities (used by all datasets)
    new s3deploy.BucketDeployment(this, "RuntimeUtils", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "utils-runtime"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: "etl/utils-runtime/",
    });

    // Glue bronze database for Athena queries (shared across all datasets)
    const bronzeDatabase = new glue.CfnDatabase(this, "BronzeDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: bronzeDbName,
        description: "Bronze layer database for all datasets"
      }
    });

    // Glue silver database for Athena queries (shared across all datasets)
    const silverDatabase = new glue.CfnDatabase(this, "SilverDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: silverDbName,
        description: "Silver layer database for all datasets"
      }
    });

    // Glue gold database for Athena queries (shared across all datasets)
    const goldDatabase = new glue.CfnDatabase(this, "GoldDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: goldDbName,
        description: "Gold layer database for all datasets"
      }
    });

    // Store references for other stacks to import
    this.dataWarehouseBucket = dataWarehouseBucket;
    this.glueRole = glueRole;
    this.bronzeDatabase = bronzeDatabase;
    this.silverDatabase = silverDatabase;
    this.goldDatabase = goldDatabase;
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

  }
}

module.exports = { EtlCoreStack };