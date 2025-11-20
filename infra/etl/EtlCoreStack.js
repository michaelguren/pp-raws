const cdk = require("aws-cdk-lib");
const s3 = require("aws-cdk-lib/aws-s3");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const { rootSharedRuntimePath } = require("./shared/deploytime/paths");

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
          // Safe to expire: Temporary processing files only
          id: "temp-cleanup",
          prefix: "temp/",
          expiration: cdk.Duration.days(7),
        },
        // ⚠️ CRITICAL: NEVER add lifecycle rules for the following prefixes:
        //   - gold/* (Delta Lake tables - needed for time travel)
        //   - */_delta_log/* (Delta transaction logs - required for ACID + versioning)
        //   - silver/* (transformed data - referenced by gold layer)
        //   - bronze/* (source of truth for lineage)
        // Only expire raw/* or temp/* prefixes if storage costs become an issue.
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

    // Grant Athena permissions for Delta test harness and ad-hoc queries
    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'athena:StartQueryExecution',
        'athena:GetQueryExecution',
        'athena:GetQueryResults',
        'athena:StopQueryExecution',
        'athena:GetWorkGroup',
        'athena:ListQueryExecutions',
        'athena:BatchGetQueryExecution',
        'athena:GetQueryResultsStream'
      ],
      resources: [
        `arn:aws:athena:${this.region}:${this.account}:workgroup/primary`,
        `arn:aws:athena:${this.region}:${this.account}:workgroup/*`
      ]
    }));

    // Grant Glue Data Catalog read permissions (for Athena queries)
    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'glue:GetDatabase',
        'glue:GetTable',
        'glue:GetPartitions',
        'glue:GetPartition',
        'glue:BatchGetPartition',
        'glue:GetCrawler'
      ],
      resources: [
        `arn:aws:glue:${this.region}:${this.account}:catalog`,
        `arn:aws:glue:${this.region}:${this.account}:database/${bronzeDbName}`,
        `arn:aws:glue:${this.region}:${this.account}:database/${silverDbName}`,
        `arn:aws:glue:${this.region}:${this.account}:database/${goldDbName}`,
        `arn:aws:glue:${this.region}:${this.account}:table/${bronzeDbName}/*`,
        `arn:aws:glue:${this.region}:${this.account}:table/${silverDbName}/*`,
        `arn:aws:glue:${this.region}:${this.account}:table/${goldDbName}/*`,
        `arn:aws:glue:${this.region}:${this.account}:crawler/*`
      ]
    }));

    // Deploy shared runtime utilities (used by all datasets)
    new s3deploy.BucketDeployment(this, "RuntimeUtils", {
      sources: [s3deploy.Source.asset(rootSharedRuntimePath(__dirname))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: "etl/shared/runtime/",
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