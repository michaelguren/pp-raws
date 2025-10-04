const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");
const etlUtils = require("../../shared/deploytime");

/**
 * RxNORM ETL Stack - Custom implementation (not using factory)
 *
 * This dataset is unique:
 * - Requires UMLS API key authentication (via Secrets Manager)
 * - Custom bronze job downloads from NLM (not standard HTTP/ZIP)
 * - Multiple RRF tables requiring separate crawlers
 * - Two gold jobs: rxcui_changes and rxnorm_ndc_mapping
 */
class RxnormStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load dataset configuration
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    // Define bronze tables explicitly
    const bronzeTables = Object.keys(datasetConfig.tables);

    // Get centralized resource names and paths
    const resourceNames = etlUtils.getResourceNames(dataset, bronzeTables);
    const paths = etlUtils.getS3Paths(bucketName, dataset, bronzeTables);
    const workerConfig = etlUtils.getWorkerConfig(datasetConfig.data_size_category);

    // Create RxNORM-specific Glue role with Secrets Manager access
    const rxnormGlueRole = new iam.Role(this, "RxnormGlueRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
      ],
    });

    // Grant S3 access to the data warehouse bucket
    dataWarehouseBucket.grantReadWrite(rxnormGlueRole);

    // Grant Secrets Manager access for UMLS API key (least privilege - only RxNORM needs this)
    rxnormGlueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["secretsmanager:GetSecretValue"],
      resources: [`arn:aws:secretsmanager:${this.region}:${this.account}:secret:umls-api-key*`]
    }));

    // Deploy Glue scripts to S3
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
    });

    // Bronze Glue job for RxNORM RRF processing (custom - requires UMLS auth)
    new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: rxnormGlueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/${dataset}/glue/bronze_job.py`,
        pythonVersion: etlUtils.glue_defaults.python_version,
      },
      glueVersion: etlUtils.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlUtils.glue_defaults.max_retries,
      timeout: etlUtils.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": resourceNames.bronzeDatabase,
        "--raw_path": paths.raw,
        "--bronze_path": paths.bronze,
        "--compression_codec": "zstd",
        "--bucket_name": bucketName,
        "--umls_api_secret": "umls-api-key",
        "--tables_to_process": bronzeTables.map(t => t.toUpperCase()).join(","),
        "--source_url_pattern": datasetConfig.source_url_pattern,
        "--default_release_date": datasetConfig.default_release_date,
        ...etlUtils.glue_defaults.default_arguments,
      },
    });

    // Bronze Crawlers - use centralized helper (handles multi-table datasets)
    etlUtils.createBronzeCrawlers(
      this,
      dataset,
      bronzeTables,
      rxnormGlueRole,
      resourceNames,
      paths
    );

    // Outputs (BronzeJobName already created by createBronzeCrawlers)
    new cdk.CfnOutput(this, "ExecutionOrder", {
      value: JSON.stringify([
        "1. Run Bronze Job (with --release_date parameter)",
        "2. Run Bronze Crawlers (after Bronze completes)"
      ]),
      description: "Execution Order",
    });

    new cdk.CfnOutput(this, "SecretsManagerNote", {
      value: "Requires UMLS API key in Secrets Manager: umls-api-key",
      description: "Authentication Requirement",
    });
  }
}

module.exports = { RxnormStack };