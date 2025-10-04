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

    // Gold Job: RxCUI Changes Tracking
    new glue.CfnJob(this, "RxcuiChangesJob", {
      name: `${etlUtils.etl_resource_prefix}-gold-rxcui-changes`,
      role: rxnormGlueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/${dataset}/glue/rxcui_changes_job.py`,
        pythonVersion: etlUtils.glue_defaults.python_version,
      },
      glueVersion: etlUtils.glue_defaults.version,
      workerType: "G.1X",
      numberOfWorkers: 5,
      maxRetries: etlUtils.glue_defaults.max_retries,
      timeout: etlUtils.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": resourceNames.bronzeDatabase,
        "--gold_database": resourceNames.goldDatabase,
        "--bucket_name": bucketName,
        "--compression_codec": "zstd",
        ...etlUtils.glue_defaults.default_arguments,
      },
    });

    // Gold Job: RxNORM-NDC Mapping Enrichment
    new glue.CfnJob(this, "GoldEnrichmentJob", {
      name: `${etlUtils.etl_resource_prefix}-gold-rxnorm-ndc-mapping`,
      role: rxnormGlueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/${dataset}/glue/gold_enrichment_job.py`,
        pythonVersion: etlUtils.glue_defaults.python_version,
      },
      glueVersion: etlUtils.glue_defaults.version,
      workerType: "G.1X",
      numberOfWorkers: 5,
      maxRetries: etlUtils.glue_defaults.max_retries,
      timeout: etlUtils.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": resourceNames.bronzeDatabase,
        "--gold_database": resourceNames.goldDatabase,
        "--bucket_name": bucketName,
        "--compression_codec": "zstd",
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

    // Gold Crawlers (RxNORM has 2 custom gold outputs)
    const crawlerConfig = JSON.stringify({
      Version: 1.0,
      CrawlerOutput: {
        Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
        Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
      }
    });

    new glue.CfnCrawler(this, "RxcuiChangesCrawler", {
      name: `${etlUtils.etl_resource_prefix}-gold-rxcui-changes-crawler`,
      role: rxnormGlueRole.roleArn,
      databaseName: resourceNames.goldDatabase,
      targets: {
        s3Targets: [{ path: `s3://${bucketName}/gold/rxcui-changes/` }]
      },
      configuration: crawlerConfig
    });

    new glue.CfnCrawler(this, "NdcMappingCrawler", {
      name: `${etlUtils.etl_resource_prefix}-gold-rxnorm-ndc-mapping-crawler`,
      role: rxnormGlueRole.roleArn,
      databaseName: resourceNames.goldDatabase,
      targets: {
        s3Targets: [{ path: `s3://${bucketName}/gold/rxnorm-ndc-mapping/` }]
      },
      configuration: crawlerConfig
    });

    // Outputs (BronzeJobName already created by createBronzeCrawlers)
    new cdk.CfnOutput(this, "GoldJobNames", {
      value: JSON.stringify({
        rxcuiChanges: `${etlUtils.etl_resource_prefix}-gold-rxcui-changes`,
        ndcMapping: `${etlUtils.etl_resource_prefix}-gold-rxnorm-ndc-mapping`
      }),
      description: "Gold Job Names",
    });

    new cdk.CfnOutput(this, "GoldCrawlerNames", {
      value: JSON.stringify({
        rxcuiChanges: `${etlUtils.etl_resource_prefix}-gold-rxcui-changes-crawler`,
        ndcMapping: `${etlUtils.etl_resource_prefix}-gold-rxnorm-ndc-mapping-crawler`
      }),
      description: "Gold Crawler Names",
    });

    new cdk.CfnOutput(this, "ExecutionOrder", {
      value: JSON.stringify([
        "1. Run Bronze Job (with --release_date parameter)",
        "2. Run Bronze Crawlers (after Bronze completes)",
        "3. Run Gold Jobs: rxcui-changes + ndc-mapping (after Crawlers complete)",
        "4. Run Gold Crawlers (after Gold Jobs complete)"
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