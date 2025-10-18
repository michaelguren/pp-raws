const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");
const deployUtils = require("../../../shared/deploytime");

/**
 * RxClass Drug Members Silver Layer Stack
 * Reads rxnorm_products silver table and enriches with class relationships via NLM RxNav API
 * Drug-first approach: Single API call per drug returns ALL class relationships
 *
 * Layer: Silver (transformation/enrichment)
 * Input: rxnorm_products (silver)
 * Output: rxclass_drug_members (silver)
 */
class RxclassDrugMembersStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load dataset configuration
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    // Get standardized resource names for silver layer
    const tables = [datasetConfig.output_table || dataset.replace(/-/g, "_")];
    const resourceNames = deployUtils.getResourceNames(dataset, tables);
    const paths = deployUtils.getS3Paths(bucketName, dataset, tables);

    // Get worker config
    const workerConfig = deployUtils.getWorkerConfig(datasetConfig.data_size_category);

    // Deploy custom Glue script to S3
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/datasets/${dataset}/glue/`,
    });

    // Read source dependency configs to get table names
    const rxnormProductsConfig = require("../rxnorm-products/config.json");
    const rxnormProductsTable = rxnormProductsConfig.output_table || "rxnorm_products";

    // Silver Glue job - API-based enrichment
    // Reads rxnorm_products and enriches with class relationships from NLM RxNav API
    const silverJob = new glue.CfnJob(this, "SilverJob", {
      name: resourceNames.silverJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/datasets/${dataset}/glue/silver_job.py`,
        pythonVersion: deployUtils.glue_defaults.python_version,
      },
      glueVersion: deployUtils.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: 0,  // Disabled for debugging - will re-enable once stable
      timeout: deployUtils.glue_defaults.timeout_minutes,
      defaultArguments: {
        // Dataset-specific arguments
        "--dataset": dataset,
        "--silver_database": resourceNames.silverDatabase,
        "--raw_path": paths.raw,
        "--silver_path": `s3://${bucketName}/silver/${dataset}/`,
        "--compression_codec": "zstd",
        // Source dependency: rxnorm_products silver table
        "--rxnorm_products_table": rxnormProductsTable,
        // API base URL (byRxcui endpoint for drug-first approach)
        "--api_base_url": datasetConfig.api_base_url,
        // Shared runtime utilities
        "--extra-py-files": `s3://${bucketName}/etl/shared/runtime/https_zip/etl_runtime_utils.py`,
        // Standard Glue arguments
        ...deployUtils.glue_defaults.default_arguments,
        "--continuous-log-logStreamPrefix": resourceNames.silverJob,
        "--spark-event-logs-path": `s3://${bucketName}/spark-logs/`
      },
    });

    // Silver crawler (auto-discovers schema from parquet files)
    const silverCrawler = new glue.CfnCrawler(this, "SilverCrawler", {
      name: `${deployUtils.etl_resource_prefix}-silver-${dataset}-crawler`,
      role: glueRole.roleArn,
      databaseName: resourceNames.silverDatabase,
      targets: {
        s3Targets: [{ path: `s3://${bucketName}/silver/${dataset}/` }]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });

    // Outputs
    new cdk.CfnOutput(this, "SilverJobName", {
      value: resourceNames.silverJob,
      description: "RxClass Drug Members Silver Job Name (API enrichment)",
    });

    new cdk.CfnOutput(this, "SilverCrawlerName", {
      value: `${deployUtils.etl_resource_prefix}-silver-${dataset}-crawler`,
      description: "RxClass Drug Members Silver Crawler Name",
    });

    new cdk.CfnOutput(this, "ApiBaseUrl", {
      value: datasetConfig.api_base_url,
      description: "RxNav byRxcui API endpoint (drug-first approach)",
    });

    new cdk.CfnOutput(this, "SourceDependency", {
      value: `pp_dw_silver.${rxnormProductsTable}`,
      description: "Source silver table (RxNORM Products)",
    });
  }
}

module.exports = { RxclassDrugMembersStack };
