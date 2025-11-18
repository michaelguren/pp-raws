const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const deployUtils = require("../../../shared/deploytime");
const { glueScriptPath } = require("../../../shared/deploytime/paths");

/**
 * RxClass ETL Stack - Pattern B (Custom API Source)
 * Multi-step REST API collection from NLM RxNav
 * Uses custom bronze job for API-specific logic (rate limiting, pagination, JSON aggregation)
 */
class RxClassStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load dataset configuration
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    // Get standardized resource names
    const tables = [dataset]; // Single table output
    const resourceNames = deployUtils.getResourceNames(dataset, tables);
    const paths = deployUtils.getS3Paths(bucketName, dataset, tables);

    // Get worker config
    const workerConfig = deployUtils.getWorkerConfig(datasetConfig.data_size_category);

    // Deploy custom Glue script to S3 (Pattern B: custom bronze job for API collection)
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(glueScriptPath(__dirname))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/datasets/${dataset}/glue/`,
    });

    // Bronze Glue job - Custom API-based data collection
    // Note: Uses custom script (not shared bronze_http_job.py) because this is an API source
    const bronzeJob = new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/datasets/${dataset}/glue/bronze_job.py`,
        pythonVersion: deployUtils.glue_defaults.python_version,
      },
      glueVersion: deployUtils.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: deployUtils.glue_defaults.max_retries,
      timeout: deployUtils.glue_defaults.timeout_minutes,
      defaultArguments: {
        // Dataset-specific arguments
        "--dataset": dataset,
        "--bronze_database": resourceNames.bronzeDatabase,
        "--raw_path": paths.raw,
        "--bronze_path": paths.bronze,
        "--compression_codec": "zstd",
        // API endpoint from config
        "--all_classes_url": datasetConfig.api_endpoints.all_classes,
        // Shared runtime utilities
        "--extra-py-files": `s3://${bucketName}/etl/shared/runtime/https_zip/etl_runtime_utils.py`,
        // Standard Glue arguments
        ...deployUtils.glue_defaults.default_arguments,
        "--continuous-log-logStreamPrefix": resourceNames.bronzeJob,
        "--spark-event-logs-path": `s3://${bucketName}/spark-logs/`
      },
    });

    // Bronze crawler (auto-discovers schema from parquet files)
    const bronzeCrawler = new glue.CfnCrawler(this, "BronzeCrawler", {
      name: resourceNames.bronzeCrawler,
      role: glueRole.roleArn,
      databaseName: resourceNames.bronzeDatabase,
      targets: {
        s3Targets: [{ path: paths.bronze }]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      }),

      // Schema change policies (consistent pattern across all layers)
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE'
      }
    });

    // Outputs
    new cdk.CfnOutput(this, "BronzeJobName", {
      value: resourceNames.bronzeJob,
      description: "RxClass Bronze Job Name (API collection)",
    });

    new cdk.CfnOutput(this, "BronzeCrawlerName", {
      value: resourceNames.bronzeCrawler,
      description: "RxClass Bronze Crawler Name",
    });

    new cdk.CfnOutput(this, "ApiEndpoints", {
      value: JSON.stringify(datasetConfig.api_endpoints),
      description: "RxNav API endpoints used for data collection",
    });
  }
}

module.exports = { RxClassStack };