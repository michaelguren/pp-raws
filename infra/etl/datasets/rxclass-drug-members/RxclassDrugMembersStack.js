const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");
const deployUtils = require("../../shared/deploytime");

/**
 * RxClass Drug Members ETL Stack - Pattern B (Custom API Source)
 * Reads rxclass bronze table and fetches drug members for each class via RxNav API
 * Uses custom bronze job for API-specific logic (parameter mapping, rate limiting)
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

    // Get standardized resource names
    const tables = [dataset]; // Single table output
    const resourceNames = deployUtils.getResourceNames(dataset, tables);
    const paths = deployUtils.getS3Paths(bucketName, dataset, tables);

    // Get worker config
    const workerConfig = deployUtils.getWorkerConfig(datasetConfig.data_size_category);

    // Deploy custom Glue script to S3 (Pattern B: custom bronze job for API collection)
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/datasets/${dataset}/glue/`,
    });

    // Bronze Glue job - Custom API-based data collection
    // Note: Uses custom script because it reads from rxclass bronze table then calls APIs
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
      maxRetries: 0,  // Disabled for debugging - will re-enable once stable
      timeout: deployUtils.glue_defaults.timeout_minutes,
      defaultArguments: {
        // Dataset-specific arguments
        "--dataset": dataset,
        "--bronze_database": resourceNames.bronzeDatabase,
        "--raw_path": paths.raw,
        "--bronze_path": paths.bronze,
        "--compression_codec": "zstd",
        // Dependency: rxclass bronze table to read from
        "--rxclass_table": datasetConfig.dependencies.rxclass.bronze_table,
        // API base URL
        "--api_base_url": datasetConfig.api_base_url,
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
      })
    });

    // Outputs
    new cdk.CfnOutput(this, "BronzeJobName", {
      value: resourceNames.bronzeJob,
      description: "RxClass Drug Members Bronze Job Name (API collection)",
    });

    new cdk.CfnOutput(this, "BronzeCrawlerName", {
      value: resourceNames.bronzeCrawler,
      description: "RxClass Drug Members Bronze Crawler Name",
    });

    new cdk.CfnOutput(this, "ApiBaseUrl", {
      value: datasetConfig.api_base_url,
      description: "RxNav classMembers API endpoint",
    });
  }
}

module.exports = { RxclassDrugMembersStack };
