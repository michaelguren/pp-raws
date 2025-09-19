const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

class RxClassStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../config.json");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    // Construct resource names from warehouse conventions
    const resourceNames = {
      bronzeJob: `${etlConfig.warehouse_prefix}-bronze-${dataset}`,
      bronzeCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-crawler`
    };

    // Get worker config based on dataset size category
    const sizeCategory = datasetConfig.data_size_category || 'small';
    const workerConfig = etlConfig.glue_worker_configs[sizeCategory];

    if (!workerConfig) {
      throw new Error(`Invalid data_size_category: ${sizeCategory}. Must be one of: small, medium, large, xlarge`);
    }

    // Build S3 paths from warehouse config patterns
    const bronzePath = etlConfig.path_patterns.bronze.replace('{dataset}', dataset);

    // Pre-compute simple S3 base paths for Glue jobs
    const rawBasePath = `s3://${bucketName}/raw/${dataset}/`;
    const bronzeBasePath = `s3://${bucketName}/bronze/${dataset}/`;

    // Deploy Glue scripts to S3
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
    });

    // Bronze Glue job - API-based data collection
    new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/${etlConfig.path_patterns.bronze_script.replace('{dataset}', dataset)}`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--class_types_url": datasetConfig.api_endpoints.class_types,
        "--all_classes_url": datasetConfig.api_endpoints.all_classes,
        "--bronze_database": etlConfig.bronze_database,
        "--raw_base_path": rawBasePath,
        "--bronze_base_path": bronzeBasePath,
        "--compression_codec": "zstd",
        "--bucket_name": bucketName,
        "--crawler_name": resourceNames.bronzeCrawler,
      },
    });

    // Bronze crawler (auto-discovers schema from parquet files)
    new glue.CfnCrawler(this, "BronzeCrawler", {
      name: resourceNames.bronzeCrawler,
      role: glueRole.roleArn,
      databaseName: etlConfig.bronze_database,
      targets: {
        s3Targets: [
          {
            path: `s3://${bucketName}/${bronzePath}`
          }
        ]
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
      description: "RxClass API ETL Glue Job Name (API collection + transform)",
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