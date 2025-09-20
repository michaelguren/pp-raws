const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

class RxnormSplMappingsStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../config.json");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    // Compute database names from prefix
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    // Construct resource names from warehouse conventions
    const resourceNames = {
      bronzeJob: `${etlConfig.etl_resource_prefix}-bronze-${dataset}`,
      bronzeCrawler: `${etlConfig.etl_resource_prefix}-bronze-${dataset}-crawler`,
    };

    // Get worker config based on dataset size category
    const sizeCategory = datasetConfig.data_size_category || 'small';
    const workerConfig = etlConfig.glue_worker_configs[sizeCategory];

    if (!workerConfig) {
      throw new Error(`Invalid data_size_category: ${sizeCategory}. Must be one of: small, medium, large, xlarge`);
    }

    // S3 path fragments using convention
    const rawPath = `raw/${dataset}/`;
    const bronzePath = `bronze/${dataset}/`;

    // Deploy Glue scripts to S3
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
    });

    // Bronze Glue job for RxNORM SPL Mappings processing
    const bronzeJob = new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/${dataset}/glue/bronze_job.py`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": bronzeDatabase,
        "--raw_path": rawPath,
        "--bronze_path": bronzePath,
        "--compression_codec": "zstd",
        "--bucket_name": bucketName,
        "--source_url": datasetConfig.source_url,
        // Default logging arguments (from ETL config)
        ...etlConfig.glue_defaults.logging_arguments,
        // Job-specific logging overrides
        "--spark-event-logs-path": `s3://${bucketName}/spark-logs/`
      },
    });

    // Bronze Crawler for the SPL mappings table
    const bronzeCrawler = new glue.CfnCrawler(this, "BronzeCrawler", {
      name: resourceNames.bronzeCrawler,
      role: glueRole.roleArn,
      databaseName: bronzeDatabase,
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
      }),
      description: `Crawler for RxNORM SPL Mappings bronze table`
    });

    // Outputs
    new cdk.CfnOutput(this, "DatasetName", {
      value: dataset,
      description: "Dataset name for RxNORM SPL Mappings"
    });

    new cdk.CfnOutput(this, "BronzeJobName", {
      value: resourceNames.bronzeJob,
      description: "RxNORM SPL Mappings Bronze ETL Glue Job Name"
    });

    new cdk.CfnOutput(this, "BronzeCrawlerName", {
      value: resourceNames.bronzeCrawler,
      description: "Bronze crawler name for SPL mappings table"
    });

    new cdk.CfnOutput(this, "BronzeTableName", {
      value: `bronze_${dataset.replace('-', '_')}`,
      description: "Bronze table name in Athena"
    });

    new cdk.CfnOutput(this, "SourceUrl", {
      value: datasetConfig.source_url,
      description: "Source URL for RxNORM SPL Mappings data"
    });

    new cdk.CfnOutput(this, "ExecutionInstructions", {
      value: JSON.stringify([
        `1. Run Glue job: ${resourceNames.bronzeJob}`,
        `2. Run crawler: ${resourceNames.bronzeCrawler}`,
        `3. Query table: ${bronzeDatabase}.bronze_${dataset.replace('-', '_')}`
      ]),
      description: "Execution steps for RxNORM SPL Mappings ETL"
    });

    new cdk.CfnOutput(this, "WorkerConfig", {
      value: JSON.stringify(workerConfig),
      description: "Glue worker configuration used"
    });
  }
}

module.exports = { RxnormSplMappingsStack };