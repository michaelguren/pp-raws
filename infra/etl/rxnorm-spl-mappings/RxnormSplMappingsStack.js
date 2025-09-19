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

    // Construct resource names from warehouse conventions
    const resourceNames = {
      bronzeJob: `${etlConfig.warehouse_prefix}-bronze-${dataset}`,
      bronzeCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-crawler`,
    };

    // Get worker config based on dataset size category
    const sizeCategory = datasetConfig.data_size_category || 'small';
    const workerConfig = etlConfig.glue_worker_configs[sizeCategory];

    if (!workerConfig) {
      throw new Error(`Invalid data_size_category: ${sizeCategory}. Must be one of: small, medium, large, xlarge`);
    }

    // Build S3 paths from warehouse config patterns
    const rawBasePath = `s3://${bucketName}/raw/${dataset}/`;
    const bronzeBasePath = `s3://${bucketName}/bronze/bronze_${dataset.replace('-', '_')}/`;

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
        "--bronze_database": etlConfig.bronze_database,
        "--raw_base_path": rawBasePath,
        "--bronze_base_path": bronzeBasePath,
        "--compression_codec": "zstd",
        "--bucket_name": bucketName,
        "--source_url": datasetConfig.source_url,
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": `s3://${bucketName}/spark-logs/`,
        "--enable-metrics": "true"
      },
    });

    // Bronze Crawler for the SPL mappings table
    const bronzeCrawler = new glue.CfnCrawler(this, "BronzeCrawler", {
      name: resourceNames.bronzeCrawler,
      role: glueRole.roleArn,
      databaseName: etlConfig.bronze_database,
      targets: {
        s3Targets: [
          {
            path: bronzeBasePath
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
        `3. Query table: ${etlConfig.bronze_database}.bronze_${dataset.replace('-', '_')}`
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