const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

class FdaAllNdcStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../config.json");
    const datasetConfig = require("./config.json");

    // Compute database names from prefix
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;
    const dataset = datasetConfig.dataset;

    // Get worker configuration based on data size category
    const workerConfig = etlConfig.glue_worker_configs[datasetConfig.data_size_category];

    // Build paths using convention - GOLD layer paths
    const goldBasePath = `s3://${bucketName}/gold/${datasetConfig.dataset}/`;
    const goldScriptPath = `s3://${bucketName}/etl/${datasetConfig.dataset}/glue/gold_job.py`;


    // Deploy Glue script to S3
    new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
      sources: [s3deploy.Source.asset(path.join(__dirname, 'glue'))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${datasetConfig.dataset}/glue/`,
      retainOnDelete: false
    });

    // Create GOLD Glue Job
    const goldJob = new glue.CfnJob(this, 'GoldJob', {
      name: `${etlConfig.etl_resource_prefix}-gold-${datasetConfig.dataset}`,
      description: `GOLD layer job for ${datasetConfig.dataset} - ${datasetConfig.description}`,
      role: glueRole.roleArn,

      command: {
        name: 'glueetl',
        scriptLocation: goldScriptPath,
        pythonVersion: etlConfig.glue_defaults.python_version
      },

      glueVersion: etlConfig.glue_defaults.version,

      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,

      timeout: etlConfig.glue_defaults.timeout_minutes,
      maxRetries: etlConfig.glue_defaults.max_retries,

      // Job arguments with all configuration pre-computed
      defaultArguments: {
        '--job-language': 'python',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        '--enable-spark-ui': 'true',
        '--spark-event-logs-path': `s3://${bucketName}/glue-spark-logs/`,

        // Dataset and database configuration
        '--dataset': datasetConfig.dataset,
        '--bucket_name': bucketName,
        '--bronze_database': bronzeDatabase,
        '--gold_database': goldDatabase,

        // Pre-computed paths
        '--gold_base_path': goldBasePath,

        // Compression and performance settings
        '--compression_codec': 'ZSTD',
        '--crawler_name': `${etlConfig.etl_resource_prefix}-gold-${datasetConfig.dataset}-crawler`,

        // Additional job-specific arguments can be added here
        '--enable-auto-scaling': 'true'
      }
    });

    // Create Crawler for GOLD layer
    const goldCrawler = new glue.CfnCrawler(this, 'GoldCrawler', {
      name: `${etlConfig.etl_resource_prefix}-gold-${datasetConfig.dataset}-crawler`,
      description: `Crawler for GOLD ${datasetConfig.dataset} parquet files`,
      role: glueRole.roleArn,
      databaseName: goldDatabase,

      targets: {
        s3Targets: [{
          path: goldBasePath
        }]
      },

      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      }),

      // Crawler schedule - runs on demand after job completion
      schedule: {
        scheduleExpression: '' // Empty = on-demand only
      }
    });

    // Create EventBridge rule for scheduling if enabled
    if (datasetConfig.schedule?.enabled) {
      // TODO: Add EventBridge scheduling when needed
    }

    // Export useful properties
    this.goldJob = goldJob;
    this.goldCrawler = goldCrawler;
    this.goldPath = goldBasePath;
  }
}

module.exports = { FdaAllNdcStack };