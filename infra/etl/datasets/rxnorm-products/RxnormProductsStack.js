const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

/**
 * RxNORM Products Silver Layer Stack
 * Enriches and denormalizes RxNORM bronze tables into prescribable drug products
 * Silver jobs are custom transformations from bronze sources
 */
class RxnormProductsStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../../config.json");
    const datasetConfig = require("./config.json");

    // Compute database names from prefix
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const dataset = datasetConfig.dataset;

    // Get worker configuration based on data size category
    const workerConfig = etlConfig.glue_worker_configs[datasetConfig.data_size_category];

    // Build paths using convention - SILVER layer paths
    const silverBasePath = `s3://${bucketName}/silver/${dataset}/`;
    const silverScriptPath = `s3://${bucketName}/etl/datasets/${dataset}/glue/silver_job.py`;

    // Deploy Glue script to S3
    new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
      sources: [s3deploy.Source.asset(path.join(__dirname, 'glue'))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/datasets/${dataset}/glue/`,
      retainOnDelete: false
    });

    // Create SILVER Glue Job
    const silverJob = new glue.CfnJob(this, 'SilverJob', {
      name: `${etlConfig.etl_resource_prefix}-silver-${dataset}`,
      description: `SILVER layer job for ${dataset} - ${datasetConfig.description}`,
      role: glueRole.roleArn,

      command: {
        name: 'glueetl',
        scriptLocation: silverScriptPath,
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
        '--dataset': dataset,
        '--bucket_name': bucketName,
        '--bronze_database': bronzeDatabase,
        '--silver_database': silverDatabase,

        // Pre-computed paths
        '--silver_base_path': silverBasePath,

        // Source bronze table names (from config dependencies)
        '--rxnconso_table': 'rxnconso',
        '--rxnrel_table': 'rxnrel',
        '--rxnsat_table': 'rxnsat',

        // Output table name
        '--output_table': datasetConfig.output_table,

        // Compression and performance settings
        '--compression_codec': 'ZSTD',
        '--crawler_name': `${etlConfig.etl_resource_prefix}-silver-${dataset}-crawler`,

        // Additional job-specific arguments
        '--enable-auto-scaling': 'true'
      }
    });

    // Create Crawler for SILVER layer
    const silverCrawler = new glue.CfnCrawler(this, 'SilverCrawler', {
      name: `${etlConfig.etl_resource_prefix}-silver-${dataset}-crawler`,
      description: `Crawler for SILVER ${dataset} parquet files`,
      role: glueRole.roleArn,
      databaseName: silverDatabase,

      targets: {
        s3Targets: [{
          path: silverBasePath
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
    this.silverJob = silverJob;
    this.silverCrawler = silverCrawler;
    this.silverPath = silverBasePath;
  }
}

module.exports = { RxnormProductsStack };
