const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const { glueScriptPath, sharedRuntimePath } = require("../../../shared/deploytime/paths");

/**
 * RxNORM NDC Mappings Gold Layer Stack
 * Applies temporal versioning (SCD Type 2) to silver rxnorm-ndc-mappings data
 * Partitions by status (current/historical) for efficient queries
 */
class GoldRxnormNdcMappingsStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../../../config.json");
    const datasetConfig = require("./config.json");

    // Load source dataset config to get table name
    const silverConfig = require("../../silver/rxnorm-ndc-mappings/config.json");

    // Compute database names from prefix
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;
    const dataset = datasetConfig.dataset;

    // Get worker configuration based on data size category
    const workerConfig = etlConfig.glue_worker_configs[datasetConfig.data_size_category];

    // Silver source table name (hyphens → underscores)
    const silverTable = silverConfig.dataset.replace(/-/g, '_');

    // Build paths using convention - GOLD layer paths
    const goldBasePath = `s3://${bucketName}/gold/${dataset}/`;
    const goldScriptPath = `s3://${bucketName}/etl/${dataset}/glue/gold_job.py`;

    // Temporal versioning library path
    const temporalLibPath = `s3://${bucketName}/etl/shared/runtime/temporal/`;

    // Deploy Glue script to S3
    new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
      sources: [s3deploy.Source.asset(glueScriptPath(__dirname))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
      retainOnDelete: false
    });

    // Deploy temporal versioning library to S3
    new s3deploy.BucketDeployment(this, 'DeployTemporalLib', {
      sources: [s3deploy.Source.asset(sharedRuntimePath(__dirname, 'temporal'))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: 'etl/shared/runtime/temporal/',
      retainOnDelete: false,
      prune: false  // Don't delete other files in this prefix
    });

    // Create GOLD Glue Job
    const goldJob = new glue.CfnJob(this, 'GoldJob', {
      name: `${etlConfig.etl_resource_prefix}-gold-${dataset}`,
      description: `GOLD layer job for ${dataset} - ${datasetConfig.description}`,
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

        // Extra Python files - temporal versioning library
        '--extra-py-files': `${temporalLibPath}temporal_versioning.py`,

        // Dataset and database configuration
        '--dataset': dataset,
        '--bucket_name': bucketName,
        '--silver_database': silverDatabase,
        '--gold_database': goldDatabase,

        // Pre-computed paths
        '--gold_base_path': goldBasePath,
        '--temporal_lib_path': '/tmp',  // Glue extracts extra-py-files to /tmp

        // Source table name from silver layer
        '--silver_table': silverTable,

        // Compression and performance settings
        '--compression_codec': 'ZSTD',
        '--crawler_name': `${etlConfig.etl_resource_prefix}-gold-${dataset}-crawler`,

        // Additional job-specific arguments
        '--enable-auto-scaling': 'true'
      }
    });

    // Create Crawler for GOLD layer
    const goldCrawler = new glue.CfnCrawler(this, 'GoldCrawler', {
      name: `${etlConfig.etl_resource_prefix}-gold-${dataset}-crawler`,
      description: `Crawler for GOLD ${dataset} parquet files (status-partitioned)`,
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

    // CloudFormation outputs
    new cdk.CfnOutput(this, 'GoldJobName', {
      value: goldJob.name,
      description: 'Gold layer Glue job name'
    });

    new cdk.CfnOutput(this, 'GoldCrawlerName', {
      value: goldCrawler.name,
      description: 'Gold layer crawler name'
    });

    new cdk.CfnOutput(this, 'GoldPath', {
      value: goldBasePath,
      description: 'S3 path for gold layer data'
    });
  }
}

module.exports = { GoldRxnormNdcMappingsStack };
