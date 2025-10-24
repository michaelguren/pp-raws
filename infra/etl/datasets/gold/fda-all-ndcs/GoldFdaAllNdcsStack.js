const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

/**
 * FDA All NDCs Gold Layer Stack - Delta Lake Edition
 * Applies temporal versioning (SCD Type 2) to silver FDA data using Delta Lake
 * Partitions by status (current/historical) for efficient queries
 *
 * Delta Lake Benefits:
 * - 90%+ reduction in S3 writes (MERGE vs full overwrite)
 * - ACID transactions
 * - Time travel capabilities
 * - Automatic schema evolution
 * - Optimized read performance with ZORDER
 */
class GoldFdaAllNdcsStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../../../config.json");
    const datasetConfig = require("./config.json");

    // Load source dataset config to get table name
    const fdaAllNdcsConfig = require("../../silver/fda-all-ndcs/config.json");

    // Compute database names from prefix
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;
    const dataset = datasetConfig.dataset;

    // Get worker configuration based on data size category
    const workerConfig = etlConfig.glue_worker_configs[datasetConfig.data_size_category];

    // Silver source table name (use output_table if specified, otherwise convert dataset name)
    const silverTable = fdaAllNdcsConfig.output_table || fdaAllNdcsConfig.dataset.replace(/-/g, '_');

    // Build paths using convention - GOLD layer paths
    const goldBasePath = `s3://${bucketName}/gold/${dataset}/`;
    const goldScriptPath = `s3://${bucketName}/etl/${dataset}/glue/gold_job.py`;

    // Temporal versioning library path
    const temporalLibPath = `s3://${bucketName}/etl/shared/runtime/temporal/`;

    // Deploy Glue script to S3
    new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
      sources: [s3deploy.Source.asset(path.join(__dirname, 'glue'))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
      retainOnDelete: false
    });

    // Deploy Delta Lake temporal versioning library to S3
    new s3deploy.BucketDeployment(this, 'DeployTemporalLib', {
      sources: [s3deploy.Source.asset(path.join(__dirname, '../../../shared/runtime/temporal'))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: 'etl/shared/runtime/temporal/',
      retainOnDelete: false,
      prune: false  // Don't delete other files in this prefix
    });

    // Create GOLD Glue Job with Delta Lake Support
    // Note: Glue 5.0 includes Delta Lake 3.0.0 built-in - no extra JARs needed
    const goldJob = new glue.CfnJob(this, 'GoldJob', {
      name: `${etlConfig.etl_resource_prefix}-gold-${dataset}`,
      description: `GOLD layer job for ${dataset} - ${datasetConfig.description} (Delta Lake)`,
      role: glueRole.roleArn,

      command: {
        name: 'glueetl',
        scriptLocation: goldScriptPath,
        pythonVersion: etlConfig.glue_defaults.python_version
      },

      glueVersion: etlConfig.glue_defaults.version,  // 5.0 has Delta Lake built-in

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

        // Delta Lake configuration
        '--conf': 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',

        // Extra Python files - Delta Lake temporal versioning library
        '--extra-py-files': `${temporalLibPath}temporal_versioning_delta.py`,

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

    // Create Crawler for GOLD layer Delta table
    const goldCrawler = new glue.CfnCrawler(this, 'GoldCrawler', {
      name: `${etlConfig.etl_resource_prefix}-gold-${dataset}-crawler`,
      description: `Crawler for GOLD ${dataset} Delta table (status-partitioned)`,
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
      description: 'Gold layer Glue job name (Delta Lake)'
    });

    new cdk.CfnOutput(this, 'GoldCrawlerName', {
      value: goldCrawler.name,
      description: 'Gold layer crawler name (Delta Lake)'
    });

    new cdk.CfnOutput(this, 'GoldPath', {
      value: goldBasePath,
      description: 'S3 path for gold layer Delta table'
    });

    new cdk.CfnOutput(this, 'DeltaFormat', {
      value: 'Delta Lake 3.0.0 (Glue 5.0)',
      description: 'Delta Lake format and version'
    });
  }
}

module.exports = { GoldFdaAllNdcsStack };
