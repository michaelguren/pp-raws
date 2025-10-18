const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

/**
 * FDA All NDCs Silver Layer Stack
 * Combines NSDE and CDER data with custom transformation logic
 * Silver jobs are dataset-specific and don't use the factory pattern
 */
class FdaAllNdcsStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../../../config.json");
    const datasetConfig = require("./config.json");

    // Load source dataset configs to get table names
    const fdaNsdeConfig = require("../../bronze/fda-nsde/config.json");
    const fdaCderConfig = require("../../bronze/fda-cder/config.json");

    // Compute database names from prefix
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const dataset = datasetConfig.dataset;

    // Get worker configuration based on data size category
    const workerConfig = etlConfig.glue_worker_configs[datasetConfig.data_size_category];

    // Extract table names from source dataset configs
    // For single-table datasets, use the table name from file_table_mapping
    // Table names in Glue have hyphens converted to underscores
    const nsdeTableName = Object.values(fdaNsdeConfig.file_table_mapping)[0].replace(/-/g, '_');

    // For multi-table datasets like fda-cder, get table names for products and packages
    const cderTableNames = Object.values(fdaCderConfig.file_table_mapping).map(name => name.replace(/-/g, '_'));
    const cderProductsTable = cderTableNames.find(name => name.includes('products'));
    const cderPackagesTable = cderTableNames.find(name => name.includes('packages'));

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

        // Source table names from dataset configs
        '--nsde_table': nsdeTableName,
        '--cder_products_table': cderProductsTable,
        '--cder_packages_table': cderPackagesTable,

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

module.exports = { FdaAllNdcsStack };