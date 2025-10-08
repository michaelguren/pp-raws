const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

/**
 * Drug Product Codesets Gold Layer Stack
 * Combines FDA NDC (silver) + RxNORM Products (silver) via RXNSAT bronze table
 * Gold layer represents the final, analytics-ready dataset with unified field prefixes
 */
class DrugProductCodesetsStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse and dataset configurations
    const etlConfig = require("../../config.json");
    const datasetConfig = require("./config.json");

    // Load source dataset configs to get table names
    const fdaAllNdcConfig = require("../fda-all-ndc/config.json");
    const rxnormProductsConfig = require("../rxnorm-products/config.json");
    const rxnormConfig = require("../rxnorm/config.json");

    // Compute database names from prefix
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;
    const dataset = datasetConfig.dataset;

    // Get worker configuration based on data size category
    const workerConfig = etlConfig.glue_worker_configs[datasetConfig.data_size_category];

    // Extract table names - Gold layer reads from silver/bronze
    const fdaAllNdcTable = fdaAllNdcConfig.dataset.replace(/-/g, '_');
    const rxnormProductsTable = rxnormProductsConfig.output_table;

    // RxNORM bronze table (RXNSAT for NDC mappings)
    const rxnsatTable = 'rxnsat'; // Bronze RxNORM tables use original RRF names (lowercase)

    // Build paths using convention - GOLD layer paths
    const goldBasePath = `s3://${bucketName}/gold/${dataset}/`;
    const goldScriptPath = `s3://${bucketName}/etl/datasets/${dataset}/glue/gold_job.py`;

    // Deploy Glue script to S3
    new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
      sources: [s3deploy.Source.asset(path.join(__dirname, 'glue'))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/datasets/${dataset}/glue/`,
      retainOnDelete: false
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

        // Dataset and database configuration
        '--dataset': dataset,
        '--bucket_name': bucketName,
        '--bronze_database': bronzeDatabase,
        '--silver_database': silverDatabase,
        '--gold_database': goldDatabase,

        // Pre-computed paths
        '--gold_base_path': goldBasePath,

        // Source table names from dataset configs
        '--fda_all_ndc_table': fdaAllNdcTable,
        '--rxnorm_products_table': rxnormProductsTable,
        '--rxnsat_table': rxnsatTable,

        // Output table name
        '--output_table': datasetConfig.output_table,

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
      description: `Crawler for GOLD ${dataset} parquet files`,
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

module.exports = { DrugProductCodesetsStack };
