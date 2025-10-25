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

    // Gold table name (hyphens → underscores for Glue)
    const goldTableName = dataset.replace(/-/g, '_');

    // Build paths using convention - GOLD layer paths
    const goldBasePath = `s3://${bucketName}/gold/${dataset}/`;
    const goldScriptPath = `s3://${bucketName}/etl/${dataset}/glue/gold_job.py`;

    // Defensive validation for S3 path
    if (!goldBasePath.startsWith('s3://')) {
      throw new Error(`goldBasePath must be an S3 URI, got: ${goldBasePath}`);
    }

    // Temporal versioning library path
    const temporalLibPath = `s3://${bucketName}/etl/shared/runtime/temporal/`;

    // Deploy Glue script to S3
    const scriptDeployment = new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
      sources: [s3deploy.Source.asset(path.join(__dirname, 'glue'))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
      retainOnDelete: false
    });

    // Deploy Delta Lake temporal versioning library to S3
    const temporalDeployment = new s3deploy.BucketDeployment(this, 'DeployTemporalLib', {
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
        // Note: --datalake-formats=delta auto-configures Spark extensions and Delta catalog in Glue 5.0
        '--datalake-formats': 'delta',

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

        // Additional job-specific arguments
        '--enable-auto-scaling': 'true'
      }
    });

    // Ensure scripts are deployed before job creation (fixes race condition)
    goldJob.node.addDependency(scriptDeployment);
    goldJob.node.addDependency(temporalDeployment);
    goldJob.node.addDependency(dataWarehouseBucket);

    // Register Delta Lake table in Glue Data Catalog using pure CDK
    // This is a first-class CloudFormation resource - no Lambda/custom resource needed
    // The table registration is fully declarative and promotes cleanly across environments
    const goldTable = new glue.CfnTable(this, 'GoldTable', {
      databaseName: goldDatabase,
      catalogId: this.account,  // Resolves at deploy-time for multi-account compatibility
      tableInput: {
        name: goldTableName,
        description: `Gold layer Delta table for ${dataset} with SCD Type 2 temporal versioning`,

        // Note: tableType is inferred by Glue when table_type='DELTA', but we keep it explicit for clarity
        tableType: 'EXTERNAL_TABLE',

        // CRITICAL: These parameters enable Athena Engine v3 to recognize this as a native Delta table
        // Without these, Athena will treat it as regular Parquet and fail to read Delta transaction logs
        parameters: {
          'classification': 'delta',      // Identifies table format as Delta Lake
          'table_type': 'DELTA',           // Enables Athena v3 native Delta reads
          'EXTERNAL': 'TRUE',              // Marks table as externally managed (data lives in S3)
          'external': 'TRUE'               // Duplicate for AWS normalization across regions
        },

        // Storage descriptor with correct Parquet I/O formats for Delta Lake
        // Delta manages schema automatically via _delta_log transaction log, so no columns array needed
        //
        // Schema Evolution Note: If schema updates become required, Glue 5.0 + Delta supports
        // automatic catalog updates via Spark session config in the Glue job:
        //   spark.databricks.delta.schema.autoMerge.enabled = true
        // This allows schema changes without manual IaC updates.
        storageDescriptor: {
          location: goldBasePath,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
          }
          // columns array omitted - Delta Lake manages schema via transaction log
        }
      }
    });

    // CRITICAL: Remove the Columns property entirely from CloudFormation template
    // CDK/CloudFormation defaults columns to [] if not specified, which breaks Athena Delta reads
    // Athena v3 needs columns completely omitted so it can infer schema from _delta_log
    //
    // Post-deployment validation (run in Athena):
    //   SHOW CREATE TABLE pp_dw_gold.fda_all_ndcs;
    //   DESCRIBE pp_dw_gold.fda_all_ndcs;
    //   SELECT COUNT(*) FROM pp_dw_gold.fda_all_ndcs;
    goldTable.addPropertyDeletionOverride('TableInput.StorageDescriptor.Columns');

    // Ensure table is created after job to prevent race conditions
    goldTable.node.addDependency(goldJob);
    goldTable.node.addDependency(dataWarehouseBucket);

    // Create EventBridge rule for scheduling if enabled
    if (datasetConfig.schedule?.enabled) {
      // TODO: Add EventBridge scheduling when needed
    }

    // Export useful properties
    this.goldJob = goldJob;
    this.goldTable = goldTable;
    this.goldPath = goldBasePath;

    // CloudFormation outputs
    new cdk.CfnOutput(this, 'GoldJobName', {
      value: goldJob.name,
      description: 'Gold layer Glue job name (Delta Lake)'
    });

    new cdk.CfnOutput(this, 'GoldTableName', {
      value: goldTableName,
      description: 'Gold layer Delta table name'
    });

    new cdk.CfnOutput(this, 'GoldPath', {
      value: goldBasePath,
      description: 'S3 path for gold layer Delta table'
    });

    new cdk.CfnOutput(this, 'DeltaFormat', {
      value: 'Delta Lake 3.0.0 (Glue 5.0) - Declarative IaC',
      description: 'Delta Lake format and version with pure CDK registration'
    });
  }
}

module.exports = { GoldFdaAllNdcsStack };
