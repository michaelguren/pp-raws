const glue = require("aws-cdk-lib/aws-glue");
const cdk = require("aws-cdk-lib");

/**
 * Factory class for creating standardized CDK resources for ETL pipelines
 * Provides reusable methods for creating Glue jobs, crawlers, and related resources
 */
class EtlStackFactory {
  constructor(stack, props) {
    this.stack = stack;
    this.dataWarehouseBucket = props.etlCoreStack.dataWarehouseBucket;
    this.glueRole = props.etlCoreStack.glueRole;
    this.bucketName = this.dataWarehouseBucket.bucketName;

    // Import base utilities
    this.deployUtils = require("./index");
  }

  /**
   * Create a complete dataset infrastructure including jobs and crawlers
   * @param {Object} config - Dataset configuration including dataset name, config, and options
   */
  createDatasetInfrastructure(config) {
    const { datasetConfig, options = {} } = config;
    const dataset = datasetConfig.dataset;

    // Extract tables from file mapping
    const tables = Object.values(datasetConfig.file_table_mapping || {});
    if (!tables.length) {
      throw new Error(`No tables defined in file_table_mapping for dataset ${dataset}`);
    }

    // Get resource names and paths
    const resourceNames = this.deployUtils.getResourceNames(dataset, tables);
    const paths = this.deployUtils.getS3Paths(this.bucketName, dataset, tables);

    // Create bronze job if not disabled
    if (!options.skipBronzeJob) {
      this.createBronzeJob(dataset, datasetConfig, tables);
    }

    // Create gold job if specified
    if (options.includeGoldJob) {
      this.createGoldJob(dataset, datasetConfig, tables);
    }

    // Create crawlers
    const outputs = this.createCrawlers(dataset, tables, resourceNames, paths, options);

    return { resourceNames, paths, outputs };
  }

  /**
   * Create bronze layer Glue job
   */
  createBronzeJob(dataset, datasetConfig, tables) {
    return this.deployUtils.createGlueJob(this.stack, {
      dataset,
      bucketName: this.bucketName,
      datasetConfig,
      layer: 'bronze',
      tables,
      glueRole: this.glueRole,
      workerSize: datasetConfig.data_size_category || 'medium'
    });
  }

  /**
   * Create gold layer Glue job
   */
  createGoldJob(dataset, datasetConfig, tables, scriptLocation) {
    return this.deployUtils.createGlueJob(this.stack, {
      dataset,
      bucketName: this.bucketName,
      datasetConfig,
      layer: 'gold',
      tables,
      glueRole: this.glueRole,
      workerSize: datasetConfig.data_size_category || 'medium',
      scriptLocation // Optional custom script location
    });
  }

  /**
   * Create all necessary crawlers for a dataset
   */
  createCrawlers(dataset, tables, resourceNames, paths, options = {}) {
    const outputs = {};

    // Bronze crawlers (always created unless disabled)
    if (!options.skipBronzeCrawlers) {
      const bronzeOutputs = this.deployUtils.createBronzeCrawlers(
        this.stack,
        dataset,
        tables,
        this.glueRole,
        resourceNames,
        paths
      );
      Object.assign(outputs, bronzeOutputs);
    }

    // Gold crawler (if requested)
    if (options.includeGoldCrawler) {
      this.createGoldCrawler(dataset, resourceNames, paths);
      outputs.GoldCrawlerName = new cdk.CfnOutput(this.stack, "GoldCrawlerName", {
        value: resourceNames.goldCrawler,
        description: "Gold Crawler Name",
      });
    }

    return outputs;
  }

  /**
   * Create gold layer crawler
   */
  createGoldCrawler(dataset, resourceNames, paths) {
    const crawlerConfig = JSON.stringify({
      Version: 1.0,
      CrawlerOutput: {
        Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
        Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
      }
    });

    return new glue.CfnCrawler(this.stack, "GoldCrawler", {
      name: resourceNames.goldCrawler,
      role: this.glueRole.roleArn,
      databaseName: resourceNames.goldDatabase,
      targets: { s3Targets: [{ path: paths.gold }] },
      configuration: crawlerConfig
    });
  }

  /**
   * Create a scheduled trigger for a Glue job
   */
  createScheduledTrigger(jobName, schedule, enabled = false) {
    const triggerName = `${jobName}-trigger`;

    return new glue.CfnTrigger(this.stack, `${jobName}Trigger`, {
      name: triggerName,
      type: "SCHEDULED",
      schedule: schedule,
      startOnCreation: enabled,
      actions: [{
        jobName: jobName,
        arguments: {}
      }]
    });
  }

  /**
   * Create outputs for common resources
   */
  createStandardOutputs(dataset, resourceNames) {
    const outputs = {};

    // Dataset name output
    outputs.DatasetName = new cdk.CfnOutput(this.stack, "DatasetName", {
      value: dataset,
      description: "Dataset name for this stack"
    });

    // Bucket output
    outputs.BucketName = new cdk.CfnOutput(this.stack, "BucketName", {
      value: this.bucketName,
      description: "S3 bucket for data storage"
    });

    return outputs;
  }

  /**
   * Deploy Glue scripts to S3 (for custom scripts)
   */
  deployGlueScripts(dataset, scriptFiles = []) {
    const s3 = require("aws-cdk-lib/aws-s3");
    const s3Deploy = require("aws-cdk-lib/aws-s3-deployment");
    const path = require("path");

    if (!scriptFiles.length) return;

    const deploymentSource = s3Deploy.Source.asset(
      path.join(__dirname, "..", "..", "datasets", dataset, "glue")
    );

    return new s3Deploy.BucketDeployment(this.stack, "GlueScriptDeployment", {
      sources: [deploymentSource],
      destinationBucket: this.dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
      prune: false,
      retainOnDelete: false
    });
  }
}

module.exports = EtlStackFactory;