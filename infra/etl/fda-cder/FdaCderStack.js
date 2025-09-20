const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

class FdaCderStack extends cdk.Stack {
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
      bronzeProductsCrawler: `${etlConfig.etl_resource_prefix}-bronze-${dataset}-products-crawler`,
      bronzePackagesCrawler: `${etlConfig.etl_resource_prefix}-bronze-${dataset}-packages-crawler`
    };

    // Get worker config based on dataset size category
    const sizeCategory = datasetConfig.data_size_category || 'medium';
    const workerConfig = etlConfig.glue_worker_configs[sizeCategory];
    
    if (!workerConfig) {
      throw new Error(`Invalid data_size_category: ${sizeCategory}. Must be one of: small, medium, large, xlarge`);
    }

    // S3 paths using convention
    const bronzeProductsPath = `bronze/${dataset}_products/`;
    const bronzePackagesPath = `bronze/${dataset}_packages/`;

    // S3 path fragments using convention
    const rawPath = `raw/${dataset}/`;
    const bronzePath = `bronze/${dataset}/`;

    // Deploy Glue scripts to S3
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
    });

    // Bronze Glue job for dual-table processing
    new glue.CfnJob(this, "BronzeJob", {
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
        "--source_url": datasetConfig.source_url,
        "--bronze_database": bronzeDatabase,
        "--raw_path": rawPath,
        "--bronze_path": bronzePath,
        "--date_format": datasetConfig.date_format,
        "--compression_codec": "zstd",
        "--bucket_name": bucketName,
        // Default arguments (from ETL config)
        ...etlConfig.glue_defaults.default_arguments,
      },
    });

    // Bronze Products crawler (auto-discovers schema from parquet files)
    new glue.CfnCrawler(this, "BronzeProductsCrawler", {
      name: resourceNames.bronzeProductsCrawler,
      role: glueRole.roleArn,
      databaseName: bronzeDatabase,
      targets: {
        s3Targets: [
          {
            path: `s3://${bucketName}/${bronzeProductsPath}`
          }
        ]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });

    // Bronze Packages crawler (auto-discovers schema from parquet files)
    new glue.CfnCrawler(this, "BronzePackagesCrawler", {
      name: resourceNames.bronzePackagesCrawler,
      role: glueRole.roleArn,
      databaseName: bronzeDatabase,
      targets: {
        s3Targets: [
          {
            path: `s3://${bucketName}/${bronzePackagesPath}`
          }
        ]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });

    // Outputs
    new cdk.CfnOutput(this, "BronzeJobName", {
      value: resourceNames.bronzeJob,
      description: "Complete CDER ETL Glue Job Name (download + transform dual tables)",
    });

    new cdk.CfnOutput(this, "BronzeProductsCrawlerName", {
      value: resourceNames.bronzeProductsCrawler,
      description: "Bronze Products Crawler Name",
    });

    new cdk.CfnOutput(this, "BronzePackagesCrawlerName", {
      value: resourceNames.bronzePackagesCrawler,
      description: "Bronze Packages Crawler Name",
    });

  }
}

module.exports = { FdaCderStack };