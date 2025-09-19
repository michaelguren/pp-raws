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
    
    // Construct resource names from warehouse conventions
    const resourceNames = {
      bronzeJob: `${etlConfig.warehouse_prefix}-bronze-${dataset}`,
      bronzeProductsCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-products-crawler`,
      bronzePackagesCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-packages-crawler`
    };

    // Get worker config based on dataset size category
    const sizeCategory = datasetConfig.data_size_category || 'medium';
    const workerConfig = etlConfig.glue_worker_configs[sizeCategory];
    
    if (!workerConfig) {
      throw new Error(`Invalid data_size_category: ${sizeCategory}. Must be one of: small, medium, large, xlarge`);
    }

    // Build S3 paths from warehouse config patterns  
    const bronzeProductsPath = etlConfig.path_patterns.bronze.replace('{dataset}', `${dataset}_products`);
    const bronzePackagesPath = etlConfig.path_patterns.bronze.replace('{dataset}', `${dataset}_packages`);
    
    // S3 path fragments from warehouse config patterns
    const rawPath = etlConfig.path_patterns.raw.replace('{dataset}', dataset).replace('run_id={run_id}/', '');
    const bronzePath = `bronze/${dataset}`;

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
        "--source_url": datasetConfig.source_url,
        "--bronze_database": etlConfig.bronze_database,
        "--raw_path": rawPath,
        "--bronze_path": bronzePath,
        "--date_format": datasetConfig.date_format,
        "--compression_codec": "zstd",
        "--bucket_name": bucketName,
        // Default logging arguments (from ETL config)
        ...etlConfig.glue_defaults.logging_arguments,
      },
    });

    // Bronze Products crawler (auto-discovers schema from parquet files)
    new glue.CfnCrawler(this, "BronzeProductsCrawler", {
      name: resourceNames.bronzeProductsCrawler,
      role: glueRole.roleArn,
      databaseName: etlConfig.bronze_database,
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
      databaseName: etlConfig.bronze_database,
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