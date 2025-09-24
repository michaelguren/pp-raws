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

    // Load configurations
    const etlConfig = require("../utils-deploytime/EtlConfig");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    // Get everything from EtlConfig methods
    const databases = etlConfig.getDatabaseNames();
    const tables = Object.values(datasetConfig.file_table_mapping); // ['fda-products', 'fda-packages']
    const resourceNames = etlConfig.getResourceNames(dataset, tables);
    const paths = etlConfig.getS3Paths(bucketName, dataset, tables);
    const workerConfig = etlConfig.getWorkerConfig(datasetConfig.data_size_category);

    // Deploy Glue scripts to S3
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: path.posix.join("etl", dataset, "glue") + "/",
    });

    // Deploy shared runtime utilities to S3
    new s3deploy.BucketDeployment(this, "RuntimeUtils", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "..", "utils-runtime"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: "etl/utils-runtime/",
    });

    // Bronze Glue job for dual-table processing
    new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: paths.scriptLocation.bronze,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        ...etlConfig.getGlueJobArguments({
          dataset,
          bucketName,
          datasetConfig,
          layer: 'bronze'
        }),
        // Dynamic multi-table specific paths
        ...Object.fromEntries(
          tables.map(tableName => [
            `--bronze_${tableName.replace('-', '_')}_path`,
            paths.bronzeTables[tableName]
          ])
        ),
      },
    });

    // Bronze Products crawler (auto-discovers schema from parquet files)
    new glue.CfnCrawler(this, "BronzeProductsCrawler", {
      name: resourceNames.bronzeProductsCrawler,
      role: glueRole.roleArn,
      databaseName: databases.bronze,
      targets: {
        s3Targets: [
          {
            path: paths.bronzeTables.products
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
      databaseName: databases.bronze,
      targets: {
        s3Targets: [
          {
            path: paths.bronzeTables.packages
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