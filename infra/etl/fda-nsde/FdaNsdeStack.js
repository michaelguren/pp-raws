const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

class FdaNsdeStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    const etlConfig = require("../utils-deploytime/EtlConfig");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    const databases = etlConfig.getDatabaseNames();
    const tables = Object.values(datasetConfig.file_table_mapping);
    const resourceNames = etlConfig.getResourceNames(dataset, tables);
    const paths = etlConfig.getS3Paths(bucketName, dataset, tables);
    const workerConfig = etlConfig.getWorkerConfig(datasetConfig.data_size_category);

    // Deploy shared runtime utilities (TODO: Move to EtlCoreStack to avoid duplication)
    new s3deploy.BucketDeployment(this, "RuntimeUtils", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "..", "utils-runtime"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: "etl/utils-runtime/",
    });

    // Bronze job using shared HTTP/ZIP processor
    new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/utils-runtime/https_zip/bronze_http_job.py`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: etlConfig.getGlueJobArguments({
        dataset,
        bucketName,
        datasetConfig,
        layer: 'bronze',
        tables
      }),
    });

    // Bronze crawler
    new glue.CfnCrawler(this, "BronzeCrawler", {
      name: resourceNames.bronzeCrawler,
      role: glueRole.roleArn,
      databaseName: databases.bronze,
      targets: { s3Targets: [{ path: paths.bronze }] },
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
      description: "Bronze Glue Job Name"
    });

    new cdk.CfnOutput(this, "BronzeCrawlerName", {
      value: resourceNames.bronzeCrawler,
      description: "Bronze Crawler Name"
    });
  }
}

module.exports = { FdaNsdeStack };
