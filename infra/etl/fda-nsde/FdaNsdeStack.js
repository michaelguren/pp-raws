const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");

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
    etlConfig.createBronzeCrawler(this, resourceNames.bronzeCrawler, glueRole, paths.bronze, databases.bronze);

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
