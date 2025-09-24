const cdk = require("aws-cdk-lib");

class FdaNsdeStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    const deployUtils = require("../utils-deploytime");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    const databases = deployUtils.getDatabaseNames();
    const tables = Object.values(datasetConfig.file_table_mapping);
    const resourceNames = deployUtils.getResourceNames(dataset, tables);
    const paths = deployUtils.getS3Paths(bucketName, dataset, tables);
    const workerConfig = deployUtils.getWorkerConfig(datasetConfig.data_size_category);

    // Bronze job using shared HTTP/ZIP processor
    deployUtils.createGlueJob(
      this,
      resourceNames.bronzeJob,
      glueRole,
      `s3://${bucketName}/etl/utils-runtime/https_zip/bronze_http_job.py`,
      workerConfig,
      deployUtils.getGlueJobArguments({
        dataset,
        bucketName,
        datasetConfig,
        layer: 'bronze',
        tables
      })
    );

    // Bronze crawler
    deployUtils.createBronzeCrawler(this, resourceNames.bronzeCrawler, glueRole, paths.bronze, databases.bronze);

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
