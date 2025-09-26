const cdk = require("aws-cdk-lib");

class FdaCderStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    const deployUtils = require("../utils-deploytime");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    const tables = Object.values(datasetConfig.file_table_mapping);
    const resourceNames = deployUtils.getResourceNames(dataset, tables);
    const paths = deployUtils.getS3Paths(bucketName, dataset, tables);

    // Bronze job using shared HTTP/ZIP processor
    deployUtils.createGlueJob(this, {
      dataset,
      bucketName,
      datasetConfig,
      layer: "bronze",
      tables,
      glueRole,
      workerSize: datasetConfig.data_size_category,
    });

    // Create crawlers and outputs (handles both single and multi-table datasets)
    deployUtils.createBronzeCrawlers(this, dataset, tables, glueRole, resourceNames, paths);
  }
}

module.exports = { FdaCderStack };
