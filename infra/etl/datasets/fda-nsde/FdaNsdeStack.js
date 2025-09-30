const cdk = require("aws-cdk-lib");
const EtlStackFactory = require("../../shared/deploytime/factory");

/**
 * FDA NSDE (Comprehensive NDC SPL Data Elements) ETL Stack
 * Single-table dataset with HTTP/ZIP source
 */
class FdaNsdeStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Load configuration
    const datasetConfig = require("./config.json");

    // Create factory instance
    const factory = new EtlStackFactory(this, props);

    // Create complete dataset infrastructure
    const { outputs } = factory.createDatasetInfrastructure({
      datasetConfig,
      options: {
        skipGoldJob: true,  // No gold layer for this dataset yet
        includeGoldCrawler: false
      }
    });

    // Add any custom outputs if needed
    factory.createStandardOutputs(datasetConfig.dataset, outputs);
  }
}

module.exports = { FdaNsdeStack };