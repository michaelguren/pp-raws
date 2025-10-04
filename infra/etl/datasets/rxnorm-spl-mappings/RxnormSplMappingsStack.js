const cdk = require("aws-cdk-lib");
const EtlStackFactory = require("../../shared/deploytime/factory");

/**
 * RxNORM SPL Mappings ETL Stack
 * Single-table dataset with HTTP/ZIP source from DailyMed
 */
class RxnormSplMappingsStack extends cdk.Stack {
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
        skipGoldJob: true,  // No gold layer for this dataset
        includeGoldCrawler: false
      }
    });

    // Add any custom outputs if needed
    factory.createStandardOutputs(datasetConfig.dataset, outputs);
  }
}

module.exports = { RxnormSplMappingsStack };
