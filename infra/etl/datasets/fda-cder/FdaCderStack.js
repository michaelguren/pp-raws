const cdk = require("aws-cdk-lib");
const EtlStackFactory = require("../../shared/deploytime/factory");

/**
 * FDA CDER (National Drug Code Directory) ETL Stack
 * Multi-table dataset with Products and Packages tables
 */
class FdaCderStack extends cdk.Stack {
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

    // Optional: Add schedule trigger if enabled
    if (datasetConfig.schedule && datasetConfig.schedule.enabled) {
      const bronzeJobName = `pp-dw-bronze-${datasetConfig.dataset}`;
      factory.createScheduledTrigger(
        bronzeJobName,
        datasetConfig.schedule.expression,
        datasetConfig.schedule.enabled
      );
    }
  }
}

module.exports = { FdaCderStack };