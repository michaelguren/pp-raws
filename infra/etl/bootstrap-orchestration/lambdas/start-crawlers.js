/**
 * Lambda function to start Glue crawlers
 * Used by Step Functions orchestration after jobs complete
 *
 * Input: { crawlerNames: string[] }
 * Output: { started: string[], failed: string[] }
 */
const { GlueClient, StartCrawlerCommand } = require("@aws-sdk/client-glue");

const glue = new GlueClient({});

exports.handler = async (event) => {
  const { crawlerNames } = event;

  // Input validation
  if (!Array.isArray(crawlerNames) || crawlerNames.length === 0) {
    throw new Error("Invalid input: must include { crawlerNames: string[] }");
  }

  console.log(`Starting ${crawlerNames.length} crawlers: ${crawlerNames.join(", ")}`);

  const started = [];
  const failed = [];

  // Start each crawler in parallel
  const promises = crawlerNames.map(async (crawlerName) => {
    try {
      await glue.send(
        new StartCrawlerCommand({
          Name: crawlerName,
        })
      );
      started.push(crawlerName);
      console.log(`Started crawler: ${crawlerName}`);
    } catch (error) {
      failed.push(crawlerName);
      console.error(`Failed to start crawler ${crawlerName}:`, error.message);
    }
  });

  await Promise.all(promises);

  return {
    started,
    failed,
    totalRequested: crawlerNames.length,
    totalStarted: started.length,
    totalFailed: failed.length,
  };
};
