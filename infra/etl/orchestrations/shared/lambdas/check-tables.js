/**
 * Lambda function to check if Glue tables exist in the Data Catalog
 * Used by Step Functions orchestration to determine if crawlers need to run
 *
 * Input: { database, tables, tableToCrawlerMap }
 * Output: { exists, missingTables, existingTables, crawlersNeeded }
 */
const { GlueClient, GetTablesCommand } = require("@aws-sdk/client-glue");

const glue = new GlueClient({});

exports.handler = async (event) => {
  const { database, tables, tableToCrawlerMap } = event;

  // Input validation
  if (
    typeof database !== "string" ||
    !Array.isArray(tables) ||
    tables.length === 0
  ) {
    throw new Error(
      "Invalid input: must include { database: string, tables: string[] }"
    );
  }

  console.log(`Checking ${tables.length} tables in database: ${database}`);

  try {
    // Fetch all tables with pagination support (handles >1000 tables)
    let existingTableNames = [];
    let nextToken;

    do {
      const response = await glue.send(
        new GetTablesCommand({
          DatabaseName: database,
          MaxResults: 1000,
          NextToken: nextToken,
        })
      );

      existingTableNames.push(
        ...(response.TableList || []).map((t) => t.Name)
      );
      nextToken = response.NextToken;
    } while (nextToken);

    // Find missing tables
    const missingTables = tables.filter((t) => !existingTableNames.includes(t));
    const allExist = missingTables.length === 0;

    // Determine which crawlers are needed based on missing tables
    let crawlersNeeded = [];
    if (tableToCrawlerMap && missingTables.length > 0) {
      // Map missing tables to their crawlers (remove duplicates)
      const crawlerSet = new Set();
      missingTables.forEach((table) => {
        const crawler = tableToCrawlerMap[table];
        if (crawler) {
          crawlerSet.add(crawler);
        }
      });
      crawlersNeeded = Array.from(crawlerSet);
    }

    console.log(
      `Found ${existingTableNames.length} tables; ${missingTables.length} missing; ${crawlersNeeded.length} crawlers needed`
    );

    return {
      exists: allExist,
      missingTables,
      existingTables: existingTableNames,
      crawlersNeeded,
    };
  } catch (error) {
    console.error("Error checking tables:", error.message);

    // Handle database not found
    if (error.name === "EntityNotFoundException") {
      // All tables are missing, so we need all crawlers
      let crawlersNeeded = [];
      if (tableToCrawlerMap) {
        const crawlerSet = new Set(Object.values(tableToCrawlerMap));
        crawlersNeeded = Array.from(crawlerSet);
      }

      return {
        exists: false,
        missingTables: tables,
        crawlersNeeded,
        error: `Database ${database} not found`,
      };
    }

    // Re-throw unexpected errors for Step Functions to handle
    throw error;
  }
};
