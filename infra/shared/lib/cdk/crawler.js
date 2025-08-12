const glue = require('aws-cdk-lib/aws-glue');

/**
 * Helper function to create Glue crawlers with standard configuration
 * Crawlers update existing tables rather than creating new ones
 * 
 * @param {Object} scope - CDK construct scope
 * @param {string} id - CDK construct ID
 * @param {Object} props - Crawler configuration properties
 * @param {string} props.name - Crawler name
 * @param {string} props.roleArn - IAM role ARN for crawler
 * @param {string} props.dbName - Glue database name
 * @param {Array} props.s3Targets - Array of S3 target configurations
 * @param {Object} [props.configuration] - Optional crawler configuration (JSON object or string)
 * @param {Object} [props.schemaChangePolicy] - Optional schema change policy
 * @param {string} [props.schedule] - Optional schedule expression (if not provided, crawler must be run manually)
 * 
 * @example
 * // Manual crawler (no schedule)
 * const crawler = makeCrawler(this, 'MyCrawler', {
 *   name: 'my-crawler',
 *   roleArn: 'arn:aws:iam::123456789012:role/GlueRole',
 *   dbName: 'my_database',
 *   s3Targets: [{ path: 's3://bucket/data/' }]
 * });
 * // Run manually: aws glue start-crawler --name my-crawler
 * 
 * @example
 * // Scheduled crawler
 * const crawler = makeCrawler(this, 'MyCrawler', {
 *   name: 'my-crawler',
 *   roleArn: 'arn:aws:iam::123456789012:role/GlueRole',
 *   dbName: 'my_database',
 *   s3Targets: [{ path: 's3://bucket/data/' }],
 *   schedule: 'cron(0 12 * * ? *)' // Daily at noon
 * });
 */
function makeCrawler(scope, id, props) {
  const { 
    name, 
    roleArn, 
    dbName, 
    s3Targets, 
    configuration, 
    schemaChangePolicy,
    schedule 
  } = props;
  
  // Validation for required props
  if (!name) {
    throw new Error('makeCrawler: name is required');
  }
  if (!roleArn) {
    throw new Error('makeCrawler: roleArn is required');
  }
  if (!dbName) {
    throw new Error('makeCrawler: dbName is required');
  }
  if (!s3Targets || !Array.isArray(s3Targets) || s3Targets.length === 0) {
    throw new Error('makeCrawler: s3Targets must be a non-empty array');
  }
  
  // Validate each s3Target has required path property
  s3Targets.forEach((target, index) => {
    if (!target.path) {
      throw new Error(`makeCrawler: s3Targets[${index}] missing required 'path' property`);
    }
  });
  
  // Default configuration for consistent crawler behavior
  const defaultConfiguration = {
    Version: 1,
    CrawlerOutput: {
      Partitions: {
        AddOrUpdateBehavior: 'InheritFromTable'
      },
      Tables: {
        AddOrUpdateBehavior: 'MergeNewColumns'
      }
    },
    Grouping: {
      TableGroupingPolicy: 'CombineCompatibleSchemas'
    }
  };
  
  // Merge user configuration with defaults (allows partial overrides)
  const finalConfiguration = configuration 
    ? Object.assign({}, defaultConfiguration, configuration)
    : defaultConfiguration;
  
  const crawlerProps = {
    name: name,
    role: roleArn,
    databaseName: dbName,
    targets: { 
      s3Targets: s3Targets
    },
    schemaChangePolicy: schemaChangePolicy || { 
      updateBehavior: 'UPDATE_IN_DATABASE', 
      deleteBehavior: 'LOG' 
    },
    configuration: typeof finalConfiguration === 'object' 
      ? JSON.stringify(finalConfiguration) 
      : finalConfiguration
  };
  
  // Add schedule if provided
  if (schedule) {
    crawlerProps.schedule = schedule;
  }
  
  return new glue.CfnCrawler(scope, id, crawlerProps);
}

module.exports = { makeCrawler };