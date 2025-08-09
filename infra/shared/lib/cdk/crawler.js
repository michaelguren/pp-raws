const glue = require('aws-cdk-lib/aws-glue');

/**
 * Helper function to create Glue crawlers with standard configuration
 * Crawlers update existing tables rather than creating new ones
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
    configuration: typeof configuration === 'object' 
      ? JSON.stringify(configuration) 
      : configuration
  };
  
  // Add schedule if provided
  if (schedule) {
    crawlerProps.schedule = schedule;
  }
  
  return new glue.CfnCrawler(scope, id, crawlerProps);
}

module.exports = { makeCrawler };