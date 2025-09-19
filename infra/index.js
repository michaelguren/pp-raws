#!/usr/bin/env node

const cdk = require('aws-cdk-lib');
const { EtlCoreStack } = require('./etl/EtlCoreStack');
const { FdaNsdeStack } = require('./etl/fda-nsde/FdaNsdeStack');
const { FdaCderStack } = require('./etl/fda-cder/FdaCderStack');
const { FdaAllNdcStack } = require('./etl/fda-all-ndc/FdaAllNdcStack');
const { RxnormStack } = require('./etl/rxnorm/RxnormStack');
const { RxnormSplMappingsStack } = require('./etl/rxnorm-spl-mappings/RxnormSplMappingsStack');
const { RxClassStack } = require('./etl/rxclass/RxClassStack');

const app = new cdk.App();

// ===== ETL Infrastructure =====
// Core ETL infrastructure (shared S3 bucket, databases, IAM roles)
const etlCoreStack = new EtlCoreStack(app, 'pp-dw-etl-core', {
  description: 'Shared ETL infrastructure - S3 bucket, databases, IAM roles',
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// Dataset-specific ETL stacks
const fdaNsdeStack = new FdaNsdeStack(app, 'pp-dw-etl-fda-nsde', {
  description: 'FDA NSDE dataset ETL pipeline',
  etlCoreStack: etlCoreStack,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// FDA CDER dataset ETL stack
const fdaCderStack = new FdaCderStack(app, 'pp-dw-etl-fda-cder', {
  description: 'FDA CDER NDC dataset ETL pipeline with dual tables',
  etlCoreStack: etlCoreStack,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// FDA All NDC GOLD layer stack (combines NSDE + CDER)
const fdaAllNdcStack = new FdaAllNdcStack(app, 'pp-dw-etl-fda-all-ndc', {
  description: 'FDA All NDC GOLD layer - combines NSDE and CDER data',
  etlCoreStack: etlCoreStack,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// RxNORM dataset ETL stack
const rxnormStack = new RxnormStack(app, 'pp-dw-etl-rxnorm', {
  description: 'RxNORM ETL pipeline with RRF tables and RxCUI/TTY mapping',
  etlCoreStack: etlCoreStack,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// RxNORM SPL Mappings dataset ETL stack
const rxnormSplMappingsStack = new RxnormSplMappingsStack(app, 'pp-dw-etl-rxnorm-spl-mappings', {
  description: 'RxNORM SPL Mappings ETL pipeline - DailyMed SET ID mappings',
  etlCoreStack: etlCoreStack,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// RxClass dataset ETL stack
const rxClassStack = new RxClassStack(app, 'pp-dw-etl-rxclass', {
  description: 'RxClass drug classification ETL pipeline from NLM RxNav API',
  etlCoreStack: etlCoreStack,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// Ensure dataset stacks depend on core infrastructure
fdaNsdeStack.addDependency(etlCoreStack);
fdaCderStack.addDependency(etlCoreStack);
fdaAllNdcStack.addDependency(etlCoreStack);
rxnormStack.addDependency(etlCoreStack);
rxnormSplMappingsStack.addDependency(etlCoreStack);
rxClassStack.addDependency(etlCoreStack);

// GOLD layer depends on bronze layer data
fdaAllNdcStack.addDependency(fdaNsdeStack);
fdaAllNdcStack.addDependency(fdaCderStack);

// ===== API Infrastructure =====
// Future: API Gateway, Lambda functions, DynamoDB tables
// const apiStack = new PpApiStack(app, 'pp-api', {...});

// ===== Frontend Infrastructure =====  
// Future: S3 static hosting, CloudFront distribution
// const frontendStack = new PpFrontendStack(app, 'pp-frontend', {...});