#!/usr/bin/env node

const cdk = require('aws-cdk-lib');
const { EtlCoreStack } = require('./etl/EtlCoreStack');
const { FdaNsdeStack } = require('./etl/fda-nsde/FdaNsdeStack');
const { FdaCderStack } = require('./etl/fda-cder/FdaCderStack');
const { FdaAllNdcStack } = require('./etl/fda-all-ndc/FdaAllNdcStack');

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

// Ensure dataset stacks depend on core infrastructure
fdaNsdeStack.addDependency(etlCoreStack);
fdaCderStack.addDependency(etlCoreStack);
fdaAllNdcStack.addDependency(etlCoreStack);

// GOLD layer depends on bronze layer data
fdaAllNdcStack.addDependency(fdaNsdeStack);
fdaAllNdcStack.addDependency(fdaCderStack);

// ===== API Infrastructure =====
// Future: API Gateway, Lambda functions, DynamoDB tables
// const apiStack = new PpApiStack(app, 'pp-api', {...});

// ===== Frontend Infrastructure =====  
// Future: S3 static hosting, CloudFront distribution
// const frontendStack = new PpFrontendStack(app, 'pp-frontend', {...});