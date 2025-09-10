#!/usr/bin/env node

const cdk = require('aws-cdk-lib');
const { EtlCoreStack } = require('./etl/EtlCoreStack');
const { FdaNsdeStack } = require('./etl/fda-nsde/FdaNsdeStack');

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
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// Ensure NSDE stack depends on core infrastructure
fdaNsdeStack.addDependency(etlCoreStack);

// ===== API Infrastructure =====
// Future: API Gateway, Lambda functions, DynamoDB tables
// const apiStack = new PpApiStack(app, 'pp-api', {...});

// ===== Frontend Infrastructure =====  
// Future: S3 static hosting, CloudFront distribution
// const frontendStack = new PpFrontendStack(app, 'pp-frontend', {...});