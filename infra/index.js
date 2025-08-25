#!/usr/bin/env node

const cdk = require('aws-cdk-lib');
const { PpDwEtlStack } = require('./etl/fda/nsde/NsdeStack');

const app = new cdk.App();

// Unified data warehouse ETL stack
const etlStack = new PpDwEtlStack(app, 'pp-dw-etl', {
  description: 'Pocket Pharmacist Data Warehouse ETL pipeline - unified stack',
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  }
});

// Future datasets will be added to this unified ETL stack by:
// 1. Adding dataset configs to config/ directories
// 2. Adding corresponding Glue jobs and lambdas
// All datasets share the same S3 bucket with prefix-based organization