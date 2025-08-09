#!/usr/bin/env node

const cdk = require('aws-cdk-lib');
const { DataLakeStack } = require('./shared/data-lake-stack');
const { NsdeEtlStack } = require('./etl/datasets/nsde/bronze/stack');

const app = new cdk.App();

// Deploy shared infrastructure first
const dataLakeStack = new DataLakeStack(app, 'PP-RAWS-DataLake', {
  description: 'Pocket Pharmacist RAWS shared data lake infrastructure (S3, Glue, IAM)',
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  },
  tags: {
    'Project': 'RAWS',
    'Component': 'DataLake',
    'Environment': 'Dev'
  }
});

// Deploy NSDE ETL pipeline (depends on shared infrastructure)
const nsdeEtlStack = new NsdeEtlStack(app, 'PP-RAWS-NSDE-ETL', {
  description: 'Pocket Pharmacist RAWS NSDE ETL pipeline for FDA drug data',
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  },
  tags: {
    'Project': 'RAWS',
    'Component': 'ETL',
    'Dataset': 'NSDE',
    'Environment': 'Dev'
  }
});

// Add explicit dependency - NSDE stack requires DataLake stack to be deployed first
nsdeEtlStack.addDependency(dataLakeStack);

// Future stacks can be added here
// const rxnormEtlStack = new RxnormEtlStack(app, 'PP-RAWS-RxNorm-ETL', {...});
// rxnormEtlStack.addDependency(dataLakeStack);