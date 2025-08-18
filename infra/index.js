#!/usr/bin/env node

const cdk = require('aws-cdk-lib');
const { PpRawsEtlFdaNsdeStack } = require('./etl/fda/nsde/NsdeStack');

const app = new cdk.App();

// Simple single stack for NSDE ETL
const nsdeStack = new PpRawsEtlFdaNsdeStack(app, 'pp-raws-etl-fda-nsde', {
  description: 'Pocket Pharmacist RAWS NSDE ETL pipeline - simplified single stack',
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

// Future datasets can be added as separate stacks:
// const cderStack = new CderStack(app, 'PP-RAWS-CDER', {...});
// const rxnormStack = new RxNormStack(app, 'PP-RAWS-RxNorm', {...});