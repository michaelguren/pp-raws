#!/usr/bin/env node

const cdk = require('aws-cdk-lib');
const { DataLakeStack } = require('./shared/data-lake-stack');
const { NsdeBronzeEtlStack } = require('./etl/bronze/nsde/stack');
const { NsdeSilverEtlStack } = require('./etl/silver/nsde/stack');

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

// Deploy NSDE Bronze ETL pipeline (depends on shared infrastructure)
const nsdeBronzeEtlStack = new NsdeBronzeEtlStack(app, 'PP-RAWS-NSDE-Bronze-ETL', {
  description: 'Pocket Pharmacist RAWS NSDE Bronze ETL pipeline (minimal ingestion)',
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  },
  tags: {
    'Project': 'RAWS',
    'Component': 'ETL',
    'Dataset': 'NSDE',
    'Layer': 'Bronze',
    'Environment': 'Dev'
  }
});

// Deploy NSDE Silver ETL pipeline (depends on Bronze)
const nsdeSilverEtlStack = new NsdeSilverEtlStack(app, 'PP-RAWS-NSDE-Silver-ETL', {
  description: 'Pocket Pharmacist RAWS NSDE Silver ETL pipeline (cleansing, normalization, DQ)',
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1'
  },
  tags: {
    'Project': 'RAWS',
    'Component': 'ETL',
    'Dataset': 'NSDE',
    'Layer': 'Silver',
    'Environment': 'Dev'
  }
});

// Add explicit dependencies - Silver depends on Bronze, Bronze depends on DataLake
nsdeBronzeEtlStack.addDependency(dataLakeStack);
nsdeSilverEtlStack.addDependency(nsdeBronzeEtlStack);

// Future stacks can be added here with layer-first structure
// v0.2: CDER Bronze and Silver
// const cderBronzeEtlStack = new CderBronzeEtlStack(app, 'PP-RAWS-CDER-Bronze-ETL', {...});
// const cderSilverEtlStack = new CderSilverEtlStack(app, 'PP-RAWS-CDER-Silver-ETL', {...});
// cderBronzeEtlStack.addDependency(dataLakeStack);
// cderSilverEtlStack.addDependency(cderBronzeEtlStack);

// v0.3: FDA NDC Combined (Silver layer join)
// const fdaNdcCombinedStack = new FdaNdcCombinedStack(app, 'PP-RAWS-FDA-NDC-Combined', {...});
// fdaNdcCombinedStack.addDependency(nsdeSilverEtlStack);
// fdaNdcCombinedStack.addDependency(cderSilverEtlStack);