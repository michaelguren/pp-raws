const { Stack, Fn } = require('aws-cdk-lib');
const s3 = require('aws-cdk-lib/aws-s3');
const glue = require('aws-cdk-lib/aws-glue');
const iam = require('aws-cdk-lib/aws-iam');
const s3deploy = require('aws-cdk-lib/aws-s3-deployment');
const { makeParquetTable } = require('../../../shared/lib/cdk/tables');
const { makeCrawler } = require('../../../shared/lib/cdk/crawler');

class NsdeSilverEtlStack extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared resources from DataLakeStack
    const dataLakeBucketName = Fn.importValue('PP-RAWS-DataLake-DataLakeBucket');
    const glueScriptsBucketName = Fn.importValue('PP-RAWS-DataLake-GlueScriptsBucket');
    const glueDatabaseName = Fn.importValue('PP-RAWS-DataLake-GlueDatabase');
    const sharedGlueRoleArn = Fn.importValue('PP-RAWS-DataLake-SharedGlueRoleArn');

    // Import from Bronze stack
    const workflowName = Fn.importValue('PP-RAWS-NSDE-Bronze-ETL-Workflow');
    const bronzeJobName = Fn.importValue('PP-RAWS-NSDE-Bronze-ETL-BronzeJob');

    // Reference shared resources
    const glueScriptsBucket = s3.Bucket.fromBucketName(this, 'GlueScriptsBucket', glueScriptsBucketName);
    const glueRole = iam.Role.fromRoleArn(this, 'SharedGlueRole', sharedGlueRoleArn);

    // Deploy Silver Glue script
    new s3deploy.BucketDeployment(this, 'DeploySilverScript', {
      sources: [s3deploy.Source.asset(__dirname)],
      destinationBucket: glueScriptsBucket,
      destinationKeyPrefix: 'silver/nsde/',
      prune: false
    });

    // Silver NSDE tables
    const silverNsdeTable = makeParquetTable(this, 'SilverNsdeTable', {
      dbName: glueDatabaseName,
      s3Location: `s3://${dataLakeBucketName}/silver/nsde/data/`,
      tableName: 'silver_nsde',
      partitionKeys: [
        { name: 'version', type: 'string' },
        { name: 'version_date', type: 'date' }
      ],
      enableProjection: true
    });

    const silverNsdeMetadataTable = makeParquetTable(this, 'SilverNsdeMetadataTable', {
      dbName: glueDatabaseName,
      s3Location: `s3://${dataLakeBucketName}/silver/nsde/metadata/data/`,
      tableName: 'silver_nsde_metadata',
      partitionKeys: []
    });

    // Silver NSDE crawler
    const silverCrawler = makeCrawler(this, 'SilverCrawler', {
      name: 'pp-raws-nsde-silver-crawler',
      roleArn: sharedGlueRoleArn,
      dbName: glueDatabaseName,
      s3Targets: [{
        path: `s3://${dataLakeBucketName}/silver/nsde/data/`,
        exclusions: ["**/_SUCCESS*", "**/*.json", "**/.*/**"]
      }, {
        path: `s3://${dataLakeBucketName}/silver/nsde/metadata/data/`,
        exclusions: ["**/_SUCCESS*", "**/*.json", "**/.*/**"]
      }]
    });

    // Silver Glue Job
    const silverJob = new glue.CfnJob(this, 'SilverJob', {
      name: 'pp-raws-nsde-silver-etl',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${glueScriptsBucketName}/silver/nsde/nsde-silver-etl.py`,
        pythonVersion: '3'
      },
      defaultArguments: {
        '--bucket_name': dataLakeBucketName,
        '--database_name': glueDatabaseName,
        '--silver_prefix': 'silver/nsde/',
        '--TempDir': `s3://${glueScriptsBucketName}/temp/`,
        '--job-bookmark-option': 'job-bookmark-disable',
        '--enable-job-insights': 'true'
      },
      glueVersion: '4.0',
      timeout: 30,
      numberOfWorkers: 2,
      workerType: 'G.1X'
    });
    
    // Add explicit dependencies to ensure proper creation order
    silverJob.node.addDependency(silverNsdeTable);
    silverJob.node.addDependency(silverNsdeMetadataTable);

    // Conditional trigger for Silver job
    const conditionalTrigger = new glue.CfnTrigger(this, 'ConditionalTrigger', {
      name: 'pp-raws-nsde-workflow-conditional-trigger',
      type: 'CONDITIONAL',
      workflowName: workflowName,
      actions: [{ jobName: silverJob.ref }],
      predicate: {
        conditions: [{
          jobName: bronzeJobName,
          logicalOperator: 'EQUALS',
          state: 'SUCCEEDED'
        }]
      }
    });
    
    // Add explicit dependency to ensure proper creation order
    conditionalTrigger.node.addDependency(silverJob);

    // No outputs needed for v0.1 - focus on function over exports
  }
}

module.exports = { NsdeSilverEtlStack };