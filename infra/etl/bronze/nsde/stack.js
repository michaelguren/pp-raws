const { Stack, CfnOutput, Fn } = require('aws-cdk-lib');
const s3 = require('aws-cdk-lib/aws-s3');
const glue = require('aws-cdk-lib/aws-glue');
const iam = require('aws-cdk-lib/aws-iam');
const s3deploy = require('aws-cdk-lib/aws-s3-deployment');
const { makeParquetTable } = require('../../../shared/lib/cdk/tables');
const { makeCrawler } = require('../../../shared/lib/cdk/crawler');

class NsdeBronzeEtlStack extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared resources from DataLakeStack
    const dataLakeBucketName = Fn.importValue('PP-RAWS-DataLake-DataLakeBucket');
    const glueScriptsBucketName = Fn.importValue('PP-RAWS-DataLake-GlueScriptsBucket');
    const glueDatabaseName = Fn.importValue('PP-RAWS-DataLake-GlueDatabase');
    const sharedGlueRoleArn = Fn.importValue('PP-RAWS-DataLake-SharedGlueRoleArn');

    // Reference shared resources
    const dataLakeBucket = s3.Bucket.fromBucketName(this, 'DataLakeBucket', dataLakeBucketName);
    const glueScriptsBucket = s3.Bucket.fromBucketName(this, 'GlueScriptsBucket', glueScriptsBucketName);
    const glueRole = iam.Role.fromRoleArn(this, 'SharedGlueRole', sharedGlueRoleArn);

    // Deploy Bronze Glue script
    new s3deploy.BucketDeployment(this, 'DeployBronzeScript', {
      sources: [s3deploy.Source.asset(__dirname)],
      destinationBucket: glueScriptsBucket,
      destinationKeyPrefix: 'bronze/nsde/',
      prune: false
    });

    // Bronze NSDE tables
    const bronzeNsdeTable = makeParquetTable(this, 'BronzeNsdeTable', {
      dbName: glueDatabaseName,
      s3Location: `s3://${dataLakeBucketName}/bronze/nsde/data/`,
      tableName: 'bronze_nsde',
      partitionKeys: [{ name: 'version_date', type: 'date' }],
      enableProjection: true,
      projectionConfig: {
        version_date: {
          type: 'date',
          format: 'yyyy-MM-dd',
          range: '2025-01-01,NOW'
        }
      }
    });

    const bronzeNsdeMetadataTable = makeParquetTable(this, 'BronzeNsdeMetadataTable', {
      dbName: glueDatabaseName,
      s3Location: `s3://${dataLakeBucketName}/bronze/nsde/metadata/data/`,
      tableName: 'bronze_nsde_metadata',
      partitionKeys: []
    });

    // Bronze NSDE crawler
    const bronzeCrawler = makeCrawler(this, 'BronzeCrawler', {
      name: 'pp-raws-nsde-bronze-crawler',
      roleArn: sharedGlueRoleArn,
      dbName: glueDatabaseName,
      s3Targets: [{
        path: `s3://${dataLakeBucketName}/bronze/nsde/data/`,
        exclusions: ["**/_SUCCESS*", "**/*.json", "**/.*/**"]
      }, {
        path: `s3://${dataLakeBucketName}/bronze/nsde/metadata/data/`,
        exclusions: ["**/_SUCCESS*", "**/*.json", "**/.*/**"]
      }]
    });

    // Bronze Glue Job
    const bronzeJob = new glue.CfnJob(this, 'BronzeJob', {
      name: 'pp-raws-nsde-bronze-etl',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${glueScriptsBucketName}/bronze/nsde/nsde-bronze-etl.py`,
        pythonVersion: '3'
      },
      defaultArguments: {
        '--bucket_name': dataLakeBucketName,
        '--database_name': glueDatabaseName,
        '--TempDir': `s3://${glueScriptsBucketName}/temp/`,
        '--job-bookmark-option': 'job-bookmark-disable'
      },
      glueVersion: '4.0',
      timeout: 30,
      numberOfWorkers: 2,
      workerType: 'G.1X'
    });

    // NSDE ETL Workflow
    const workflow = new glue.CfnWorkflow(this, 'Workflow', {
      name: 'pp-raws-nsde-etl-workflow'
    });

    // Scheduled start trigger
    const startTrigger = new glue.CfnTrigger(this, 'StartTrigger', {
      name: 'pp-raws-nsde-workflow-start-trigger',
      type: 'SCHEDULED',
      schedule: 'cron(0 3 * * ? *)',
      workflowName: workflow.ref,
      actions: [{ jobName: bronzeJob.ref }],
      startOnCreation: true
    });

    // Outputs
    new CfnOutput(this, 'WorkflowName', {
      value: workflow.ref,
      exportName: `${this.stackName}-Workflow`,
      description: 'NSDE ETL workflow name'
    });

    new CfnOutput(this, 'BronzeJobName', {
      value: bronzeJob.ref,
      exportName: `${this.stackName}-BronzeJob`,
      description: 'Bronze job name'
    });
  }
}

module.exports = { NsdeBronzeEtlStack };