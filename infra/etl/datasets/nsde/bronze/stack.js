const { Stack, Duration, RemovalPolicy, CfnOutput, Fn } = require('aws-cdk-lib');
const s3 = require('aws-cdk-lib/aws-s3');
const glue = require('aws-cdk-lib/aws-glue');
const iam = require('aws-cdk-lib/aws-iam');
const s3deploy = require('aws-cdk-lib/aws-s3-deployment');
const path = require('path');

class NsdeEtlStack extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared resources from DataLakeStack
    const dataLakeBucketName = Fn.importValue('PP-RAWS-DataLake-DataLakeBucket');
    const glueScriptsBucketName = Fn.importValue('PP-RAWS-DataLake-GlueScriptsBucket');
    const glueDatabaseName = Fn.importValue('PP-RAWS-DataLake-GlueDatabase');
    const sharedGlueRoleArn = Fn.importValue('PP-RAWS-DataLake-SharedGlueRoleArn');

    // Reference the shared buckets
    const dataLakeBucket = s3.Bucket.fromBucketName(this, 'DataLakeBucket', dataLakeBucketName);
    const glueScriptsBucket = s3.Bucket.fromBucketName(this, 'GlueScriptsBucket', glueScriptsBucketName);

    // Reference the shared Glue role
    const glueRole = iam.Role.fromRoleArn(this, 'SharedGlueRole', sharedGlueRoleArn);

    // Deploy NSDE-specific Glue script to shared scripts bucket
    new s3deploy.BucketDeployment(this, 'DeployNsdeGlueScript', {
      sources: [s3deploy.Source.asset(path.join(__dirname, 'glue-scripts'))],
      destinationBucket: glueScriptsBucket,
      destinationKeyPrefix: 'nsde/', // Organize scripts by dataset
      prune: false // Don't delete other scripts
    });

    // NSDE-specific Glue Job (download from FDA and transform to bronze)
    const nsdeGlueJob = new glue.CfnJob(this, 'NsdeEtlJob', {
      name: 'pp-raws-nsde-etl',
      description: 'ETL job for FDA NSDE data: download, cleanse, and store in bronze layer',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${glueScriptsBucketName}/nsde/nsde-bronze-etl.py`,
        pythonVersion: '3'
      },
      defaultArguments: {
        '--bucket_name': dataLakeBucketName,
        '--database_name': glueDatabaseName,
        '--TempDir': `s3://${glueScriptsBucketName}/temp/`,
        '--enable-metrics': '',
        '--enable-continuous-cloudwatch-log': 'true',
        '--enable-continuous-log-filter': 'true',
        '--job-bookmark-option': 'job-bookmark-disable', // Fresh data each run
        '--enable-glue-datacatalog': ''
      },
      glueVersion: '4.0',
      maxRetries: 1,
      timeout: 30, // 30 minutes for download + transform
      numberOfWorkers: 2,
      workerType: 'G.1X',
      executionProperty: {
        maxConcurrentRuns: 1 // Prevent overlapping runs
      },
      tags: {
        'Dataset': 'NSDE',
        'Source': 'FDA',
        'Environment': 'Dev'
      }
    });

    // Daily Schedule Trigger for NSDE ETL
    const nsdeTrigger = new glue.CfnTrigger(this, 'NsdeDailyTrigger', {
      name: 'pp-raws-nsde-daily-trigger',
      description: 'Daily trigger for NSDE ETL job at 3 AM UTC',
      type: 'SCHEDULED',
      schedule: 'cron(0 3 * * ? *)', // Daily at 3 AM UTC
      actions: [{
        jobName: nsdeGlueJob.ref
        // Date handling done in Glue script using datetime.utcnow()
      }],
      startOnCreation: false // Set to true when ready for automated runs
    });

    // Outputs
    new CfnOutput(this, 'NsdeGlueJobName', {
      value: nsdeGlueJob.ref,
      description: 'NSDE Glue ETL job name'
    });

    new CfnOutput(this, 'NsdeTriggerName', {
      value: nsdeTrigger.ref,
      description: 'NSDE daily trigger name'
    });

    new CfnOutput(this, 'NsdeScriptLocation', {
      value: `s3://${glueScriptsBucketName}/nsde/nsde-bronze-etl.py`,
      description: 'Location of NSDE Glue script'
    });

    new CfnOutput(this, 'NsdeBronzeLocation', {
      value: `s3://${dataLakeBucketName}/bronze/nsde/`,
      description: 'S3 location for NSDE bronze data'
    });
  }
}

module.exports = { NsdeEtlStack };