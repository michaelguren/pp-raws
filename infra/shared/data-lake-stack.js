const { Stack, RemovalPolicy, CfnOutput, Duration } = require('aws-cdk-lib');
const s3 = require('aws-cdk-lib/aws-s3');
const glue = require('aws-cdk-lib/aws-glue');
const iam = require('aws-cdk-lib/aws-iam');
const events = require('aws-cdk-lib/aws-events');
const targets = require('aws-cdk-lib/aws-events-targets');
const s3n = require('aws-cdk-lib/aws-s3-notifications');

class DataLakeStack extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Shared S3 Data Lake Bucket
    const dataLakeBucket = new s3.Bucket(this, 'DataLakeBucket', {
      bucketName: `pp-raws-data-lake-${this.account}-${this.region}`,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          abortIncompleteMultipartUploadAfter: Duration.days(7),
          enabled: true
        },
        {
          // Clean up ETL markers after 30 days
          id: 'CleanupETLMarkers',
          prefix: 'bronze/nsde/markers/',
          expiration: Duration.days(30),
          enabled: true
        },
        {
          // Clean up Silver ETL markers after 30 days
          id: 'CleanupSilverETLMarkers',
          prefix: 'silver/nsde/markers/',
          expiration: Duration.days(30),
          enabled: true
        }
      ]
    });

    // Shared Glue Scripts Bucket
    const glueScriptsBucket = new s3.Bucket(this, 'GlueScriptsBucket', {
      bucketName: `pp-raws-glue-scripts-${this.account}-${this.region}`,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [{
        abortIncompleteMultipartUploadAfter: Duration.days(7),
        enabled: true
      }]
    });

    // Shared IAM Role for Glue Jobs
    const sharedGlueRole = new iam.Role(this, 'SharedGlueRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
      ]
    });

    // Grant S3 permissions to shared Glue role
    dataLakeBucket.grantReadWrite(sharedGlueRole);
    glueScriptsBucket.grantReadWrite(sharedGlueRole);
    
    // Add specific permissions for ETL markers and CloudWatch
    sharedGlueRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3:GetObject',
        's3:PutObject',
        's3:ListBucket'
      ],
      resources: [
        `arn:aws:s3:::${dataLakeBucket.bucketName}/bronze/*/markers/*`,
        `arn:aws:s3:::${dataLakeBucket.bucketName}/silver/*/markers/*`
      ]
    }));
    
    // CloudWatch metrics permissions
    sharedGlueRole.addToPolicy(new iam.PolicyStatement({
      actions: ['cloudwatch:PutMetricData'],
      resources: ['*'],
      conditions: {
        StringEquals: {
          'cloudwatch:namespace': 'NSDE/ETL'
        }
      }
    }));
    
    // Glue workflow properties permissions
    sharedGlueRole.addToPolicy(new iam.PolicyStatement({
      actions: ['glue:PutWorkflowRunProperties'],
      resources: [`arn:aws:glue:${this.region}:${this.account}:workflow/*`]
    }));

    // Shared Glue Database
    const glueDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'pp_raws_data_lake',
        locationUri: `s3://${dataLakeBucket.bucketName}/`
      }
    });

    // S3 Event Notification to EventBridge for Bronze data writes
    dataLakeBucket.enableEventBridgeNotification();

    // Minimal exports for dataset stacks
    new CfnOutput(this, 'DataLakeBucketName', { 
      value: dataLakeBucket.bucketName, 
      exportName: `${this.stackName}-DataLakeBucket`
    });

    new CfnOutput(this, 'GlueScriptsBucketName', { 
      value: glueScriptsBucket.bucketName, 
      exportName: `${this.stackName}-GlueScriptsBucket`
    });

    new CfnOutput(this, 'GlueDatabaseName', { 
      value: glueDatabase.ref, 
      exportName: `${this.stackName}-GlueDatabase`
    });

    new CfnOutput(this, 'SharedGlueRoleArn', { 
      value: sharedGlueRole.roleArn, 
      exportName: `${this.stackName}-SharedGlueRoleArn`
    });
  }
}

module.exports = { DataLakeStack };