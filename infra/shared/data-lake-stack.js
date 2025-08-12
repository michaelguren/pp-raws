const { Stack, RemovalPolicy, CfnOutput } = require('aws-cdk-lib');
const s3 = require('aws-cdk-lib/aws-s3');
const glue = require('aws-cdk-lib/aws-glue');
const iam = require('aws-cdk-lib/aws-iam');

class DataLakeStack extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Shared S3 Data Lake Bucket
    const dataLakeBucket = new s3.Bucket(this, 'DataLakeBucket', {
      bucketName: `pp-raws-data-lake-${this.account}-${this.region}`,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [{
        abortIncompleteMultipartUploadDays: 7,
        enabled: true
      }]
    });

    // Shared Glue Scripts Bucket
    const glueScriptsBucket = new s3.Bucket(this, 'GlueScriptsBucket', {
      bucketName: `pp-raws-glue-scripts-${this.account}-${this.region}`,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [{
        abortIncompleteMultipartUploadDays: 7,
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

    // Shared Glue Database
    const glueDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'pp_raws_data_lake',
        locationUri: `s3://${dataLakeBucket.bucketName}/`
      }
    });

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