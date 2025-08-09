const { Stack, Duration, RemovalPolicy, CfnOutput } = require('aws-cdk-lib');
const s3 = require('aws-cdk-lib/aws-s3');
const glue = require('aws-cdk-lib/aws-glue');
const iam = require('aws-cdk-lib/aws-iam');
const { makeParquetTable } = require('./lib/cdk/tables');
const { makeCrawler } = require('./lib/cdk/crawler');

class DataLakeStack extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);
    

    // Shared S3 Data Lake Bucket (medallion: raw/bronze/silver/gold)
    // RAWS convention: dataset prefixes e.g., /raw/nsde/, /raw/rxnorm/
    const dataLakeBucket = new s3.Bucket(this, 'DataLakeBucket', {
      bucketName: `pp-raws-data-lake-${this.account}-${this.region}`,
      versioned: true,
      removalPolicy: RemovalPolicy.DESTROY, // Dev only - change to RETAIN for production
      autoDeleteObjects: true, // Dev only - remove for production to prevent accidental data loss
      encryption: s3.BucketEncryption.S3_MANAGED, // SSE-S3 (AES256) for dev, upgrade to KMS in prod
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL, // Security: block all public access
      enforceSSL: true, // Security: deny unencrypted PUTs
      lifecycleRules: [
        {
          id: 'expire-raw-data',
          prefix: 'raw/',
          expiration: Duration.days(30),
          abortIncompleteMultipartUploadDays: 7,
          noncurrentVersionExpiration: Duration.days(90),
          enabled: true
        },
        {
          id: 'transition-bronze-to-ia',
          prefix: 'bronze/',
          transitions: [
            { storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: Duration.days(30) },
            { storageClass: s3.StorageClass.GLACIER_INSTANT_RETRIEVAL, transitionAfter: Duration.days(90) }
          ],
          abortIncompleteMultipartUploadDays: 7,
          noncurrentVersionExpiration: Duration.days(180),
          enabled: true
        }
        // Silver/Gold rules commented out for v0.1 - add when needed
        // {
        //   id: 'transition-silver-to-ia',
        //   prefix: 'silver/',
        //   transitions: [{ storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: Duration.days(60) }],
        //   abortIncompleteMultipartUploadDays: 7,
        //   enabled: true
        // },
        // {
        //   id: 'transition-gold-to-ia',
        //   prefix: 'gold/',
        //   transitions: [{ storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: Duration.days(90) }],
        //   abortIncompleteMultipartUploadDays: 7,
        //   enabled: true
        // }
      ]
      // CORS removed for v0.1 - add when app integration needed
    });

    // Shared Glue Scripts Bucket
    const glueScriptsBucket = new s3.Bucket(this, 'GlueScriptsBucket', {
      bucketName: `pp-raws-glue-scripts-${this.account}-${this.region}`,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        { 
          id: 'expire-temp-files', 
          prefix: 'temp/', 
          expiration: Duration.days(7),
          abortIncompleteMultipartUploadDays: 7,
          enabled: true 
        },
        { 
          id: 'expire-spark-logs', 
          prefix: 'spark-logs/', 
          expiration: Duration.days(14),
          abortIncompleteMultipartUploadDays: 7,
          enabled: true 
        }
      ]
    });

    // Shared IAM Role for Glue Jobs (reusable across all ETL pipelines)
    // RAWS: Single role for all Glue jobs to simplify permissions management
    const sharedGlueRole = new iam.Role(this, 'SharedGlueRole', {
      roleName: `raws-shared-glue-role-${this.region}`,
      description: 'RAWS shared Glue execution role for all ETL jobs',
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        // AWS managed policy provides Glue service permissions (CloudWatch, S3, EC2 for VPC)
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
      ]
    });

    // Grant S3 permissions to shared Glue role
    dataLakeBucket.grantReadWrite(sharedGlueRole);
    glueScriptsBucket.grantReadWrite(sharedGlueRole);

    // Add permissions for VPC/internet access if needed for downloads
    sharedGlueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'ec2:CreateNetworkInterface',
        'ec2:DescribeNetworkInterfaces', 
        'ec2:DeleteNetworkInterface'
      ],
      resources: ['*']
    }));

    // Shared Glue Database for Data Catalog (Athena queries)
    const glueDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'pp_raws_data_lake',
        description: 'Pocket Pharmacist RAWS shared data lake database for all domains (NSDE, RxNorm, etc.)',
        locationUri: `s3://${dataLakeBucket.bucketName}/`
        // Table-specific parameters (parquet, compression) set by crawler/jobs
      }
    });

    // Create explicit Glue tables (v0.1 - deterministic naming)
    // These tables pre-exist so crawler only updates schema/partitions
    
    // NSDE bronze data table - partitioned by version and version_date with partition projection
    const bronzeNsdeTable = makeParquetTable(this, 'Tablebronze_nsde', {
      dbName: glueDatabase.ref,
      s3Location: `s3://${dataLakeBucket.bucketName}/bronze/nsde/data/`,
      tableName: 'bronze_nsde',
      partitionKeys: [
        { name: 'version', type: 'string' },
        { name: 'version_date', type: 'date' }
      ],
      enableProjection: true  // Eliminates need for MSCK REPAIR TABLE
    });
    
    // NSDE metadata table - unpartitioned to avoid tiny partitions
    const bronzeNsdeMetadataTable = makeParquetTable(this, 'Tablebronze_nsde_metadata', {
      dbName: glueDatabase.ref,
      s3Location: `s3://${dataLakeBucket.bucketName}/bronze/nsde/metadata/data/`,
      tableName: 'bronze_nsde_metadata',
      partitionKeys: [] // No partitions - version stored as column
    });
    
    // NSDE-specific crawler (update-only, doesn't create tables)
    const nsdeCrawler = makeCrawler(this, 'NsdeCrawler', {
      name: 'pp-raws-nsde-crawler',
      roleArn: sharedGlueRole.roleArn,
      dbName: glueDatabase.ref,
      s3Targets: [
        { 
          path: `s3://${dataLakeBucket.bucketName}/bronze/nsde/data/`,
          exclusions: ["**/_SUCCESS*", "**/*.json", "**/.*/**", "**/_temporary/**", "**/*.crc"]
        },
        {
          path: `s3://${dataLakeBucket.bucketName}/bronze/nsde/metadata/data/`,
          exclusions: ["**/_SUCCESS*", "**/*.json", "**/.*/**", "**/_temporary/**", "**/*.crc"]
        }
      ],
      // No schedule for v0.1 - run manually after ETL
      // TODO: Enable daily schedule after ETL jobs are stable
      // schedule: { scheduleExpression: 'cron(0 4 * * ? *)' },
      schemaChangePolicy: { 
        updateBehavior: 'UPDATE_IN_DATABASE', 
        deleteBehavior: 'LOG' 
      },
      configuration: {
        Version: 1,
        CrawlerOutput: { 
          Partitions: { AddOrUpdateBehavior: 'InheritFromTable' },
          Tables: { AddOrUpdateBehavior: 'MergeNewColumns' }
        },
        Grouping: {
          TableGroupingPolicy: 'CombineCompatibleSchemas'
        }
      }
    });

    // Export resources for other stacks
    this.dataLakeBucket = dataLakeBucket;
    this.glueScriptsBucket = glueScriptsBucket;
    this.glueDatabase = glueDatabase;
    this.sharedGlueRole = sharedGlueRole;
    this.nsdeCrawler = nsdeCrawler;
    this.bronzeNsdeTable = bronzeNsdeTable;
    this.bronzeNsdeMetadataTable = bronzeNsdeMetadataTable;

    // Outputs with exports for cross-stack references
    new CfnOutput(this, 'DataLakeBucketName', { 
      value: dataLakeBucket.bucketName, 
      exportName: `${this.stackName}-DataLakeBucket`,
      description: 'Shared S3 data lake bucket for all domains' 
    });

    new CfnOutput(this, 'DataLakeBucketArn', { 
      value: dataLakeBucket.bucketArn, 
      exportName: `${this.stackName}-DataLakeBucketArn`,
      description: 'Shared S3 data lake bucket ARN' 
    });

    new CfnOutput(this, 'GlueScriptsBucketName', { 
      value: glueScriptsBucket.bucketName, 
      exportName: `${this.stackName}-GlueScriptsBucket`,
      description: 'Shared Glue scripts bucket' 
    });

    new CfnOutput(this, 'GlueDatabaseName', { 
      value: glueDatabase.ref, 
      exportName: `${this.stackName}-GlueDatabase`,
      description: 'Shared Glue database for Athena queries' 
    });

    new CfnOutput(this, 'SharedGlueRoleArn', { 
      value: sharedGlueRole.roleArn, 
      exportName: `${this.stackName}-SharedGlueRoleArn`,
      description: 'Shared IAM role for Glue jobs' 
    });

    new CfnOutput(this, 'SharedGlueRoleName', {
      value: sharedGlueRole.roleName,
      exportName: `${this.stackName}-SharedGlueRoleName`,
      description: 'Shared IAM role name for Glue jobs'
    });

    // Table and crawler outputs for v0.1
    new CfnOutput(this, 'BronzeNsdeTableName', {
      value: bronzeNsdeTable.ref,
      exportName: `${this.stackName}-BronzeNsdeTable`,
      description: 'Bronze NSDE data table name'
    });

    new CfnOutput(this, 'BronzeNsdeMetadataTableName', {
      value: bronzeNsdeMetadataTable.ref,
      exportName: `${this.stackName}-BronzeNsdeMetadataTable`,
      description: 'Bronze NSDE metadata table name'
    });

    new CfnOutput(this, 'NsdeCrawlerName', {
      value: nsdeCrawler.ref,
      exportName: `${this.stackName}-NsdeCrawler`,
      description: 'NSDE-specific Glue crawler name'
    });
  }
}

module.exports = { DataLakeStack };