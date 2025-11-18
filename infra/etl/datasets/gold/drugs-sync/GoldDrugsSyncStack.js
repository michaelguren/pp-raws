const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const { glueScriptPath } = require("../../../shared/deploytime/paths");

/**
 * Gold Drugs Sync Stack
 * Syncs drug_product_codesets from Gold layer (Athena/S3) to DynamoDB operational table
 *
 * Following RAWS principles:
 * - Kill-and-fill: Complete refresh each run
 * - Bridge layer: Connects data warehouse (analytics) to operational database (API)
 * - No crawler needed: Writes to DynamoDB, not S3
 * - CDK defines pattern, runtime defines parameters (RUN_ID passed at invocation)
 */
class GoldDrugsSyncStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load warehouse configuration
    const etlConfig = require("../../../config.json");
    const dataset = "gold-drugs-sync";

    // Worker configuration - medium size for 300K+ records
    const workerConfig = etlConfig.glue_worker_configs["medium"];

    // Build script path
    const scriptPath = `s3://${bucketName}/etl/datasets/${dataset}/glue/sync_job.py`;

    // Deploy Glue script to S3
    new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
      sources: [s3deploy.Source.asset(glueScriptPath(__dirname))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/datasets/${dataset}/glue/`,
      retainOnDelete: false
    });

    // Grant DynamoDB permissions to Glue role
    // Glue role needs to read/write the pocket-pharmacist table
    glueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'dynamodb:PutItem',
        'dynamodb:BatchWriteItem',
        'dynamodb:Query',
        'dynamodb:DeleteItem'
      ],
      resources: [
        // Table ARN - allow access to pocket-pharmacist table
        `arn:aws:dynamodb:${this.region}:${this.account}:table/pocket-pharmacist`
      ]
    }));

    // Create Glue Job for Gold → DynamoDB sync
    const syncJob = new glue.CfnJob(this, 'SyncJob', {
      name: `${etlConfig.etl_resource_prefix}-${dataset}`,
      description: 'Sync drug_product_codesets from Gold layer to DynamoDB operational table',
      role: glueRole.roleArn,

      command: {
        name: 'glueetl',
        scriptLocation: scriptPath,
        pythonVersion: etlConfig.glue_defaults.python_version
      },

      glueVersion: etlConfig.glue_defaults.version,

      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,

      timeout: etlConfig.glue_defaults.timeout_minutes,
      maxRetries: etlConfig.glue_defaults.max_retries,

      // Job arguments - infrastructure pattern only
      // Runtime parameters (RUN_ID) passed at job invocation
      defaultArguments: {
        '--job-language': 'python',
        '--job-bookmark-option': 'job-bookmark-disable',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        '--enable-spark-ui': 'true',
        '--spark-event-logs-path': `s3://${bucketName}/glue-spark-logs/`,

        // DynamoDB table name (infrastructure config)
        '--TABLE_NAME': 'pocket-pharmacist',

        // NOTE: RUN_ID is generated inside the job at runtime using datetime.now(timezone.utc)
        // No need to pass RUN_ID as an argument - the job creates it automatically

        // Additional Spark configurations for DynamoDB writes
        '--enable-auto-scaling': 'true'
      }
    });

    // Export useful properties
    this.syncJob = syncJob;

    // CloudFormation outputs
    new cdk.CfnOutput(this, 'SyncJobName', {
      value: syncJob.name,
      description: 'Glue job name for Gold → DynamoDB sync',
      exportName: `${etlConfig.etl_resource_prefix}-${dataset}-job-name`
    });
  }
}

module.exports = { GoldDrugsSyncStack };
