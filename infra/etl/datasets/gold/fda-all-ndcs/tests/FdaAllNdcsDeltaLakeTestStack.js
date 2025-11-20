const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

/**
 * FdaAllNdcsDeltaLakeTestStack - Delta Lake confidence test for FDA All NDCs
 *
 * DESIGN: Simple. Focused. Reliable.
 * - Glue Python Shell job (no Spark overhead)
 * - Uses Athena for all validations (no Delta log parsing)
 * - Three scenarios: add, update, term
 *
 * USAGE:
 *   // In etl/index.js
 *   const FdaAllNdcsDeltaLakeTestStack = require("./datasets/gold/fda-all-ndcs/tests/FdaAllNdcsDeltaLakeTestStack");
 *   new FdaAllNdcsDeltaLakeTestStack(app, "pp-dw-test-gold-fda-all-ndcs", {
 *     env,
 *     etlCoreStack
 *   });
 */
class FdaAllNdcsDeltaLakeTestStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;

    // Load global config
    const etlConfig = require("../../../../config.json");

    // Configuration
    const dataset = "fda-all-ndcs";
    const bucketName = dataWarehouseBucket.bucketName;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;
    const silverDatabase = `${etlConfig.database_prefix}_silver`;

    // Deploy test script to S3 (script is directly in tests/ directory)
    const scriptDeployment = new s3deploy.BucketDeployment(
      this,
      "DeployTestScript",
      {
        sources: [s3deploy.Source.asset(__dirname)],
        destinationBucket: dataWarehouseBucket,
        destinationKeyPrefix: `etl/tests/gold-${dataset}/`,
        description: "Delta Lake confidence test script",
        exclude: ["*.js", "*.md", "node_modules/*", ".git/*"]  // Only deploy .py files
      }
    );

    // Create Glue Python Shell job for Delta Lake confidence test
    const testJob = new glue.CfnJob(this, "DeltaLakeTestJob", {
      name: `${etlConfig.etl_resource_prefix}-gold-${dataset}-delta-test`,
      role: glueRole.roleArn,
      command: {
        name: "pythonshell", // Python Shell (not Spark)
        pythonVersion: "3.9",
        scriptLocation: `s3://${bucketName}/etl/tests/gold-${dataset}/simplified_delta_test.py`,
      },
      glueVersion: "3.0", // Python Shell version
      maxRetries: 0,
      timeout: 60, // 1 hour max (should complete in ~10 minutes)
      defaultArguments: {
        "--AWS_REGION": cdk.Aws.REGION,
        "--BUCKET": bucketName,
        "--GOLD_DATABASE": goldDatabase,
        "--SILVER_DATABASE": silverDatabase,
        "--TABLE": dataset.replace(/-/g, "_"), // fda_all_ndcs
        "--scenario": "add", // Default scenario (can override at runtime)
        "--run_date": "2050-01-01", // Default run date (can override at runtime)
      },
      description:
        "Delta Lake confidence test - validates temporal versioning with synthetic canary data (add/update/term scenarios)",
    });

    // Dependencies
    testJob.node.addDependency(scriptDeployment);

    // Outputs
    new cdk.CfnOutput(this, "TestJobName", {
      value: testJob.name,
      description: "Simplified test job name",
      exportName: `${id}-test-job-name`,
    });

    new cdk.CfnOutput(this, "TestJobUsage", {
      value: `aws glue start-job-run --job-name ${testJob.name} --arguments '{"--scenario":"add","--run_date":"2050-01-01"}'`,
      description: "Example test invocation command",
    });
  }
}

module.exports = { FdaAllNdcsDeltaLakeTestStack };
