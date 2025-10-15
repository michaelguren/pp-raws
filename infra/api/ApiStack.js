const cdk = require("aws-cdk-lib");
const dynamodb = require("aws-cdk-lib/aws-dynamodb");

/**
 * API Stack - Operational data layer for Pocket Pharmacist application
 *
 * Creates the DynamoDB table for operational data with single-table design:
 * - Drug records: PK=DRUG#{id}, SK=METADATA#{timestamp}
 * - Search tokens: PK=SEARCH#DRUG, SK=token#{word}#{field}#DRUG#{id}
 *
 * Following RAWS principles:
 * - Convention over configuration (predictable naming)
 * - Good enough > perfect (DynamoDB over OpenSearch)
 * - Infrastructure as code (all resources defined in CDK)
 */
class ApiStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Create DynamoDB table with single-table design
    // This table will hold ALL application entities (drugs, users, favorites, sessions, etc.)
    // For v0.2, we start with drug records and search tokens
    this.table = new dynamodb.Table(this, "PocketPharmacistTable", {
      tableName: "pocket-pharmacist",

      // Primary key design
      partitionKey: {
        name: "PK",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: "SK",
        type: dynamodb.AttributeType.STRING,
      },

      // On-demand billing - pay per request, no capacity planning
      // Perfect for v0.2 with unknown traffic patterns
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,

      // Encryption at rest (AWS best practice)
      encryption: dynamodb.TableEncryption.AWS_MANAGED,

      // Point-in-time recovery - enables backup and restore
      pointInTimeRecovery: true,

      // Removal policy - for v0.2, retain on stack deletion to prevent data loss
      // Change to DESTROY in dev/test environments if needed
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // No GSI needed!
    // Search tokens are stored with PK=SEARCH#DRUG directly in the table.
    // Query pattern: KeyConditionExpression = "PK = :pk AND begins_with(SK, :prefix)"
    // DynamoDB accesses the SEARCH#DRUG partition natively - no scan, no GSI.
    // This is the beauty of single-table design.

    // CloudFormation outputs for cross-stack references
    new cdk.CfnOutput(this, "TableName", {
      value: this.table.tableName,
      description: "DynamoDB table name for operational data",
      exportName: "pp-api-table-name",
    });

    new cdk.CfnOutput(this, "TableArn", {
      value: this.table.tableArn,
      description: "DynamoDB table ARN",
      exportName: "pp-api-table-arn",
    });
  }
}

module.exports = { ApiStack };
