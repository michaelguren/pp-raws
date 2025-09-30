#!/usr/bin/env node

/**
 * Main Infrastructure Orchestrator
 *
 * This file is optional - each domain (ETL, API, Frontend) has its own CDK app.
 * Use this for cross-domain deployments or as a reference for all infrastructure.
 *
 * Individual apps can be deployed directly:
 * - ETL: cd infra/etl && cdk deploy --all
 * - API: cd infra/api && cdk deploy --all (future)
 * - Frontend: cd infra/frontend && cdk deploy --all (future)
 */

const cdk = require("aws-cdk-lib");

// Import domain-specific stacks only if you need cross-domain dependencies
const { EtlCoreStack } = require("./etl/EtlCoreStack");

const app = new cdk.App();

// Define environment once
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION || "us-east-1",
};

// ===== ETL Infrastructure =====
// Note: ETL has its own CDK app at infra/etl/index.js
// Only include here if you need it as a dependency for other stacks
const etlCoreStack = new EtlCoreStack(app, "pp-dw-etl-core", {
  description: "Shared ETL infrastructure - S3 bucket, databases, IAM roles",
  env,
});

// ===== API Infrastructure (Future) =====
// const apiStack = new ApiStack(app, "pp-dw-api", {
//   description: "API Gateway, Lambda functions, DynamoDB tables",
//   env,
//   etlBucket: etlCoreStack.dataWarehouseBucket, // Cross-domain dependency example
// });

// ===== Frontend Infrastructure (Future) =====
// const frontendStack = new FrontendStack(app, "pp-dw-frontend", {
//   description: "S3 static hosting, CloudFront distribution",
//   env,
//   apiUrl: apiStack.apiUrl, // Cross-domain dependency example
// });

// Add global tags
cdk.Tags.of(app).add("Project", "pp-dw");
cdk.Tags.of(app).add("ManagedBy", "CDK");
cdk.Tags.of(app).add("Environment", "production");