#!/usr/bin/env node
const cdk = require("aws-cdk-lib");

// Import core stack
const { EtlCoreStack } = require("./EtlCoreStack");

// Import dataset stacks
const { FdaNsdeStack } = require("./datasets/fda-nsde/FdaNsdeStack");
const { FdaCderStack } = require("./datasets/fda-cder/FdaCderStack");
const { FdaAllNdcStack } = require("./datasets/fda-all-ndc/FdaAllNdcStack");
const { RxnormStack } = require("./datasets/rxnorm/RxnormStack");

// Future dataset imports will go here:
// const { RxClassStack } = require("./datasets/rxclass/RxClassStack");
// etc.

const app = new cdk.App();

// Define environment once
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION || "us-east-1",
};

// Deploy core infrastructure first
const etlCoreStack = new EtlCoreStack(app, "pp-dw-etl-core", {
  env,
  description: "Core ETL infrastructure: S3 bucket, Glue databases, IAM roles"
});

// Deploy dataset stacks with dependency on core stack
const fdaNsdeStack = new FdaNsdeStack(app, "pp-dw-etl-fda-nsde", {
  env,
  etlCoreStack,
  description: "FDA NSDE (Comprehensive NDC SPL Data Elements) ETL pipeline"
});
fdaNsdeStack.addDependency(etlCoreStack);

const fdaCderStack = new FdaCderStack(app, "pp-dw-etl-fda-cder", {
  env,
  etlCoreStack,
  description: "FDA CDER (National Drug Code Directory) ETL pipeline"
});
fdaCderStack.addDependency(etlCoreStack);

const fdaAllNdcStack = new FdaAllNdcStack(app, "pp-dw-etl-fda-all-ndc", {
  env,
  etlCoreStack,
  description: "FDA All NDC Gold Layer - Combined NSDE and CDER data"
});
fdaAllNdcStack.addDependency(etlCoreStack);

const rxnormStack = new RxnormStack(app, "pp-dw-etl-rxnorm", {
  env,
  etlCoreStack,
  description: "RxNORM ETL pipeline - NLM drug nomenclature (requires UMLS API key)"
});
rxnormStack.addDependency(etlCoreStack);

// Add tags to all ETL stacks
cdk.Tags.of(app).add("Project", "pp-dw");
cdk.Tags.of(app).add("Component", "ETL");
cdk.Tags.of(app).add("Environment", "production");
cdk.Tags.of(app).add("ManagedBy", "CDK");

// Export the app for testing
module.exports = { app };