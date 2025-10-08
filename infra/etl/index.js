#!/usr/bin/env node
const cdk = require("aws-cdk-lib");

// Import core stack
const { EtlCoreStack } = require("./EtlCoreStack");

// Import dataset stacks
const { FdaNsdeStack } = require("./datasets/fda-nsde/FdaNsdeStack");
const { FdaCderStack } = require("./datasets/fda-cder/FdaCderStack");
const { FdaAllNdcStack } = require("./datasets/fda-all-ndc/FdaAllNdcStack");
const { RxnormStack } = require("./datasets/rxnorm/RxnormStack");
const { RxnormSplMappingsStack } = require("./datasets/rxnorm-spl-mappings/RxnormSplMappingsStack");
const { RxClassStack } = require("./datasets/rxclass/RxClassStack");
const { RxclassDrugMembersStack } = require("./datasets/rxclass-drug-members/RxclassDrugMembersStack");
const { RxnormProductsStack } = require("./datasets/rxnorm-products/RxnormProductsStack");
const { DrugProductCodesetsStack } = require("./datasets/drug-product-codesets/DrugProductCodesetsStack");

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

const rxnormSplMappingsStack = new RxnormSplMappingsStack(app, "pp-dw-etl-rxnorm-spl-mappings", {
  env,
  etlCoreStack,
  description: "RxNORM SPL Mappings ETL pipeline - DailyMed RxCUI to SPL SET ID mappings"
});
rxnormSplMappingsStack.addDependency(etlCoreStack);

const rxClassStack = new RxClassStack(app, "pp-dw-etl-rxclass", {
  env,
  etlCoreStack,
  description: "RxClass ETL pipeline - NLM RxNav drug classification API"
});
rxClassStack.addDependency(etlCoreStack);

const rxclassDrugMembersStack = new RxclassDrugMembersStack(app, "pp-dw-etl-rxclass-drug-members", {
  env,
  etlCoreStack,
  description: "RxClass Drug Members ETL pipeline - Drug relationships for each RxClass classification"
});
rxclassDrugMembersStack.addDependency(etlCoreStack);

const rxnormProductsStack = new RxnormProductsStack(app, "pp-dw-etl-rxnorm-products", {
  env,
  etlCoreStack,
  description: "RxNORM Products Silver Layer - Prescribable drug products from RxNORM bronze"
});
rxnormProductsStack.addDependency(etlCoreStack);

const drugProductCodesetsStack = new DrugProductCodesetsStack(app, "pp-dw-etl-drug-product-codesets", {
  env,
  etlCoreStack,
  description: "Drug Product Codesets Gold Layer - Combined FDA NDC and RxNORM product data"
});
drugProductCodesetsStack.addDependency(etlCoreStack);

// Add tags to all ETL stacks
cdk.Tags.of(app).add("Project", "pp-dw");
cdk.Tags.of(app).add("Component", "ETL");
cdk.Tags.of(app).add("Environment", "production");
cdk.Tags.of(app).add("ManagedBy", "CDK");

// Export the app for testing
module.exports = { app };