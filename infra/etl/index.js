#!/usr/bin/env node
const cdk = require("aws-cdk-lib");

// Import core stack
const { EtlCoreStack } = require("./EtlCoreStack");

// Import orchestration stacks
const { DailyFdaOrchestrationStack } = require("./orchestrations/daily/fda/DailyFdaOrchestrationStack");

// Import dataset stacks - Bronze
const { FdaNsdeStack } = require("./datasets/bronze/fda-nsde/FdaNsdeStack");
const { FdaCderStack } = require("./datasets/bronze/fda-cder/FdaCderStack");
const { RxnormStack } = require("./datasets/bronze/rxnorm/RxnormStack");
const { RxnormSplMappingsStack } = require("./datasets/bronze/rxnorm-spl-mappings/RxnormSplMappingsStack");
const { RxClassStack } = require("./datasets/bronze/rxclass/RxClassStack");

// Import dataset stacks - Silver
const { FdaAllNdcsStack } = require("./datasets/silver/fda-all-ndcs/FdaAllNdcsStack");
const { RxnormProductsStack } = require("./datasets/silver/rxnorm-products/RxnormProductsStack");
const { RxnormNdcMappingsStack } = require("./datasets/silver/rxnorm-ndc-mappings/RxnormNdcMappingsStack");
const { RxclassDrugMembersStack } = require("./datasets/silver/rxclass-drug-members/RxclassDrugMembersStack");

// Import dataset stacks - Gold
const { GoldFdaAllNdcsStack } = require("./datasets/gold/fda-all-ndcs/GoldFdaAllNdcsStack");
const { GoldRxnormProductsStack } = require("./datasets/gold/rxnorm-products/GoldRxnormProductsStack");
const { GoldRxnormNdcMappingsStack } = require("./datasets/gold/rxnorm-ndc-mappings/GoldRxnormNdcMappingsStack");
const { GoldRxnormProductClassificationsStack } = require("./datasets/gold/rxnorm-product-classifications/GoldRxnormProductClassificationsStack");
const { GoldDrugsSyncStack } = require("./datasets/gold/drugs-sync/GoldDrugsSyncStack");

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

// Deploy daily FDA orchestration (Step Functions, Lambda)
// Daily pipeline for FDA datasets: fda-nsde, fda-cder → fda-all-ndcs (bronze → silver → gold)
const dailyFdaOrchestrationStack = new DailyFdaOrchestrationStack(app, "pp-dw-daily-fda-orchestration", {
  env,
  etlCoreStack,
  description: "Daily FDA Orchestration: Automated daily pipeline for FDA drug data (fda-nsde, fda-cder → fda-all-ndcs)"
});
dailyFdaOrchestrationStack.addDependency(etlCoreStack);

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

const fdaAllNdcsStack = new FdaAllNdcsStack(app, "pp-dw-etl-silver-fda-all-ndcs", {
  env,
  etlCoreStack,
  description: "FDA All NDCs Silver Layer - Combined NSDE and CDER data"
});
fdaAllNdcsStack.addDependency(etlCoreStack);

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


const rxnormProductsStack = new RxnormProductsStack(app, "pp-dw-etl-rxnorm-products", {
  env,
  etlCoreStack,
  description: "RxNORM Products Silver Layer - Prescribable drug products from RxNORM bronze"
});
rxnormProductsStack.addDependency(etlCoreStack);

const rxnormNdcMappingsStack = new RxnormNdcMappingsStack(app, "pp-dw-etl-rxnorm-ndc-mappings", {
  env,
  etlCoreStack,
  description: "RxNORM NDC Mappings Silver Layer - RxCUI to NDC mappings extracted from RXNSAT"
});
rxnormNdcMappingsStack.addDependency(etlCoreStack);

const rxclassDrugMembersStack = new RxclassDrugMembersStack(app, "pp-dw-etl-rxclass-drug-members", {
  env,
  etlCoreStack,
  description: "RxClass Drug Members Silver Layer - Drug-to-class relationships enriched via NLM RxNav API"
});
rxclassDrugMembersStack.addDependency(etlCoreStack);

const goldFdaAllNdcsStack = new GoldFdaAllNdcsStack(app, "pp-dw-etl-gold-fda-all-ndcs", {
  env,
  etlCoreStack,
  description: "Gold FDA All NDCs - Temporally versioned FDA drug data with status partitioning"
});
goldFdaAllNdcsStack.addDependency(etlCoreStack);

const goldRxnormProductsStack = new GoldRxnormProductsStack(app, "pp-dw-etl-gold-rxnorm-products", {
  env,
  etlCoreStack,
  description: "Gold RxNORM Products - Temporally versioned RxNORM drug data with status partitioning"
});
goldRxnormProductsStack.addDependency(etlCoreStack);

const goldRxnormNdcMappingsStack = new GoldRxnormNdcMappingsStack(app, "pp-dw-etl-gold-rxnorm-ndc-mappings", {
  env,
  etlCoreStack,
  description: "Gold RxNORM NDC Mappings - Temporally versioned RxCUI-to-NDC mappings with status partitioning"
});
goldRxnormNdcMappingsStack.addDependency(etlCoreStack);

const goldRxnormProductClassificationsStack = new GoldRxnormProductClassificationsStack(app, "pp-dw-etl-gold-rxnorm-product-classifications", {
  env,
  etlCoreStack,
  description: "Gold RxNORM Product Classifications - Temporally versioned drug-class relationships with status partitioning"
});
goldRxnormProductClassificationsStack.addDependency(etlCoreStack);

const goldDrugsSyncStack = new GoldDrugsSyncStack(app, "pp-dw-etl-gold-drugs-sync", {
  env,
  etlCoreStack,
  description: "Gold Drugs Sync - Syncs drug_product_codesets from Gold layer to DynamoDB"
});
goldDrugsSyncStack.addDependency(etlCoreStack);

// Add tags to all ETL stacks
cdk.Tags.of(app).add("Project", "pp-dw");
cdk.Tags.of(app).add("Component", "ETL");
cdk.Tags.of(app).add("Environment", "production");
cdk.Tags.of(app).add("ManagedBy", "CDK");

// Export the app for testing
module.exports = { app };