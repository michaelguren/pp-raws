#!/usr/bin/env node
const cdk = require("aws-cdk-lib");
const { ApiStack } = require("./ApiStack");

const app = new cdk.App();

// Define environment
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION || "us-east-1",
};

// Deploy API infrastructure
const apiStack = new ApiStack(app, "pp-api", {
  env,
  description: "Pocket Pharmacist API - Operational data layer (DynamoDB single-table design)",
});

// Export the app for testing
module.exports = { app };
