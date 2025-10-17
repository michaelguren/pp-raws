const cdk = require("aws-cdk-lib");
const lambda = require("aws-cdk-lib/aws-lambda");
const iam = require("aws-cdk-lib/aws-iam");
const stepfunctions = require("aws-cdk-lib/aws-stepfunctions");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");
const fs = require("fs");

/**
 * ETL Orchestration Stack
 * Deploys Step Functions state machine and supporting Lambda functions
 * to orchestrate bronze → silver → gold ETL job execution
 */
class EtlOrchestrationStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load configurations
    const etlConfig = require("../config.json");
    const orchestrationConfig = require("./orchestration-config.json");

    // Compute database names
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;

    // ========== LAMBDA EXECUTION ROLES ==========

    // Lambda execution role for table checking
    const checkTablesLambdaRole = new iam.Role(this, "CheckTablesLambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole"
        ),
      ],
    });

    // Allow Lambda to describe Glue tables
    checkTablesLambdaRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
        ],
        resources: ["*"],
      })
    );

    // Lambda execution role for crawler management
    const startCrawlersLambdaRole = new iam.Role(this, "StartCrawlersLambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole"
        ),
      ],
    });

    // Allow Lambda to start Glue crawlers
    startCrawlersLambdaRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["glue:StartCrawler"],
        resources: ["*"],
      })
    );

    // ========== LAMBDA FUNCTIONS ==========

    const checkTablesFunction = new lambda.Function(
      this,
      "CheckTablesFunction",
      {
        functionName: "pp-dw-etl-check-tables",
        runtime: lambda.Runtime.NODEJS_22_X,
        handler: "check-tables.handler",
        code: lambda.Code.fromAsset(path.join(__dirname, "lambdas")),
        timeout: cdk.Duration.seconds(60),
        memorySize: 256,
        role: checkTablesLambdaRole,
        // Note: AWS_REGION is automatically set by Lambda runtime, don't override
      }
    );

    const startCrawlersFunction = new lambda.Function(
      this,
      "StartCrawlersFunction",
      {
        functionName: "pp-dw-etl-start-crawlers",
        runtime: lambda.Runtime.NODEJS_22_X,
        handler: "start-crawlers.handler",
        code: lambda.Code.fromAsset(path.join(__dirname, "lambdas")),
        timeout: cdk.Duration.seconds(60),
        memorySize: 256,
        role: startCrawlersLambdaRole,
      }
    );

    // ========== STEP FUNCTIONS ROLE ==========

    // Step Functions execution role
    const stepFunctionsRole = new iam.Role(this, "StepFunctionsRole", {
      assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
    });

    // Allow Step Functions to start Glue jobs
    stepFunctionsRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
        ],
        resources: ["*"],
      })
    );

    // Allow Step Functions to invoke Lambda functions
    stepFunctionsRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["lambda:InvokeFunction"],
        resources: [
          checkTablesFunction.functionArn,
          startCrawlersFunction.functionArn,
        ],
      })
    );

    // ========== STEP FUNCTIONS STATE MACHINE ==========

    // Read and process the state machine definition
    let stateMachineDefinition = fs.readFileSync(
      path.join(__dirname, "state-machine.json"),
      "utf-8"
    );

    // Replace placeholders
    stateMachineDefinition = stateMachineDefinition
      .replace("${REGION}", this.region)
      .replace("${ACCOUNT_ID}", this.account)
      .replace(/arn:aws:lambda:\$\{REGION\}:\$\{ACCOUNT_ID\}:function:pp-dw-etl-check-tables/g, checkTablesFunction.functionArn)
      .replace(/arn:aws:lambda:\$\{REGION\}:\$\{ACCOUNT_ID\}:function:pp-dw-etl-start-crawlers/g, startCrawlersFunction.functionArn);

    // Parse and create the state machine
    const stateMachineDefinitionObj = JSON.parse(stateMachineDefinition);

    const stateMachine = new stepfunctions.CfnStateMachine(
      this,
      "EtlOrchestrationStateMachine",
      {
        stateMachineType: "STANDARD",
        roleArn: stepFunctionsRole.roleArn,
        definitionString: JSON.stringify(stateMachineDefinitionObj),
      }
    );

    stateMachine.node.addDependency(checkTablesFunction);

    // ========== HELPER FUNCTION TO GENERATE EXECUTION INPUT ==========

    // Export a utility to generate execution input
    this.generateExecutionInput = (overrides = {}) => {
      return {
        // Bronze layer job names
        bronzeJobName_fda_nsde: `${etlConfig.etl_resource_prefix}-bronze-fda-nsde`,
        bronzeJobName_fda_cder: `${etlConfig.etl_resource_prefix}-bronze-fda-cder`,
        bronzeJobName_rxnorm: `${etlConfig.etl_resource_prefix}-bronze-rxnorm`,
        bronzeJobName_rxnorm_spl_mappings: `${etlConfig.etl_resource_prefix}-bronze-rxnorm-spl-mappings`,
        bronzeJobName_rxclass: `${etlConfig.etl_resource_prefix}-bronze-rxclass`,
        bronzeJobName_rxclass_drug_members: `${etlConfig.etl_resource_prefix}-bronze-rxclass-drug-members`,

        // Bronze crawler names
        bronzeCrawlerName_fda_nsde: `${etlConfig.etl_resource_prefix}-bronze-fda-nsde-crawler`,
        bronzeCrawlerName_fda_cder: `${etlConfig.etl_resource_prefix}-bronze-fda-cder-crawler`,
        bronzeCrawlerName_rxnorm: `${etlConfig.etl_resource_prefix}-bronze-rxnorm-crawler`,
        bronzeCrawlerName_rxnorm_spl_mappings: `${etlConfig.etl_resource_prefix}-bronze-rxnorm-spl-mappings-crawler`,
        bronzeCrawlerName_rxclass: `${etlConfig.etl_resource_prefix}-bronze-rxclass-crawler`,
        bronzeCrawlerName_rxclass_drug_members: `${etlConfig.etl_resource_prefix}-bronze-rxclass-drug-members-crawler`,

        // Bronze expected tables (hyphens → underscores)
        bronzeDatabase,
        bronzeExpectedTables: [
          "fda_nsde",
          "fda_cder",
          "rxnconso",
          "rxnrel",
          "rxnsat",
          "rxnorm_spl_mappings",
          "rxclass",
          "rxclass_drug_members",
        ],

        // Bronze crawler names (for orchestration)
        bronzeCrawlerNames: [
          `${etlConfig.etl_resource_prefix}-bronze-fda-nsde-crawler`,
          `${etlConfig.etl_resource_prefix}-bronze-fda-cder-crawler`,
          `${etlConfig.etl_resource_prefix}-bronze-rxnorm-crawler`,
          `${etlConfig.etl_resource_prefix}-bronze-rxnorm-spl-mappings-crawler`,
          `${etlConfig.etl_resource_prefix}-bronze-rxclass-crawler`,
          `${etlConfig.etl_resource_prefix}-bronze-rxclass-drug-members-crawler`,
        ],

        // Silver layer job names
        silverJobName_rxnorm_products: `${etlConfig.etl_resource_prefix}-silver-rxnorm-products`,
        silverJobName_rxnorm_ndc_mappings: `${etlConfig.etl_resource_prefix}-silver-rxnorm-ndc-mappings`,
        silverJobName_fda_all_ndc: `${etlConfig.etl_resource_prefix}-silver-fda-all-ndc`,

        // Silver crawler names
        silverCrawlerName_rxnorm_products: `${etlConfig.etl_resource_prefix}-silver-rxnorm-products-crawler`,
        silverCrawlerName_rxnorm_ndc_mappings: `${etlConfig.etl_resource_prefix}-silver-rxnorm-ndc-mappings-crawler`,
        silverCrawlerName_fda_all_ndc: `${etlConfig.etl_resource_prefix}-silver-fda-all-ndc-crawler`,

        // Silver expected tables
        silverDatabase,
        silverExpectedTables: [
          "rxnorm_products",
          "rxnorm_ndc_mappings",
          "fda_all_ndc",
        ],

        // Silver crawler names (for orchestration)
        silverCrawlerNames: [
          `${etlConfig.etl_resource_prefix}-silver-rxnorm-products-crawler`,
          `${etlConfig.etl_resource_prefix}-silver-rxnorm-ndc-mappings-crawler`,
          `${etlConfig.etl_resource_prefix}-silver-fda-all-ndc-crawler`,
        ],

        // Gold layer job names
        goldJobName_rxnorm_products: `${etlConfig.etl_resource_prefix}-gold-rxnorm-products`,
        goldJobName_rxnorm_product_classifications: `${etlConfig.etl_resource_prefix}-gold-rxnorm-product-classifications`,
        goldJobName_rxnorm_ndc_mappings: `${etlConfig.etl_resource_prefix}-gold-rxnorm-ndc-mappings`,
        goldJobName_fda_all_ndcs: `${etlConfig.etl_resource_prefix}-gold-fda-all-ndcs`,

        // Gold crawler names
        goldCrawlerName_rxnorm_products: `${etlConfig.etl_resource_prefix}-gold-rxnorm-products-crawler`,
        goldCrawlerName_rxnorm_product_classifications: `${etlConfig.etl_resource_prefix}-gold-rxnorm-product-classifications-crawler`,
        goldCrawlerName_rxnorm_ndc_mappings: `${etlConfig.etl_resource_prefix}-gold-rxnorm-ndc-mappings-crawler`,
        goldCrawlerName_fda_all_ndcs: `${etlConfig.etl_resource_prefix}-gold-fda-all-ndcs-crawler`,

        // Gold expected tables (plural names per convention)
        goldDatabase,
        goldExpectedTables: [
          "rxnorm_products",
          "rxnorm_product_classifications",
          "rxnorm_ndc_mappings",
          "fda_all_ndcs",
        ],

        // Gold crawler names (for orchestration)
        goldCrawlerNames: [
          `${etlConfig.etl_resource_prefix}-gold-rxnorm-products-crawler`,
          `${etlConfig.etl_resource_prefix}-gold-rxnorm-product-classifications-crawler`,
          `${etlConfig.etl_resource_prefix}-gold-rxnorm-ndc-mappings-crawler`,
          `${etlConfig.etl_resource_prefix}-gold-fda-all-ndcs-crawler`,
        ],

        ...overrides,
      };
    };

    // ========== OUTPUTS ==========

    new cdk.CfnOutput(this, "StateMachineArn", {
      value: stateMachine.ref,
      description: "ARN of the ETL orchestration Step Functions state machine",
      exportName: "pp-dw-etl-orchestration-state-machine-arn",
    });

    new cdk.CfnOutput(this, "StateMachineExecution", {
      value: `https://console.aws.amazon.com/states/home?region=${this.region}#/statemachines/view/${stateMachine.ref}`,
      description: "Link to Step Functions state machine in AWS Console",
    });

    new cdk.CfnOutput(this, "CheckTablesLambdaArn", {
      value: checkTablesFunction.functionArn,
      description: "ARN of the check-tables Lambda function",
    });

    // Store references
    this.stateMachine = stateMachine;
    this.checkTablesFunction = checkTablesFunction;
  }
}

module.exports = { EtlOrchestrationStack };
