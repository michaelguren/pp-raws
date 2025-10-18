const cdk = require("aws-cdk-lib");
const lambda = require("aws-cdk-lib/aws-lambda");
const iam = require("aws-cdk-lib/aws-iam");
const stepfunctions = require("aws-cdk-lib/aws-stepfunctions");
const path = require("path");
const fs = require("fs");

/**
 * ETL Bootstrap Orchestration Stack
 * One-time initialization: Deploys Step Functions state machine and supporting Lambda functions
 * to orchestrate bronze → silver → gold ETL job execution for initial data load
 */
class EtlBootstrapOrchestrationStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load configurations
    const etlConfig = require("../../config.json");
    const bootstrapConfig = require("./config.json");
    const etlUtils = require("../../shared/deploytime");

    // Compute database names
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;

    // ========== COMPUTE ALL RESOURCE NAMES AT DEPLOY TIME ==========

    // Helper function to get resource names for a dataset
    const getDatasetResourceNames = (layer, dataset) => {
      const tables = etlUtils.getTablesForDataset(layer, dataset);
      const names = etlUtils.getResourceNames(dataset, tables);
      return { tables, names };
    };

    // Build bronze layer metadata
    const bronzeDatasets = {};
    const bronzeTableToCrawlerMap = {}; // Map each table to its crawler
    bootstrapConfig.bronze_jobs.forEach(dataset => {
      const { tables, names } = getDatasetResourceNames("bronze", dataset);

      // Determine crawler names for this dataset
      const crawlerNames = tables.length === 1
        ? [names.bronzeCrawler]
        : tables.map(table => {
            const camelKey = table.split("-").map((part, i) =>
              i === 0 ? part : part.charAt(0).toUpperCase() + part.slice(1)
            ).join("");
            return names[`bronze${camelKey.charAt(0).toUpperCase() + camelKey.slice(1)}Crawler`];
          });

      bronzeDatasets[dataset] = {
        jobName: names.bronzeJob,
        crawlerNames: crawlerNames,
        tables: tables.map(t => t.replace(/-/g, "_")) // Convert to Glue table names
      };

      // Build table-to-crawler mapping
      const glueTableNames = tables.map(t => t.replace(/-/g, "_"));
      glueTableNames.forEach((tableName, index) => {
        // For single-table datasets, use the single crawler
        // For multi-table datasets, use the specific crawler for this table
        bronzeTableToCrawlerMap[tableName] = crawlerNames.length === 1
          ? crawlerNames[0]
          : crawlerNames[index];
      });
    });

    // Build silver layer metadata (sequential order matters)
    const silverDatasets = {};
    bootstrapConfig.silver_jobs_sequence.forEach(dataset => {
      const { tables, names } = getDatasetResourceNames("silver", dataset);
      silverDatasets[dataset] = {
        jobName: names.silverJob,
        crawlerName: `${etlConfig.etl_resource_prefix}-silver-${dataset}-crawler`,
        tables: tables
      };
    });

    // Build gold layer metadata
    const goldDatasets = {};
    bootstrapConfig.gold_jobs_sequence.forEach(dataset => {
      const { tables, names } = getDatasetResourceNames("gold", dataset);
      goldDatasets[dataset] = {
        jobName: names.goldJob,
        crawlerName: names.goldCrawler,
        tables: tables
      };
    });

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
        code: lambda.Code.fromAsset(path.join(__dirname, "..", "shared", "lambdas")),
        timeout: cdk.Duration.seconds(60),
        memorySize: 256,
        role: checkTablesLambdaRole,
      }
    );

    const startCrawlersFunction = new lambda.Function(
      this,
      "StartCrawlersFunction",
      {
        functionName: "pp-dw-etl-start-crawlers",
        runtime: lambda.Runtime.NODEJS_22_X,
        handler: "start-crawlers.handler",
        code: lambda.Code.fromAsset(path.join(__dirname, "..", "shared", "lambdas")),
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

    // ========== BUILD STATE MACHINE DEFINITION AT DEPLOY TIME ==========

    // Build state machine with all values hardcoded (no JSON input required at runtime)
    const stateMachineDefinition = {
      Comment: "ETL Bootstrap Orchestration - One-time full pipeline run with job and crawler orchestration",
      StartAt: "StartBronzeJobs",
      States: {
        // ========== BRONZE LAYER ==========
        StartBronzeJobs: {
          Type: "Parallel",
          Comment: "Start all bronze jobs in parallel",
          Branches: Object.entries(bronzeDatasets).map(([dataset, meta]) => ({
            StartAt: `BronzeJob_${dataset}`,
            States: {
              [`BronzeJob_${dataset}`]: {
                Type: "Task",
                Resource: "arn:aws:states:::glue:startJobRun.sync",
                Parameters: {
                  JobName: meta.jobName
                },
                End: true
              }
            }
          })),
          OutputPath: "$[0]",
          Next: "CheckBronzeTables"
        },
        CheckBronzeTables: {
          Type: "Task",
          Resource: checkTablesFunction.functionArn,
          Parameters: {
            database: bronzeDatabase,
            tables: Object.values(bronzeDatasets).flatMap(d => d.tables),
            tableToCrawlerMap: bronzeTableToCrawlerMap
          },
          ResultPath: "$",
          Next: "BronzeTablesExist?"
        },
        "BronzeTablesExist?": {
          Type: "Choice",
          Choices: [
            {
              Variable: "$.exists",
              BooleanEquals: true,
              Next: "StartSilverJobSequence"
            }
          ],
          Default: "StartBronzeCrawlers"
        },
        StartBronzeCrawlers: {
          Type: "Task",
          Resource: startCrawlersFunction.functionArn,
          Parameters: {
            "crawlerNames.$": "$.crawlersNeeded"
          },
          ResultPath: "$",
          Next: "WaitForBronzeCrawlers"
        },
        WaitForBronzeCrawlers: {
          Type: "Wait",
          Seconds: 30,
          Comment: "Wait for crawlers to complete catalog updates",
          Next: "StartSilverJobSequence"
        },

        // ========== SILVER LAYER (Sequential with table checks between jobs) ==========
        StartSilverJobSequence: {
          Type: "Pass",
          Comment: "Initialize silver job sequencing with dependency handling",
          Next: Object.keys(silverDatasets)[0] ? `SilverJob_${Object.keys(silverDatasets)[0]}` : "StartGoldJobSequence"
        },

        // ========== GOLD LAYER (Sequential) ==========
        StartGoldJobSequence: {
          Type: "Pass",
          Comment: "Initialize gold job sequencing",
          Next: Object.keys(goldDatasets)[0] ? `GoldJob_${Object.keys(goldDatasets)[0]}` : "OrchestrationComplete"
        }
      }
    };

    // Add sequential silver jobs with table checks and crawlers between each
    const silverDatasetKeys = Object.keys(silverDatasets);
    silverDatasetKeys.forEach((dataset, index) => {
      const meta = silverDatasets[dataset];
      const isLast = index === silverDatasetKeys.length - 1;
      const nextDataset = isLast ? null : silverDatasetKeys[index + 1];

      // Silver job task
      stateMachineDefinition.States[`SilverJob_${dataset}`] = {
        Type: "Task",
        Resource: "arn:aws:states:::glue:startJobRun.sync",
        Parameters: {
          JobName: meta.jobName
        },
        Next: `CheckSilverTable_${dataset}`
      };

      // Check if table exists after job
      // Build table-to-crawler map for this dataset
      const silverTableToCrawlerMap = {};
      meta.tables.forEach(table => {
        silverTableToCrawlerMap[table] = meta.crawlerName;
      });

      stateMachineDefinition.States[`CheckSilverTable_${dataset}`] = {
        Type: "Task",
        Resource: checkTablesFunction.functionArn,
        Parameters: {
          database: silverDatabase,
          tables: meta.tables,
          tableToCrawlerMap: silverTableToCrawlerMap
        },
        ResultPath: "$",
        Next: `SilverTableExists_${dataset}?`
      };

      // Choice: does table exist?
      stateMachineDefinition.States[`SilverTableExists_${dataset}?`] = {
        Type: "Choice",
        Choices: [
          {
            Variable: "$.exists",
            BooleanEquals: true,
            Next: isLast ? "StartGoldJobSequence" : `SilverJob_${nextDataset}`
          }
        ],
        Default: `StartSilverCrawler_${dataset}`
      };

      // Start crawler if table doesn't exist
      stateMachineDefinition.States[`StartSilverCrawler_${dataset}`] = {
        Type: "Task",
        Resource: startCrawlersFunction.functionArn,
        Parameters: {
          "crawlerNames.$": "$.crawlersNeeded"
        },
        ResultPath: "$",
        Next: `WaitForSilverCrawler_${dataset}`
      };

      // Wait for crawler
      stateMachineDefinition.States[`WaitForSilverCrawler_${dataset}`] = {
        Type: "Wait",
        Seconds: 30,
        Comment: `Wait for ${dataset} crawler to complete catalog updates`,
        Next: isLast ? "StartGoldJobSequence" : `SilverJob_${nextDataset}`
      };
    });

    // Add sequential gold jobs
    const goldDatasetKeys = Object.keys(goldDatasets);
    goldDatasetKeys.forEach((dataset, index) => {
      const meta = goldDatasets[dataset];
      const isLast = index === goldDatasetKeys.length - 1;

      stateMachineDefinition.States[`GoldJob_${dataset}`] = {
        Type: "Task",
        Resource: "arn:aws:states:::glue:startJobRun.sync",
        Parameters: {
          JobName: meta.jobName
        },
        Next: isLast ? "CheckGoldTables" : `GoldJob_${goldDatasetKeys[index + 1]}`
      };
    });

    // Add gold table check and crawler states
    // Build table-to-crawler map for gold layer
    const goldTableToCrawlerMap = {};
    Object.values(goldDatasets).forEach(meta => {
      meta.tables.forEach(table => {
        goldTableToCrawlerMap[table] = meta.crawlerName;
      });
    });

    stateMachineDefinition.States.CheckGoldTables = {
      Type: "Task",
      Resource: checkTablesFunction.functionArn,
      Parameters: {
        database: goldDatabase,
        tables: Object.values(goldDatasets).flatMap(d => d.tables),
        tableToCrawlerMap: goldTableToCrawlerMap
      },
      ResultPath: "$",
      Next: "GoldTablesExist?"
    };

    stateMachineDefinition.States["GoldTablesExist?"] = {
      Type: "Choice",
      Choices: [
        {
          Variable: "$.exists",
          BooleanEquals: true,
          Next: "OrchestrationComplete"
        }
      ],
      Default: "StartGoldCrawlers"
    };

    stateMachineDefinition.States.StartGoldCrawlers = {
      Type: "Task",
      Resource: startCrawlersFunction.functionArn,
      Parameters: {
        "crawlerNames.$": "$.crawlersNeeded"
      },
      ResultPath: "$",
      Next: "WaitForGoldCrawlers"
    };

    stateMachineDefinition.States.WaitForGoldCrawlers = {
      Type: "Wait",
      Seconds: 30,
      Comment: "Wait for crawlers to complete catalog updates",
      Next: "OrchestrationComplete"
    };

    stateMachineDefinition.States.OrchestrationComplete = {
      Type: "Succeed",
      Comment: "All ETL jobs and crawlers completed successfully"
    };

    // ========== CREATE STATE MACHINE ==========

    const stateMachine = new stepfunctions.CfnStateMachine(
      this,
      "EtlBootstrapStateMachine",
      {
        stateMachineType: "STANDARD",
        roleArn: stepFunctionsRole.roleArn,
        definitionString: JSON.stringify(stateMachineDefinition, null, 2),
      }
    );

    stateMachine.node.addDependency(checkTablesFunction);
    stateMachine.node.addDependency(startCrawlersFunction);

    // ========== OUTPUTS ==========

    new cdk.CfnOutput(this, "StateMachineArn", {
      value: stateMachine.ref,
      description: "ARN of the ETL bootstrap orchestration Step Functions state machine",
      exportName: "pp-dw-etl-bootstrap-state-machine-arn",
    });

    new cdk.CfnOutput(this, "StateMachineExecution", {
      value: `https://console.aws.amazon.com/states/home?region=${this.region}#/statemachines/view/${stateMachine.ref}`,
      description: "Link to Step Functions state machine in AWS Console",
    });

    new cdk.CfnOutput(this, "CheckTablesLambdaArn", {
      value: checkTablesFunction.functionArn,
      description: "ARN of the check-tables Lambda function",
    });

    new cdk.CfnOutput(this, "ExecutionNote", {
      value: "This state machine requires NO JSON input - just click 'Start execution' with empty {}",
      description: "Execution Instructions",
    });

    // Store references
    this.stateMachine = stateMachine;
    this.checkTablesFunction = checkTablesFunction;
  }
}

module.exports = { EtlBootstrapOrchestrationStack };
