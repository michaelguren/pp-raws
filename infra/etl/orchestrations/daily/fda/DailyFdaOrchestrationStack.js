const cdk = require("aws-cdk-lib");
const lambda = require("aws-cdk-lib/aws-lambda");
const iam = require("aws-cdk-lib/aws-iam");
const stepfunctions = require("aws-cdk-lib/aws-stepfunctions");
const events = require("aws-cdk-lib/aws-events");
const targets = require("aws-cdk-lib/aws-events-targets");
const { sharedLambdaPath } = require("../../../shared/deploytime/paths");

/**
 * Daily FDA Orchestration Stack
 * Runs daily FDA data pipeline: fda-nsde, fda-cder → fda-all-ndcs (bronze → silver → gold)
 */
class DailyFdaOrchestrationStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure
    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Load configurations
    const etlConfig = require("../../../config.json");
    const dailyConfig = require("./config.json");
    const etlUtils = require("../../../shared/deploytime");

    // Compute database names
    const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
    const silverDatabase = `${etlConfig.database_prefix}_silver`;
    const goldDatabase = `${etlConfig.database_prefix}_gold`;

    // ========== COMPUTE ALL RESOURCE NAMES AT DEPLOY TIME ==========

    // Build bronze layer metadata
    const bronzeDatasets = {};
    const bronzeTableToCrawlerMap = {};

    dailyConfig.bronze_jobs.forEach(dataset => {
      const tables = etlUtils.getTablesForDataset("bronze", dataset);
      const names = etlUtils.getResourceNames(dataset, tables);
      const tableToCrawlerMap = etlUtils.getTableToCrawlerMap("bronze", tables, names);

      bronzeDatasets[dataset] = {
        jobName: names.bronzeJob,
        tables: tables.map(t => t.replace(/-/g, "_"))
      };

      Object.assign(bronzeTableToCrawlerMap, tableToCrawlerMap);
    });

    // Build silver layer metadata
    const silverDatasets = {};

    dailyConfig.silver_jobs_sequence.forEach(dataset => {
      const tables = etlUtils.getTablesForDataset("silver", dataset);
      const names = etlUtils.getResourceNames(dataset, tables);

      silverDatasets[dataset] = {
        jobName: names.silverJob,
        crawlerName: names.silverCrawler,
        tables: tables
      };
    });

    // Build gold layer metadata
    const goldDatasets = {};

    dailyConfig.gold_jobs_sequence.forEach(dataset => {
      const tables = etlUtils.getTablesForDataset("gold", dataset);
      const names = etlUtils.getResourceNames(dataset, tables);

      goldDatasets[dataset] = {
        jobName: names.goldJob,
        crawlerName: names.goldCrawler,
        tables: tables
      };
    });

    // ========== LAMBDA EXECUTION ROLES ==========

    const checkTablesLambdaRole = new iam.Role(this, "CheckTablesLambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
      ],
    });

    checkTablesLambdaRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["glue:GetTables", "glue:GetDatabase", "glue:GetDatabases"],
        resources: ["*"],
      })
    );

    const startCrawlersLambdaRole = new iam.Role(this, "StartCrawlersLambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
      ],
    });

    startCrawlersLambdaRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["glue:StartCrawler"],
        resources: ["*"],
      })
    );

    // ========== LAMBDA FUNCTIONS ==========

    const checkTablesFunction = new lambda.Function(this, "CheckTablesFunction", {
      functionName: "pp-dw-daily-fda-check-tables",
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "check-tables.handler",
      code: lambda.Code.fromAsset(sharedLambdaPath(__dirname)),
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      role: checkTablesLambdaRole,
    });

    const startCrawlersFunction = new lambda.Function(this, "StartCrawlersFunction", {
      functionName: "pp-dw-daily-fda-start-crawlers",
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "start-crawlers.handler",
      code: lambda.Code.fromAsset(sharedLambdaPath(__dirname)),
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      role: startCrawlersLambdaRole,
    });

    // ========== STEP FUNCTIONS ROLE ==========

    const stepFunctionsRole = new iam.Role(this, "StepFunctionsRole", {
      assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
    });

    stepFunctionsRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
        resources: ["*"],
      })
    );

    stepFunctionsRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["lambda:InvokeFunction"],
        resources: [checkTablesFunction.functionArn, startCrawlersFunction.functionArn],
      })
    );

    // ========== BUILD STATE MACHINE DEFINITION ==========

    const stateMachineDefinition = {
      Comment: "Daily FDA ETL Pipeline - fda-nsde, fda-cder → fda-all-ndcs (bronze → silver → gold)",
      StartAt: "StartBronzeJobs",
      States: {
        // ========== BRONZE LAYER ==========
        StartBronzeJobs: {
          Type: "Parallel",
          Comment: "Start all FDA bronze jobs in parallel",
          Branches: Object.entries(bronzeDatasets).map(([dataset, meta]) => ({
            StartAt: `BronzeJob_${dataset}`,
            States: {
              [`BronzeJob_${dataset}`]: {
                Type: "Task",
                Resource: "arn:aws:states:::glue:startJobRun.sync",
                Parameters: { JobName: meta.jobName },
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
          Choices: [{
            Variable: "$.exists",
            BooleanEquals: true,
            Next: "StartSilverJobSequence"
          }],
          Default: "StartBronzeCrawlers"
        },
        StartBronzeCrawlers: {
          Type: "Task",
          Resource: startCrawlersFunction.functionArn,
          Parameters: { "crawlerNames.$": "$.crawlersNeeded" },
          ResultPath: "$",
          Next: "WaitForBronzeCrawlers"
        },
        WaitForBronzeCrawlers: {
          Type: "Wait",
          Seconds: 30,
          Next: "StartSilverJobSequence"
        },

        // ========== SILVER LAYER ==========
        StartSilverJobSequence: {
          Type: "Pass",
          Next: Object.keys(silverDatasets)[0] ? `SilverJob_${Object.keys(silverDatasets)[0]}` : "StartGoldJobSequence"
        },

        // ========== GOLD LAYER ==========
        StartGoldJobSequence: {
          Type: "Pass",
          Next: Object.keys(goldDatasets)[0] ? `GoldJob_${Object.keys(goldDatasets)[0]}` : "OrchestrationComplete"
        }
      }
    };

    // Add sequential silver jobs with table checks
    const silverDatasetKeys = Object.keys(silverDatasets);
    silverDatasetKeys.forEach((dataset, index) => {
      const meta = silverDatasets[dataset];
      const isLast = index === silverDatasetKeys.length - 1;
      const nextDataset = isLast ? null : silverDatasetKeys[index + 1];

      const silverTables = etlUtils.getTablesForDataset("silver", dataset);
      const silverNames = etlUtils.getResourceNames(dataset, silverTables);
      const silverTableToCrawlerMap = etlUtils.getTableToCrawlerMap("silver", silverTables, silverNames);

      stateMachineDefinition.States[`SilverJob_${dataset}`] = {
        Type: "Task",
        Resource: "arn:aws:states:::glue:startJobRun.sync",
        Parameters: { JobName: meta.jobName },
        Next: `CheckSilverTable_${dataset}`
      };

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

      stateMachineDefinition.States[`SilverTableExists_${dataset}?`] = {
        Type: "Choice",
        Choices: [{
          Variable: "$.exists",
          BooleanEquals: true,
          Next: isLast ? "StartGoldJobSequence" : `SilverJob_${nextDataset}`
        }],
        Default: `StartSilverCrawler_${dataset}`
      };

      stateMachineDefinition.States[`StartSilverCrawler_${dataset}`] = {
        Type: "Task",
        Resource: startCrawlersFunction.functionArn,
        Parameters: { "crawlerNames.$": "$.crawlersNeeded" },
        ResultPath: "$",
        Next: `WaitForSilverCrawler_${dataset}`
      };

      stateMachineDefinition.States[`WaitForSilverCrawler_${dataset}`] = {
        Type: "Wait",
        Seconds: 30,
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
        Parameters: { JobName: meta.jobName },
        Next: isLast ? "CheckGoldTables" : `GoldJob_${goldDatasetKeys[index + 1]}`
      };
    });

    // Add gold table check
    const goldTableToCrawlerMap = {};
    Object.keys(goldDatasets).forEach(dataset => {
      const goldTables = etlUtils.getTablesForDataset("gold", dataset);
      const goldNames = etlUtils.getResourceNames(dataset, goldTables);
      const goldMap = etlUtils.getTableToCrawlerMap("gold", goldTables, goldNames);
      Object.assign(goldTableToCrawlerMap, goldMap);
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
      Choices: [{
        Variable: "$.exists",
        BooleanEquals: true,
        Next: "OrchestrationComplete"
      }],
      Default: "StartGoldCrawlers"
    };

    stateMachineDefinition.States.StartGoldCrawlers = {
      Type: "Task",
      Resource: startCrawlersFunction.functionArn,
      Parameters: { "crawlerNames.$": "$.crawlersNeeded" },
      ResultPath: "$",
      Next: "WaitForGoldCrawlers"
    };

    stateMachineDefinition.States.WaitForGoldCrawlers = {
      Type: "Wait",
      Seconds: 30,
      Next: "OrchestrationComplete"
    };

    stateMachineDefinition.States.OrchestrationComplete = {
      Type: "Succeed"
    };

    // ========== CREATE STATE MACHINE ==========

    const stateMachine = new stepfunctions.CfnStateMachine(this, "DailyFdaStateMachine", {
      stateMachineName: "pp-dw-daily-fda-orchestration",
      stateMachineType: "STANDARD",
      roleArn: stepFunctionsRole.roleArn,
      definitionString: JSON.stringify(stateMachineDefinition, null, 2),
    });

    stateMachine.node.addDependency(checkTablesFunction);
    stateMachine.node.addDependency(startCrawlersFunction);

    // ========== EVENTBRIDGE SCHEDULE (OPTIONAL) ==========

    if (dailyConfig.schedule?.enabled) {
      const rule = new events.Rule(this, "DailyFdaScheduleRule", {
        ruleName: "pp-dw-daily-fda-schedule",
        description: dailyConfig.schedule.description,
        schedule: events.Schedule.expression(dailyConfig.schedule.expression),
      });

      rule.addTarget(
        new targets.SfnStateMachine(
          stepfunctions.StateMachine.fromStateMachineArn(this, "StateMachineTarget", stateMachine.ref)
        )
      );
    }

    // ========== OUTPUTS ==========

    new cdk.CfnOutput(this, "StateMachineArn", {
      value: stateMachine.ref,
      description: "ARN of the daily FDA orchestration Step Functions state machine",
      exportName: "pp-dw-daily-fda-state-machine-arn",
    });

    new cdk.CfnOutput(this, "StateMachineConsole", {
      value: `https://console.aws.amazon.com/states/home?region=${this.region}#/statemachines/view/${stateMachine.ref}`,
      description: "Link to Step Functions console",
    });

    // Store references
    this.stateMachine = stateMachine;
  }
}

module.exports = { DailyFdaOrchestrationStack };
