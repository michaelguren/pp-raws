const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");
const iam = require("aws-cdk-lib/aws-iam");
const s3deploy = require("aws-cdk-lib/aws-s3-deployment");
const path = require("path");

class RxnormStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    // Import shared ETL infrastructure from EtlCoreStack
    const { dataWarehouseBucket } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    // Create RxNORM-specific Glue role with Secrets Manager access
    const rxnormGlueRole = new iam.Role(this, "RxnormGlueRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
      ],
    });

    // Grant S3 access to the data warehouse bucket
    dataWarehouseBucket.grantReadWrite(rxnormGlueRole);

    // Grant Secrets Manager access for UMLS API key (least privilege - only RxNORM needs this)
    rxnormGlueRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["secretsmanager:GetSecretValue"],
      resources: [`arn:aws:secretsmanager:${this.region}:${this.account}:secret:umls-api-key*`]
    }));

    // Load warehouse and dataset configurations
    const etlConfig = require("../config.json");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    // Release date will be specified at runtime, not deployment time

    // Construct resource names from warehouse conventions
    const resourceNames = {
      bronzeJob: `${etlConfig.warehouse_prefix}-bronze-${dataset}`,
      rxcuiChangesJob: `${etlConfig.warehouse_prefix}-gold-rxcui-changes`,
      goldEnrichmentJob: `${etlConfig.warehouse_prefix}-gold-rxnorm-ndc-mapping`,
      // Crawlers for each major table
      rxnconsoCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-rxnconso-crawler`,
      rxnsatCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-rxnsat-crawler`,
      rxnrelCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-rxnrel-crawler`,
      rxnstyCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-rxnsty-crawler`,
      rxncuiCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-rxncui-crawler`,
      rxnatomarchiveCrawler: `${etlConfig.warehouse_prefix}-bronze-${dataset}-rxnatomarchive-crawler`,
      // Gold crawlers
      rxcuiChangesCrawler: `${etlConfig.warehouse_prefix}-gold-rxcui-changes-crawler`,
      ndcMappingCrawler: `${etlConfig.warehouse_prefix}-gold-rxnorm-ndc-mapping-crawler`
    };

    // Get worker config based on dataset size category
    const sizeCategory = datasetConfig.data_size_category || 'xlarge';
    const workerConfig = etlConfig.glue_worker_configs[sizeCategory];

    if (!workerConfig) {
      throw new Error(`Invalid data_size_category: ${sizeCategory}. Must be one of: small, medium, large, xlarge`);
    }

    // S3 path fragments from warehouse config patterns
    const rawPath = etlConfig.path_patterns.raw.replace('{dataset}', dataset).replace('run_id={run_id}/', '');
    const bronzePath = `bronze/${dataset}`;
    const goldPath = `gold/`;

    // Deploy Glue scripts to S3
    new s3deploy.BucketDeployment(this, "GlueScripts", {
      sources: [s3deploy.Source.asset(path.join(__dirname, "glue"))],
      destinationBucket: dataWarehouseBucket,
      destinationKeyPrefix: `etl/${dataset}/glue/`,
    });

    // Bronze Glue job for RxNORM RRF processing
    new glue.CfnJob(this, "BronzeJob", {
      name: resourceNames.bronzeJob,
      role: rxnormGlueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/${etlConfig.path_patterns.bronze_script.replace('{dataset}', dataset)}`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: 120, // 2 hours for large RxNORM download
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": etlConfig.bronze_database,
        "--raw_path": rawPath,
        "--bronze_path": bronzePath,
        "--compression_codec": "zstd",
        "--bucket_name": bucketName,
        "--umls_api_secret": "umls-api-key",
        "--tables_to_process": "RXNCONSO,RXNSAT,RXNREL,RXNSTY,RXNCUI,RXNATOMARCHIVE",
        // Default logging arguments (from ETL config)
        ...etlConfig.glue_defaults.logging_arguments,
      },
    });

    // RxCUI Changes Job (depends on bronze data)
    new glue.CfnJob(this, "RxcuiChangesJob", {
      name: resourceNames.rxcuiChangesJob,
      role: rxnormGlueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/${dataset}/glue/rxcui_changes_job.py`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: "G.1X",
      numberOfWorkers: 5,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": etlConfig.bronze_database,
        "--gold_database": etlConfig.gold_database,
        "--bucket_name": bucketName,
        "--compression_codec": "zstd"
      },
    });

    // Gold Enrichment Job (creates NDC mapping table)
    new glue.CfnJob(this, "GoldEnrichmentJob", {
      name: resourceNames.goldEnrichmentJob,
      role: rxnormGlueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: `s3://${bucketName}/etl/${dataset}/glue/gold_enrichment_job.py`,
        pythonVersion: etlConfig.glue_defaults.python_version,
      },
      glueVersion: etlConfig.glue_defaults.version,
      workerType: "G.1X",
      numberOfWorkers: 5,
      maxRetries: etlConfig.glue_defaults.max_retries,
      timeout: etlConfig.glue_defaults.timeout_minutes,
      defaultArguments: {
        "--dataset": dataset,
        "--bronze_database": etlConfig.bronze_database,
        "--gold_database": etlConfig.gold_database,
        "--bucket_name": bucketName,
        "--compression_codec": "zstd"
      },
    });

    // Bronze Crawlers for each RRF table
    const bronzeTables = [
      { name: "rxnconso", path: `s3://${bucketName}/${bronzePath}_rxnconso` },
      { name: "rxnsat", path: `s3://${bucketName}/${bronzePath}_rxnsat` },
      { name: "rxnrel", path: `s3://${bucketName}/${bronzePath}_rxnrel` },
      { name: "rxnsty", path: `s3://${bucketName}/${bronzePath}_rxnsty` },
      { name: "rxncui", path: `s3://${bucketName}/${bronzePath}_rxncui` },
      { name: "rxnatomarchive", path: `s3://${bucketName}/${bronzePath}_rxnatomarchive` }
    ];

    bronzeTables.forEach(table => {
      const crawlerName = resourceNames[`${table.name}Crawler`];

      if (!crawlerName) {
        throw new Error(`Crawler name not found for table: ${table.name}. Check resourceNames object.`);
      }

      new glue.CfnCrawler(this, `Bronze${table.name.charAt(0).toUpperCase() + table.name.slice(1)}Crawler`, {
        name: crawlerName,
        role: rxnormGlueRole.roleArn,
        databaseName: etlConfig.bronze_database,
        targets: {
          s3Targets: [
            {
              path: table.path
            }
          ]
        },
        configuration: JSON.stringify({
          Version: 1.0,
          CrawlerOutput: {
            Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
            Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
          }
        })
      });
    });

    // Gold Crawlers
    new glue.CfnCrawler(this, "RxcuiChangesCrawler", {
      name: resourceNames.rxcuiChangesCrawler,
      role: rxnormGlueRole.roleArn,
      databaseName: etlConfig.gold_database,
      targets: {
        s3Targets: [
          {
            path: `s3://${bucketName}/${goldPath}gold_rxcui_changes/`
          }
        ]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });

    new glue.CfnCrawler(this, "NdcMappingCrawler", {
      name: resourceNames.ndcMappingCrawler,
      role: rxnormGlueRole.roleArn,
      databaseName: etlConfig.gold_database,
      targets: {
        s3Targets: [
          {
            path: `s3://${bucketName}/${goldPath}gold_rxnorm_ndc_mapping/`
          }
        ]
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });

    // Outputs
    new cdk.CfnOutput(this, "BronzeJobName", {
      value: resourceNames.bronzeJob,
      description: "RxNORM Bronze ETL Glue Job Name (download + transform RRF tables)",
    });

    new cdk.CfnOutput(this, "RxcuiChangesJobName", {
      value: resourceNames.rxcuiChangesJob,
      description: "RxCUI Changes Tracking Job Name",
    });

    new cdk.CfnOutput(this, "GoldEnrichmentJobName", {
      value: resourceNames.goldEnrichmentJob,
      description: "NDC to RxCUI/TTY Mapping Job Name",
    });

    new cdk.CfnOutput(this, "BronzeCrawlerNames", {
      value: JSON.stringify({
        rxnconso: resourceNames.rxnconsoCrawler,
        rxnsat: resourceNames.rxnsatCrawler,
        rxnrel: resourceNames.rxnrelCrawler,
        rxnsty: resourceNames.rxnstyCrawler,
        rxncui: resourceNames.rxncuiCrawler,
        rxnatomarchive: resourceNames.rxnatomarchiveCrawler
      }),
      description: "Bronze RRF Table Crawler Names",
    });

    new cdk.CfnOutput(this, "GoldCrawlerNames", {
      value: JSON.stringify({
        rxcuiChanges: resourceNames.rxcuiChangesCrawler,
        ndcMapping: resourceNames.ndcMappingCrawler
      }),
      description: "Gold Table Crawler Names",
    });

    new cdk.CfnOutput(this, "ReleaseDate", {
      value: "Specify at runtime with --release_date parameter",
      description: "RxNORM Release Date (runtime parameter)",
    });

    new cdk.CfnOutput(this, "ExecutionOrder", {
      value: JSON.stringify([
        "1. Run Bronze Job",
        "2. Run Bronze Crawlers (after Bronze completes)",
        "3. Run RxCUI Changes + Gold Enrichment Jobs (after Crawlers complete)"
      ]),
      description: "Recommended Execution Order",
    });

  }
}

module.exports = { RxnormStack };