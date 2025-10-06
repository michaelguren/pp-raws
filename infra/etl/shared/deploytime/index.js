const path = require("path");

class EtlDeployUtils {
  constructor() {
    // Core configuration (from original config.json)
    this.etl_resource_prefix = "pp-dw";
    this.database_prefix = "pp_dw";

    this.glue_defaults = {
      version: "5.0",
      python_version: "3",
      timeout_minutes: 30,
      max_retries: 0,  // No retries - monthly batch jobs should fail fast for debugging
      default_arguments: {
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-continuous-log-filter": "true",
        "--continuous-log-logGroup": "/aws-glue/jobs/logs-v2",
        "--enable-spark-ui": "true",
      },
    };

    this.glue_worker_configs = {
      small: {
        worker_type: "G.1X",
        number_of_workers: 2,
        description: "For datasets < 100MB - minimum viable configuration",
      },
      medium: {
        worker_type: "G.1X",
        number_of_workers: 5,
        description: "For datasets 100MB - 1GB - optimal for medium files",
      },
      large: {
        worker_type: "G.1X",
        number_of_workers: 10,
        description:
          "For datasets 1GB - 5GB - horizontal scaling for parallelism",
      },
      xlarge: {
        worker_type: "G.2X",
        number_of_workers: 10,
        description: "For datasets > 5GB - vertical + horizontal scaling",
      },
    };

    this.lambda_defaults = {
      runtime: "python3.12",
      timeout_seconds: 300,
      memory_mb: 1024,
    };
  }

  // Resource naming helpers (includes all resource names: jobs, crawlers, databases)
  getResourceNames(dataset, tables) {
    const base = this.etl_resource_prefix;
    const names = {
      // Database names
      bronzeDatabase: `${this.database_prefix}_bronze`,
      silverDatabase: `${this.database_prefix}_silver`,
      goldDatabase: `${this.database_prefix}_gold`,
      // Job names
      bronzeJob: `${base}-bronze-${dataset}`,
      silverJob: `${base}-silver-${dataset}`,
      goldJob: `${base}-gold-${dataset}`,
      // Crawler names
      goldCrawler: `${base}-gold-${dataset}-crawler`,
    };

    // Generate crawler names based on number of tables
    if (tables.length === 1) {
      // Single table: use dataset name in crawler
      names.bronzeCrawler = `${base}-bronze-${dataset}-crawler`;
    } else {
      // Multiple tables: include table name in each crawler
      tables.forEach((tableName) => {
        // Create camelCase key: "fda-products" → "bronzeFdaProductsCrawler"
        const camelKey = tableName
          .split("-")
          .map((part, i) =>
            i === 0 ? part : part.charAt(0).toUpperCase() + part.slice(1)
          )
          .join("");
        names[
          `bronze${camelKey.charAt(0).toUpperCase() + camelKey.slice(1)}Crawler`
        ] = `${base}-bronze-${dataset}-${tableName}-crawler`;
      });
    }

    return names;
  }

  // S3 path builder
  s3Path(bucketName, ...segments) {
    return `s3://${bucketName}/` + path.posix.join(...segments) + "/";
  }

  // Get all standard S3 paths for a dataset
  getS3Paths(bucketName, dataset, tables) {
    // Validate tables parameter
    if (!tables || !Array.isArray(tables) || tables.length === 0) {
      throw new Error(
        `Tables parameter is required for getS3Paths. Received: ${JSON.stringify(tables)}`
      );
    }

    const paths = {
      raw: this.s3Path(bucketName, "raw", dataset),
      bronze: this.s3Path(bucketName, "bronze", dataset),
      gold: this.s3Path(bucketName, "gold", dataset),
      scripts: this.s3Path(bucketName, "etl", dataset, "glue"),
      scriptLocation: {
        bronze:
          `s3://${bucketName}/` +
          path.posix.join("etl", dataset, "glue", "bronze_job.py"),
        gold:
          `s3://${bucketName}/` +
          path.posix.join("etl", dataset, "glue", "gold_job.py"),
      },
      bronzeTables: {},
    };

    // Generate bronze table paths for each table
    tables.forEach((tableName) => {
      // Multi-table datasets store under: bronze/{dataset}/{tableName}/
      // Single-table datasets this path isn't used (crawler uses paths.bronze instead)
      paths.bronzeTables[tableName] = this.s3Path(
        bucketName,
        "bronze",
        dataset,
        tableName
      );
    });

    return paths;
  }

  // Get worker configuration
  getWorkerConfig(sizeCategory) {
    const category = sizeCategory || "medium";
    const config = this.glue_worker_configs[category];

    if (!config) {
      throw new Error(
        `Invalid data_size_category: ${sizeCategory}. Must be one of: ${Object.keys(
          this.glue_worker_configs
        ).join(", ")}`
      );
    }

    return config;
  }

  // Create standardized Glue job
  createGlueJob(scope, options = {}) {
    const glue = require("aws-cdk-lib/aws-glue");

    const {
      dataset,
      bucketName,
      datasetConfig = {},
      layer = "bronze",
      tables = [],
      glueRole,
      scriptLocation, // Optional override
      workerSize = "medium"
    } = options;

    // Get worker configuration
    const workerConfig = this.getWorkerConfig(workerSize);

    // Get resource names
    const resourceNames = this.getResourceNames(dataset, tables);

    // Build job arguments
    const paths = this.getS3Paths(bucketName, dataset, tables);
    const args = {
      "--dataset": dataset,
      "--compression_codec": "zstd",
      ...this.glue_defaults.default_arguments,
    };

    // Bronze layer arguments
    if (layer === "bronze") {
      args["--bronze_database"] = resourceNames.bronzeDatabase;
      args["--raw_path"] = paths.raw;
      args["--bronze_path"] = paths.bronze;

      if (datasetConfig.source_url) {
        args["--source_url"] = datasetConfig.source_url;
      }

      if (datasetConfig.column_schema) {
        args["--column_schema"] = JSON.stringify(datasetConfig.column_schema);
      }

      if (datasetConfig.date_format) {
        args["--date_format"] = datasetConfig.date_format;
      }

      if (datasetConfig.delimiter) {
        args["--delimiter"] = datasetConfig.delimiter;
      }

      if (datasetConfig.file_table_mapping) {
        args["--file_table_mapping"] = JSON.stringify(
          datasetConfig.file_table_mapping
        );
      }

      if (datasetConfig.raw_files) {
        args["--raw_files_config"] = JSON.stringify(datasetConfig.raw_files);
      }
    }

    // Gold layer arguments
    if (layer === "gold") {
      args["--bronze_database"] = resourceNames.bronzeDatabase;
      args["--gold_database"] = resourceNames.goldDatabase;
      args["--gold_base_path"] = paths.gold;
    }

    // Determine job name and construct ID
    const jobName = layer === "bronze" ? resourceNames.bronzeJob : resourceNames.goldJob;
    const constructId = layer === "bronze" ? "BronzeJob" : "GoldJob";

    // Determine script location - use default for HTTP/ZIP bronze jobs unless overridden
    let finalScriptLocation = scriptLocation;
    if (!finalScriptLocation) {
      if (layer === "bronze" && datasetConfig.source_url) {
        // Default to shared HTTP/ZIP processor for bronze jobs with source_url
        finalScriptLocation = `s3://${bucketName}/etl/shared/runtime/https_zip/bronze_http_job.py`;
      } else {
        // Default to dataset-specific script for other cases
        const scriptFile = layer === "bronze" ? "bronze_job.py" : "gold_job.py";
        finalScriptLocation = `s3://${bucketName}/etl/${dataset}/glue/${scriptFile}`;
      }
    }

    // Add shared runtime utilities as extra Python files for bronze jobs
    const jobConfig = {
      name: jobName,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: finalScriptLocation,
        pythonVersion: this.glue_defaults.python_version,
      },
      glueVersion: this.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: this.glue_defaults.max_retries,
      timeout: this.glue_defaults.timeout_minutes,
      defaultArguments: args,
    };

    // Package runtime utilities with bronze jobs
    if (layer === "bronze" && datasetConfig.source_url) {
      args["--extra-py-files"] = `s3://${bucketName}/etl/shared/runtime/https_zip/etl_runtime_utils.py`;
    }

    return new glue.CfnJob(scope, constructId, jobConfig);
  }

  // Create all bronze crawlers for a dataset (handles both single and multi-table)
  createBronzeCrawlers(scope, dataset, tables, glueRole, resourceNames, paths) {
    const glue = require("aws-cdk-lib/aws-glue");
    const cdk = require("aws-cdk-lib");
    const outputs = {};

    const crawlerConfig = JSON.stringify({
      Version: 1.0,
      CrawlerOutput: {
        Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
        Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
      }
    });

    if (tables.length === 1) {
      // Single table - one crawler for the whole bronze path
      new glue.CfnCrawler(scope, "BronzeCrawler", {
        name: resourceNames.bronzeCrawler,
        role: glueRole.roleArn,
        databaseName: resourceNames.bronzeDatabase,
        targets: { s3Targets: [{ path: paths.bronze }] },
        configuration: crawlerConfig
      });

      outputs.BronzeCrawlerName = new cdk.CfnOutput(scope, "BronzeCrawlerName", {
        value: resourceNames.bronzeCrawler,
        description: "Bronze Crawler Name",
      });
    } else {
      // Multi-table - one crawler per table
      tables.forEach((tableName) => {
        const camelTableName = tableName
          .split("-")
          .map((part, i) => i === 0 ? part : part.charAt(0).toUpperCase() + part.slice(1))
          .join("");
        const crawlerKey = `bronze${camelTableName.charAt(0).toUpperCase() + camelTableName.slice(1)}Crawler`;
        const constructId = `Bronze${camelTableName.charAt(0).toUpperCase() + camelTableName.slice(1)}Crawler`;
        const outputKey = `Bronze${camelTableName.charAt(0).toUpperCase() + camelTableName.slice(1)}CrawlerName`;

        new glue.CfnCrawler(scope, constructId, {
          name: resourceNames[crawlerKey],
          role: glueRole.roleArn,
          databaseName: resourceNames.bronzeDatabase,
          targets: { s3Targets: [{ path: paths.bronzeTables[tableName] }] },
          configuration: crawlerConfig
        });

        outputs[outputKey] = new cdk.CfnOutput(scope, outputKey, {
          value: resourceNames[crawlerKey],
          description: `Bronze ${tableName} Crawler Name`,
        });
      });
    }

    // Always add job output
    outputs.BronzeJobName = new cdk.CfnOutput(scope, "BronzeJobName", {
      value: resourceNames.bronzeJob,
      description: "Bronze Glue Job Name",
    });

    return outputs;
  }
}

// Export singleton instance
module.exports = new EtlDeployUtils();
