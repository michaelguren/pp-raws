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
      max_retries: 1,
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
      // Use table name directly as path: bronze/fda-products/, bronze/fda-packages/
      // This results in table names: fda_products, fda_packages
      paths.bronzeTables[tableName] = this.s3Path(
        bucketName,
        "bronze",
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

  // Build Glue job default arguments
  getGlueJobArguments(options = {}) {
    const { dataset, bucketName, datasetConfig, layer = "bronze", tables } = options;

    const resourceNames = this.getResourceNames(dataset, tables);
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

    return args;
  }

  // Create standardized Glue job
  createGlueJob(scope, jobName, glueRole, scriptLocation, workerConfig, jobArguments, layer = "bronze") {
    const glue = require("aws-cdk-lib/aws-glue");

    const constructId = layer === "bronze" ? "BronzeJob" : "GoldJob";

    return new glue.CfnJob(scope, constructId, {
      name: jobName,
      role: glueRole.roleArn,
      command: {
        name: "glueetl",
        scriptLocation: scriptLocation,
        pythonVersion: this.glue_defaults.python_version,
      },
      glueVersion: this.glue_defaults.version,
      workerType: workerConfig.worker_type,
      numberOfWorkers: workerConfig.number_of_workers,
      maxRetries: this.glue_defaults.max_retries,
      timeout: this.glue_defaults.timeout_minutes,
      defaultArguments: jobArguments,
    });
  }

  // Create standardized bronze crawler
  createBronzeCrawler(scope, resourceName, glueRole, targetPath, databaseName) {
    const glue = require("aws-cdk-lib/aws-glue");

    return new glue.CfnCrawler(scope, "BronzeCrawler", {
      name: resourceName,
      role: glueRole.roleArn,
      databaseName: databaseName,
      targets: { s3Targets: [{ path: targetPath }] },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
        }
      })
    });
  }
}

// Export singleton instance
module.exports = new EtlDeployUtils();
