# ETL Data Pipeline Architecture

**IMPORTANT**: First read `../CLAUDE.md` for general infrastructure requirements.

## Core Principles

- **Convention over Configuration**: Predictable patterns reduce config complexity
- **DRY**: Single source of truth for naming and conventions
- **Raw → Bronze**: Simple data flow for initial processing
- **Kill-and-Fill**: Complete data refresh each run for simplicity

## Architecture Overview

```
EventBridge → Glue Job → S3 (Raw + Bronze) → Crawler → Athena
```

**Infrastructure Pattern:**

- **EtlCoreStack**: Shared S3 bucket, Glue databases, IAM roles
- **Dataset Stacks**: Per-dataset Glue jobs, crawlers, script deployment

## Configuration Structure

**`config.json`** - Core ETL settings:

```json
{
  "etl_resource_prefix": "pp-dw",        // AWS resource naming
  "database_prefix": "pp_dw",            // Glue database naming
  "glue_defaults": { ... },              // Worker configs, logging, timeouts
  "glue_worker_configs": { ... }         // Size-based worker allocation
}
```

**Dataset configs** - `{dataset}/config.json`:

- `dataset`: Name used in paths and resources
- `data_size_category`: Determines worker allocation (small/medium/large/xlarge)
- `source_url` or `api_endpoints`: Data source configuration
- `delimiter`: File delimiter (`,` for CSV, `\t` for TSV, `|` for pipe-delimited, etc.)
- `file_table_mapping`: Maps source filenames to table names
- `column_schema`: Explicit column definitions with types and transformations:
  ```json
  "column_schema": {
    "Source Column Name": {
      "target_name": "snake_case_name",
      "type": "string|date|integer|decimal|boolean",
      "format": "yyyyMMdd"  // for date columns only
    }
  }
  ```
- `raw_files`: File handling configuration (legacy):
  - `source_filename`: Name for downloaded source file (e.g., "source.zip")
  - `extracted_file_extensions`: Array of file types to extract (e.g., [".csv"])
  - `file_count_expected`: Expected number of files (for validation)

## Naming Conventions

**AWS Resources:**

- Jobs: `{etl_resource_prefix}-{layer}-{dataset}` → `pp-dw-bronze-fda-nsde`
- Crawlers: `{etl_resource_prefix}-{layer}-{dataset}-crawler`
- Bucket: `{etl_resource_prefix}-{account}` → `pp-dw-123456789`

**Glue Databases:**

- Bronze: `{database_prefix}_bronze` → `pp_dw_bronze`
- Gold: `{database_prefix}_gold` → `pp_dw_gold`

**S3 Path Conventions:**

```
s3://pp-dw-{account}/
├── raw/{dataset}/run_id={timestamp}/     # Original source files
├── bronze/{dataset}/                     # Single-table datasets
├── bronze/{dataset}/{table_name}/        # Multi-table datasets
├── gold/{dataset}/                       # Business logic transformations
└── etl/{dataset}/glue/                   # Deployed job scripts
```

**Multi-File Dataset Conventions:**
For datasets containing multiple files (e.g., zip with product.txt + package.txt):

- **Bronze paths**: `bronze/{dataset}/{table_name}/` (e.g., `bronze/fda-cder/products/`)
- **Table names**: `{dataset}_{table_name}` (e.g., `fda_cder_products`)
- **Crawler names**: `{prefix}-bronze-{dataset}-{table_name}-crawler`
- **Path computation**: Use `${bronzePath}{table_name}/` pattern for DRY implementation
- **Config structure**: Use `file_table_mapping` to map source files to table names:
  ```json
  "file_table_mapping": {
    "product.txt": "products",
    "package.txt": "packages"
  }
  ```

**Glue Script Deployment:**

- HTTP/ZIP datasets: Use shared `utils-runtime/https_zip/bronze_http_job.py`
- Custom bronze jobs: `etl/{dataset}/glue/bronze_job.py`
- Gold layer: `etl/{dataset}/glue/gold_job.py`

**Shared Code Organization:**

- `utils-deploytime/` - Deploy-time utilities (used by CDK during deployment)
  - `index.js` - Shared configuration, factory methods, and helpers
- `utils-runtime/` - Runtime utilities (deployed to S3, used by Glue jobs)
  - `https_zip/` - Shared bronze job and utilities for HTTP/ZIP data sources
    - `bronze_http_job.py` - Complete shared bronze job (download, extract, transform, write to S3)
    - `etl_utils.py` - Helper functions for download and extraction

## Stack Implementation Patterns

**Deploy Utilities:**
Use the centralized deploy utilities for all configuration and resource creation:

**Unified Pattern for ALL Bronze Stacks (Single and Multi-Table):**
```javascript
const cdk = require("aws-cdk-lib");

class FdaDatasetStack extends cdk.Stack {
  constructor(scope, id, props) {
    super(scope, id, props);

    const { dataWarehouseBucket, glueRole } = props.etlCoreStack;
    const bucketName = dataWarehouseBucket.bucketName;

    const deployUtils = require("../utils-deploytime");
    const datasetConfig = require("./config.json");
    const dataset = datasetConfig.dataset;

    const tables = Object.values(datasetConfig.file_table_mapping);
    const resourceNames = deployUtils.getResourceNames(dataset, tables);
    const paths = deployUtils.getS3Paths(bucketName, dataset, tables);

    // Bronze job using shared HTTP/ZIP processor
    deployUtils.createGlueJob(this, {
      dataset,
      bucketName,
      datasetConfig,
      layer: 'bronze',
      tables,
      glueRole,
      workerSize: datasetConfig.data_size_category
    });

    // Create crawlers and outputs (handles both single and multi-table datasets automatically)
    deployUtils.createBronzeCrawlers(this, dataset, tables, glueRole, resourceNames, paths);
  }
}
```

This same code works for:
- Single-table datasets (one crawler for entire bronze path)
- Multi-table datasets (one crawler per table with appropriate paths)
- Automatic output generation for all crawlers

**Deploy Utilities Methods:**

- `getResourceNames(dataset, tables)` - generates all job/crawler names
  - Single-table: `{ bronzeJob, bronzeCrawler, goldJob, goldCrawler, bronzeDatabase, goldDatabase }`
  - Multi-table: Includes individual crawler names like `{ bronzeFdaProductsCrawler, bronzeFdaPackagesCrawler }` etc.
- `getS3Paths(bucketName, dataset, tables)` - returns all standard S3 paths as **complete S3 URLs**
  - Always includes: `{ raw, bronze, gold, scripts, scriptLocation }`
  - Multi-table: Also includes `{ bronzeTables: { "fda-products": "s3://...", "fda-packages": "s3://..." } }`
- `getWorkerConfig(sizeCategory)` - validates and returns worker configuration from predefined sizes
- `createGlueJob(scope, options)` - creates complete Glue job with all configurations
  - Automatically determines script location (shared HTTP/ZIP or custom)
  - Handles all job arguments for both single and multi-table datasets
  - Returns the created job resource
- `createBronzeCrawlers(scope, dataset, tables, glueRole, resourceNames, paths)` - creates all crawlers and outputs
  - Single-table: Creates one crawler for entire bronze path
  - Multi-table: Creates one crawler per table with correct paths
  - Automatically generates CfnOutputs for all resources
  - Returns outputs object for further customization if needed


**Shared Bronze Job for HTTP/ZIP Datasets:**

For HTTP/ZIP data sources, datasets now use the shared `bronze_http_job.py` which handles:
- Download from HTTP/HTTPS URLs
- ZIP extraction with file mapping
- Schema-driven transformations (column renaming, type casting, date parsing)
- Writing to Bronze layer as Parquet

Stack configuration points to shared script:
```javascript
// For HTTP/ZIP datasets using shared bronze job
const scriptLocation = `s3://${bucketName}/etl/utils-runtime/https_zip/bronze_http_job.py`;
```

**IAM Role Strategy:**

- **Shared Role**: Used by most datasets (S3, Glue, CloudWatch access)
- **Custom Role**: For sensitive operations (Secrets Manager, external APIs)

**Worker Configuration:**

- **small**: G.1X × 2 workers (< 100MB, API data)
- **medium**: G.1X × 5 workers (100MB - 1GB)
- **large**: G.1X × 10 workers (1GB - 5GB)
- **xlarge**: G.2X × 10 workers (> 5GB)

## Data Processing Patterns

**Bronze Layer:**

- Download source data → Extract and save files to S3 raw (original archives discarded)
- **Schema-driven transformations**: Apply `column_schema` for renaming, type casting, and date parsing
- Transform: Clean columns, type data properly, add `meta_run_id`
- Output: Parquet with ZSTD compression
- Kill-and-fill approach (complete overwrite)

**Glue Job Best Practices:**

- **Use shared `bronze_http_job.py` for all HTTP/ZIP datasets** (no custom bronze jobs needed)
- Leverage `file_table_mapping` for explicit file-to-table relationships
- **Use `column_schema` for predictable transformations** instead of guessing column types
- For custom jobs: Use `posixpath.join()` for safe S3 path construction
- Read data from S3 paths with Spark, never local filesystem

**Spark Configuration:**

```python
# ✅ Use current parquet settings
spark.conf.set("spark.sql.parquet.summary.metadata.level", "ALL")
```

**S3 Connector Strategy:**

- **Current**: Using EMRFS (`s3://`) - Glue 5.0 default, stable, optimized
- **Future**: Consider S3A (`s3a://`) benchmarking after pipeline stabilizes
- EMRFS provides optimized Parquet committer out of the box
- For greenfield testing, stick with `s3://` URIs for fastest iteration

**Crawler Management:**

- Bronze jobs: Print manual crawler instruction (don't auto-trigger)
- Run crawlers manually when schema changes
- Let crawlers handle table creation and schema updates

## Adding New Datasets

1. **Create dataset config**: `{dataset}/config.json` with `file_table_mapping` and `column_schema`
2. **Create dataset stack**: `{dataset}/{Dataset}Stack.js`:
   - Import deploy utilities: `const deployUtils = require("../utils-deploytime")`
   - For HTTP/ZIP sources: Point to shared `utils-runtime/https_zip/bronze_http_job.py`
   - Only create custom bronze job for non-HTTP/ZIP sources
3. **Update index.js**: Add stack with dependency on EtlCoreStack
4. **Deploy**: `cdk deploy --all`

**Role Decision:**

- Use shared role for public data sources
- Create custom role for APIs requiring authentication/secrets

## Critical Implementation Rules

**✅ Always Follow:**

- Compute paths by convention, don't use config patterns
- Use `getResolvedOptions()` with exact parameter names matching stack
- Stream file processing to handle any data size
- Include proper error handling and logging

**❌ Never Do:**

- Load entire files into memory
- Use string replacement on S3 paths
- Auto-trigger crawlers from bronze jobs
- Reference undefined variables after refactoring
