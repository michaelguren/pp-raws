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

**Glue Script Conventions:**

- Bronze: `etl/{dataset}/glue/bronze_job.py`
- Gold: `etl/{dataset}/glue/gold_job.py`

**Shared Code Organization:**

- `util-deploytime/` - Deploy-time utilities (used by CDK during deployment)
  - `EtlConfig.js` - Shared configuration methods and helpers
- `util-runtime/` - Runtime utilities (deployed to S3, used by Glue jobs)
  - `etl_utils.py` - Shared download, extraction, and file handling functions

## Stack Implementation Patterns

**EtlConfig Class:**
Use the centralized `EtlConfig.js` class for all configuration and helper methods:

```javascript
const etlConfig = require("../util-deploytime/EtlConfig");
const datasetConfig = require("./config.json");

// Get all computed values from EtlConfig methods
const databases = etlConfig.getDatabaseNames();
const resourceNames = etlConfig.getResourceNames(dataset);
const paths = etlConfig.getS3Paths(bucketName, dataset);
const workerConfig = etlConfig.getWorkerConfig(
  datasetConfig.data_size_category
);
```

**EtlConfig Methods:**

- `getDatabaseNames()` - returns `{bronze, gold}` database names
- `getResourceNames(dataset, options)` - generates job/crawler names
- `getS3Paths(bucketName, dataset, options)` - returns all standard S3 paths as **complete S3 URLs** (e.g., `s3://bucket/raw/dataset/`)
- `getWorkerConfig(sizeCategory)` - validates and returns worker configuration
- `getGlueJobArguments(options)` - builds complete job arguments including:
  - S3 URLs directly from `getS3Paths()` without modification
  - `column_schema` for schema-driven transformations

**Path Computation (Convention-based):**

```javascript
const path = require("path");

// ✅ Helper function returns complete S3 URLs
const s3Path = (bucket, ...segments) =>
  `s3://${bucket}/` + path.posix.join(...segments) + "/";

// Computed in stack files, not config - complete S3 URLs
const rawPath = s3Path(bucketName, "raw", dataset); // "s3://bucket/raw/dataset/"
const bronzePath = s3Path(bucketName, "bronze", dataset); // "s3://bucket/bronze/dataset/"
const scriptLocation =
  `s3://${bucketName}/` +
  path.posix.join("etl", dataset, "glue", "bronze_job.py");

// Database names computed from prefix
const bronzeDatabase = `${etlConfig.database_prefix}_bronze`;
```

**Python - Runtime Utilities and File Handling:**

```python
import json
import posixpath

# ✅ Load shared runtime utilities
sys.path.append('/tmp')
s3_client = boto3.client('s3')
s3_client.download_file(raw_bucket, 'etl/util-runtime/etl_utils.py', '/tmp/etl_utils.py')
from etl_utils import download_and_extract

# ✅ Stack passes complete S3 URLs and schema directly from getS3Paths()
raw_base_path = args['raw_path']      # "s3://bucket/raw/dataset/" - full URL from stack
bronze_s3_path = args['bronze_path']  # "s3://bucket/bronze/dataset/" - full URL from stack
file_table_mapping = json.loads(args['file_table_mapping'])
column_schema = json.loads(args['column_schema']) if 'column_schema' in args else None

# ✅ Append run_id to raw path for lineage tracking
scheme, path = raw_base_path.rstrip('/').split('://', 1)
raw_s3_path = f"{scheme}://{posixpath.join(path, f'run_id={run_id}')}/"

# ✅ Use shared utilities for download and extraction
result = download_and_extract(source_url, raw_s3_path, file_table_mapping)
csv_count = result["files_extracted"]

# ✅ Read and apply schema-driven transformations
csv_filename = list(file_table_mapping.keys())[0]
csv_file_path = posixpath.join(raw_s3_path, csv_filename)
df = spark.read.option("header", "true").csv(csv_file_path)

# ✅ Apply schema for column renaming and type casting
if column_schema:
    df = apply_schema(df, column_schema)  # Handles dates, types, renaming
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

- Use shared `etl_utils.download_and_extract()` for all file operations
- Leverage `file_table_mapping` for explicit file-to-table relationships
- **Use `column_schema` for predictable transformations** instead of guessing column types
- Apply schema with `apply_schema(df, column_schema)` function for consistent type casting
- Use `posixpath.join()` for safe S3 path construction
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
2. **Create Glue job**: `{dataset}/glue/bronze_job.py` using:
   - `etl_utils.download_and_extract()` for file operations
   - `apply_schema(df, column_schema)` for schema-driven transformations
3. **Create dataset stack**: `{dataset}/{Dataset}Stack.js` importing from `util-deploytime/EtlConfig`
4. **Update index.js**: Add stack with dependency on EtlCoreStack
5. **Deploy**: `cdk deploy --all`

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
