# ETL Data Pipeline Architecture

**Read `../CLAUDE.md` first** for general infrastructure requirements.

## Principles
- **Convention over Configuration**: Predictable naming and structure
- **Factory Pattern**: Bronze layers use shared code via `EtlStackFactory`
- **Kill-and-Fill**: Complete data refresh each run
- **Config-Driven**: `config.json` files define all dataset behavior

---

## ⚠️ CRITICAL RULE: Filesystem Paths = Deploy-Time Only

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  Filesystem paths (e.g., __dirname, path.join) are DEPLOY-TIME ONLY.       ║
║                                                                              ║
║  ❌ Runtime code (Glue jobs, Lambda handlers) must NEVER compute            ║
║     filesystem paths. They work exclusively with S3 URIs and /tmp.          ║
║                                                                              ║
║  ✅ CDK stacks use centralized path helper at:                              ║
║     infra/etl/shared/deploytime/paths.js                                    ║
║                                                                              ║
║  This separation keeps jobs portable and prevents mixing control-plane      ║
║  and data-plane concerns.                                                   ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Deploy-Time Path Usage (CDK Stacks):**
```javascript
// Import the centralized paths helper
const { glueScriptPath, sharedRuntimePath } = require('../../../shared/deploytime/paths');

// ✅ CORRECT - Use helper functions
new s3deploy.BucketDeployment(this, 'DeployGlueScript', {
  sources: [s3deploy.Source.asset(glueScriptPath(__dirname))],
  destinationBucket: dataWarehouseBucket,
  destinationKeyPrefix: `etl/${dataset}/glue/`
});

new s3deploy.BucketDeployment(this, 'DeployTemporalLib', {
  sources: [s3deploy.Source.asset(sharedRuntimePath(__dirname, 'temporal'))],
  destinationBucket: dataWarehouseBucket,
  destinationKeyPrefix: 'etl/shared/runtime/temporal/'
});

// ❌ INCORRECT - No raw path.join in stacks
const scriptPath = path.join(__dirname, 'glue');  // DON'T DO THIS
const sharedLib = path.join(__dirname, '../../../shared/runtime/temporal');  // DON'T DO THIS
```

**Runtime Path Usage (Glue Jobs):**
```python
# ✅ CORRECT - Work with S3 URIs only
silver_df = glueContext.create_dynamic_frame.from_catalog(
    database=args['silver_database'],
    table_name=args['silver_table']
).toDF()

df.write.mode("overwrite").parquet(args['gold_base_path'])  # S3 URI passed from CDK

# ✅ CORRECT - Use /tmp for temporary files
with open('/tmp/temp_file.csv', 'w') as f:
    f.write(data)

# ❌ INCORRECT - Never compute filesystem paths
import os
script_dir = os.path.dirname(__file__)  # DON'T DO THIS in runtime code
```

**Available Path Helpers:**

| Helper Function | Use Case | Example |
|----------------|----------|---------|
| `glueScriptPath(__dirname)` | Dataset-local `glue/` directory | Returns `{stackDir}/glue` |
| `sharedRuntimePath(__dirname, 'temporal')` | Shared runtime libraries | Returns path to `infra/etl/shared/runtime/temporal` |
| `rootSharedRuntimePath(__dirname)` | From EtlCoreStack (root level) | Returns `{etlRoot}/shared/runtime` |
| `sharedLambdaPath(__dirname)` | Orchestration Lambda functions | Returns path to `orchestrations/shared/lambdas` |

**Benefits:**
- **Single source of truth**: Directory refactors require changing only one file
- **Clear separation**: Deploy-time vs runtime concerns never mix
- **Portable jobs**: Runtime code works anywhere (local Spark, EMR, Glue)
- **Readable stacks**: Intent is clear from helper function names

---

## 📋 Canonical Reference: The FDA Pipeline (NSDE → CDER → fda-all-ndcs)

**When building new ETL pipelines, start by copying the FDA pipeline structure.**

This is the **reference implementation** demonstrating the complete RAWS pattern:

**Bronze Layer** (Pattern A):
- `datasets/bronze/fda-nsde/` - Factory-based HTTP/ZIP ingestion
- `datasets/bronze/fda-cder/` - Factory-based HTTP/ZIP ingestion
- Shared `bronze_http_job.py` runtime, zero Glue API calls

**Silver Layer** (Custom):
- `datasets/silver/fda-all-ndcs/` - INNER JOIN of NSDE + CDER
- Custom transformation logic, zero Glue API calls
- Demonstrates multi-source data lineage

**Gold Layer** (Temporal + Delta):
- `datasets/gold/fda-all-ndcs/` - **Delta Lake with Glue Crawler**
- Temporal versioning (SCD Type 2) via shared library
- Delta Lake 3.0 (`--datalake-formats=delta`)
- Automatic schema evolution via Crawler + `mergeSchema=true`
- Zero Glue API calls, pure data-plane

**Why This Pipeline Is The Reference:**
1. ✅ Complete Bronze → Silver → Gold medallion flow
2. ✅ Both factory pattern (Bronze) and custom stacks (Silver/Gold)
3. ✅ Multi-source joins with data quality (INNER JOIN only)
4. ✅ Temporal versioning for incremental DynamoDB sync
5. ✅ Delta Lake ACID transactions and schema evolution
6. ✅ Zero crawler coupling - Step Functions owns orchestration
7. ✅ Centralized paths helper for deploy-time filesystem access

**For new pipelines:** Copy the FDA structure, swap out the data sources and transformation logic, keep the architectural patterns.

**See also:** `DELTA_TABLE_REGISTRATION.md` for Delta Lake implementation details.

---

## ⚠️ HARD RULE: Jobs Never Touch Glue Catalog

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  🚫 Glue jobs MUST NOT call Glue APIs or register tables/partitions.       ║
║                                                                              ║
║  ✅ All catalog management is done via Glue Crawlers + Step Functions.     ║
║                                                                              ║
║  ✅ Jobs are pure data-plane: read → transform → write to S3.              ║
║                                                                              ║
║  This rule prevents brittle, non-deterministic catalog management.          ║
║  Violation of this rule will cause orchestration failures and tech debt.    ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**What this means in practice:**
- ❌ **NEVER** use `boto3.client("glue")` in job code
- ❌ **NEVER** call `glue.create_table()`, `glue.update_table()`, `glue.create_partition()`
- ❌ **NEVER** invoke crawlers programmatically from jobs
- ❌ **NEVER** check if tables exist via Glue APIs
- ✅ **ALWAYS** delegate catalog concerns to Step Functions orchestration
- ✅ **ALWAYS** use `glueContext.create_dynamic_frame.from_catalog()` to read tables (AWS-native method)

**Enforcement:** All existing jobs (NSDE, CDER, fda-all-ndcs Silver/Gold) follow this rule. Do not regress.

---

## Blessed Pattern: Data-Plane vs. Control-Plane Separation

**Principle**: Jobs handle data transformation. Orchestration handles infrastructure coordination.

### Core Tenet

> **Jobs should never call `boto3.client("glue")` or manage the Glue Data Catalog.**
>
> Catalog management is an orchestration concern, not a data engineering concern.

### What This Means

**Data-Plane (Glue Jobs)**:
- ✅ Read data from S3 or Glue catalog (via `glueContext.create_dynamic_frame.from_catalog()`)
- ✅ Transform, join, aggregate, enrich data
- ✅ Write data to S3 (with appropriate partitioning in paths)
- ✅ Log completion status
- ❌ **NEVER** call Glue APIs to create/update tables
- ❌ **NEVER** call Glue APIs to register partitions
- ❌ **NEVER** invoke crawlers programmatically
- ❌ **NEVER** check if tables exist via boto3

**Control-Plane (Step Functions + Crawlers)**:
- ✅ Start Glue jobs
- ✅ Check if tables exist in Glue catalog (via Lambda)
- ✅ Start crawlers when tables are missing (via Lambda)
- ✅ Wait for crawlers to complete
- ✅ Coordinate dependencies (Bronze → Silver → Gold)
- ✅ Handle retries and error cases

**Infrastructure (CDK Stacks)**:
- ✅ Define Glue jobs (no catalog awareness in job code)
- ✅ Define crawlers with schema change policies
- ✅ Define Step Functions orchestration
- ✅ For Delta Lake: Use Glue Crawlers with schema change policies (see DELTA_TABLE_REGISTRATION.md)

### Reference Implementation

**FDA NSDE Bronze** (`datasets/bronze/fda-nsde/`):
- Uses factory pattern → `EtlStackFactory`
- Shared job: `shared/runtime/https_zip/bronze_http_job.py`
- **Zero Glue API calls** in job code
- Final line: `print("Note: Run crawlers to update Glue catalog")`
- Crawler defined in CDK, invoked by Step Functions

**FDA All NDCs Silver** (`datasets/silver/fda-all-ndcs/`):
- Custom transformation job
- Reads Bronze tables via `glueContext.create_dynamic_frame.from_catalog()`
- Writes to S3 Parquet
- **Zero Glue API calls**
- Comment: `# Table registration handled by Step Functions orchestration`

**FDA All NDCs Gold** (`datasets/gold/fda-all-ndcs/`):
- Temporal versioning (SCD Type 2)
- Reads Silver tables via catalog
- Writes Delta Lake to S3
- **Zero Glue API calls**
- Crawler with schema change policies managed via CDK

### Benefits

1. **Testability**: Jobs can be unit tested without AWS infrastructure
2. **Portability**: Jobs work with any catalog (Hive, Iceberg REST, Lake Formation)
3. **Debuggability**: Catalog issues? Check orchestration. Data issues? Check job.
4. **Maintainability**: Single source of truth for catalog strategy
5. **Future-proof**: Easy to swap catalog implementations (e.g., Glue → Iceberg REST)
6. **Schema Evolution**: Boring, predictable column additions without breaking changes

### Schema Evolution Pattern

**For Gold Delta Lake tables** (temporal versioning):
- Jobs use `mergeSchema=true` (write operations) + `spark.databricks.delta.schema.autoMerge.enabled=true` (MERGE operations)
- Crawlers use `schemaChangePolicy: { updateBehavior: 'UPDATE_IN_DATABASE', deleteBehavior: 'DEPRECATE_IN_DATABASE' }`
- **Rule**: Add new nullable columns only; never drop/rename columns in place
- **Result**: Schema evolution becomes routine, not a production incident

See Gold layer README files for schema evolution test validation (Run 5).

### Anti-Patterns to Avoid

❌ **Job-driven catalog management**:
```python
# DON'T DO THIS
import boto3
glue = boto3.client("glue")
glue.start_crawler(Name="my-crawler")  # Job shouldn't know about crawlers
```

❌ **Mixed responsibilities**:
```python
# DON'T DO THIS
if table_exists(database, table):
    df.write.mode("overwrite").parquet(path)
else:
    create_table(database, table)  # Orchestration's job, not data engineering
```

❌ **Passing crawler names to jobs**:
```javascript
// DON'T DO THIS (in CDK)
'--crawler_name': 'my-crawler'  // Job shouldn't know infrastructure names
```

### Migration Path

If you have jobs with Glue API calls:
1. Remove all `boto3.client("glue")` usage from job code
2. Ensure CDK creates crawlers with proper schema change policies
3. Verify Step Functions orchestration checks tables and runs crawlers
4. Test: Job → Check tables Lambda → Crawler → Verify catalog

### Delta Lake Gold Tables

For Gold tables using Delta Lake, use Glue Crawlers with schema change policies (see `DELTA_TABLE_REGISTRATION.md`). Modern Glue Crawlers properly detect Delta format and support automatic schema evolution.

**Historical Note**: Earlier implementations used `AwsCustomResource` (Lambda-backed), which is now deprecated. The current pattern uses `glue.CfnCrawler` with `UPDATE_IN_DATABASE` / `DEPRECATE_IN_DATABASE` policies.

**Maintenance Operations**: Delta Lake tables require periodic OPTIMIZE, ZORDER BY, and VACUUM operations. See [Delta Lake Maintenance Strategy](DELTA_TABLE_REGISTRATION.md#delta-lake-maintenance-strategy) for implementation guidance, retention policies, and automation roadmap.

## Architecture
```
EventBridge → Glue Job → S3 (raw/bronze/gold) → Crawler → Athena
```

## Directory Structure
```
etl/
├── config.json                        # Global config (prefixes, worker sizes)
├── index.js                           # Stack orchestrator
├── EtlCoreStack.js                   # Shared infrastructure (S3, IAM, databases)
├── datasets/                          # Organized by medallion layer (RAWS convention)
│   ├── bronze/{dataset}/             # Raw data ingestion datasets
│   │   ├── config.json               # Dataset config (source URL, schema, schedule)
│   │   ├── {Dataset}Stack.js         # Stack definition
│   │   └── glue/*.py                 # Custom bronze jobs (Pattern B only)
│   ├── silver/{dataset}/             # Transformed/joined datasets
│   │   ├── config.json               # Dataset config (dependencies, transformations)
│   │   ├── {Dataset}Stack.js         # Stack definition (always custom)
│   │   └── glue/*.py                 # Custom transformation jobs
│   └── gold/{dataset}/               # Curated, analytics-ready datasets
│       ├── config.json               # Dataset config (dependencies, temporal config)
│       ├── {Dataset}Stack.js         # Stack definition (always custom)
│       └── glue/*.py                 # Custom gold layer jobs
└── shared/
    ├── deploytime/
    │   ├── factory.js                # CDK factory for bronze stacks
    │   └── index.js                  # Shared deployment utilities
    └── runtime/
        ├── https_zip/
        │   ├── bronze_http_job.py    # Shared bronze job for HTTP/ZIP sources
        │   └── etl_runtime_utils.py  # Runtime utilities (packaged via --extra-py-files)
        └── temporal/
            └── temporal_versioning.py # Shared temporal versioning for gold layer
```

## Configuration Files

**Global** (`etl/config.json`): Resource naming, database prefixes, worker sizes

**Dataset** (`datasets/{dataset}/config.json`):
```json
{
  "dataset": "dataset-name",
  "source_url": "https://...",
  "data_size_category": "small|medium|large|xlarge",
  "delimiter": ",|\t||",
  "file_table_mapping": {
    "source.csv": "table-name"    // Hyphens → underscores in Glue tables
  },
  "column_schema": {
    "table-name": {
      "Source Column": {
        "target_name": "snake_case",
        "type": "string|date|integer|long|float|double|decimal|boolean",
        "format": "yyyyMMdd",       // dates only
        "precision": "10,2"         // decimal only (optional, defaults to 10,2)
      }
    }
  }
}
```

## Naming Conventions

**AWS Resources**: `{prefix}-{layer}-{dataset}[-{table}][-crawler]`
- Glue Jobs: `pp-dw-bronze-fda-nsde`, `pp-dw-silver-fda-all-ndc`, `pp-dw-gold-fda-all-ndcs`
- Crawlers: `pp-dw-bronze-fda-products-crawler`, `pp-dw-gold-fda-all-ndcs-crawler`
- Databases: `pp_dw_bronze`, `pp_dw_silver`, `pp_dw_gold` (underscores, not hyphens)
- Glue Tables: Hyphens in dataset names → underscores (`fda-all-ndcs` → `fda_all_ndcs`)
- **Naming Convention**: Gold datasets use **plural names** for consistency

**S3 Paths**:
```
s3://pp-dw-{account}/
├── raw/{dataset}/run_id={timestamp}/
├── bronze/{dataset}/              # Single-table datasets
├── bronze/{dataset}/{table}/      # Multi-table datasets
├── silver/{dataset}/              # Silver layer tables
├── gold/{dataset}/                # Gold layer tables (status-partitioned)
└── etl/{dataset}/glue/            # Dataset-specific scripts
```

## Stack Types

### Bronze Layer Patterns

**Pattern A: Factory-Based (Standard HTTP/ZIP CSV Sources)**
Use for datasets with CSV/TSV files, standard delimiters, and column headers:
```javascript
const EtlStackFactory = require("../../shared/deploytime/factory");
const datasetConfig = require("./config.json");
const factory = new EtlStackFactory(this, props);

factory.createDatasetInfrastructure({
  datasetConfig,
  options: { skipSilverJob: true }  // Bronze only
});
```
- Uses shared `bronze_http_job.py` and `etl_runtime_utils.py`
- Schema defined in `config.json` (column mappings, types)
- Supports single and multi-table datasets
- Examples: `fda-nsde`, `fda-cder`

**📋 Reference Implementation: FDA NSDE**

`infra/etl/datasets/bronze/fda-nsde/` is the **canonical Pattern A implementation**.

**Part of the FDA Pipeline**: NSDE + CDER → fda-all-ndcs (see canonical reference at top of this document)

This is the "blessed" bronze pattern. New HTTP/ZIP datasets should copy this structure:

**What makes it canonical:**
- ✅ Uses `EtlStackFactory` (no custom stack code)
- ✅ Uses shared `bronze_http_job.py` (no custom job code)
- ✅ **Zero Glue API calls** in job code (pure data-plane)
- ✅ Catalog entirely crawler-owned (orchestration-driven table registration)
- ✅ Clean separation: data-plane (job) vs control-plane (orchestration)

**What to copy:**
1. Stack structure: `FdaNsdeStack.js` with factory pattern
2. Config structure: `config.json` with schema definitions
3. Orchestration: Step Functions handles "job → check-tables → start-crawlers"
4. Job behavior: Writes to S3, prints completion, delegates catalog to orchestration

**When to use Pattern B instead:**
- Non-CSV formats (RRF, JSON, binary)
- Authentication requirements (API keys, UMLS, OAuth)
- Complex transformations beyond column mapping

**Pattern B: Custom Bronze Jobs (Specialized Sources)**
Use for datasets with unique formats, authentication, or processing requirements:
- Custom `{Dataset}Stack.js` with manual Glue job creation
- Custom `glue/bronze_job.py` with specialized logic
- May have inline schema definitions or custom processing
- Examples:
  - **RxNORM**: RRF files (no headers, pipe-delimited, UMLS auth)
  - **APIs**: REST/GraphQL sources requiring pagination, auth tokens
  - **Binary formats**: Parquet, Avro, or proprietary formats

**Silver Stacks** (always custom, no factory):
- Read source dataset configs at deploy time for table names
- Pass table names as Glue job arguments
- Custom transformation logic in `glue/*.py`
- Examples:
  - `fda-all-ndc` joins `fda-nsde` + `fda-cder` tables
  - `rxnorm-ndc-mappings` extracts NDC mappings from RXNSAT
  - `rxnorm-products` filters prescribable products from RxNORM

## Gold Layer - Temporal Versioning (SCD Type 2)

Gold datasets use **temporal versioning** to enable efficient incremental DynamoDB syncs and historical tracking.

### Core Pattern
- **End-of-Time**: Current records use `active_to = '9999-12-31'` (not NULL)
- **Status Partitioning**: Physical separation into `status=current/` and `status=historical/`
- **Change Detection**: MD5 hash of business columns identifies changes
  - NEW: Insert with `active_to=9999-12-31`
  - CHANGED: Expire old (set `active_to=today`), insert new
  - UNCHANGED: Skip (no write)
  - EXPIRED: Set `active_to=today`

### Schema
```
# Version tracking
version_id       STRING    UUID for this version
content_hash     STRING    MD5 of business columns
active_from      DATE      When version became active
active_to        DATE      When expired (9999-12-31 = current)
status           STRING    Partition: current or historical

# Business columns (no layer prefixes)
ndc_11, proprietary_name, ...           (fda-all-ndcs)
rxcui, str, ingredient_names, ...       (rxnorm-products)
rxcui, ndc_11, ...                      (rxnorm-ndc-mappings - composite key)
rxcui, class_id, class_type, ...        (rxnorm-product-classifications - composite key)

# Metadata
run_date, run_id, source_file
```

### Shared Library
**Location**: `shared/runtime/temporal/temporal_versioning.py`

**Key Functions**:
```python
# Apply temporal versioning to incoming data
versioned_df = apply_temporal_versioning(
    spark, incoming_df, existing_table,
    business_key, business_columns, run_date, run_id
)

# Query helpers
get_current_records(spark, table)              # WHERE active_to = '9999-12-31'
get_changes_for_date(spark, table, date)       # Incremental sync query
```

### Common Queries
```sql
-- Current records only
SELECT * FROM pp_dw_gold.fda_all_ndcs WHERE status = 'current';

-- Incremental changes for DynamoDB sync
SELECT * FROM pp_dw_gold.fda_all_ndcs
WHERE run_date = '2025-10-14'
  AND (active_from = '2025-10-14' OR active_to = '2025-10-14');

-- Point-in-time historical query
SELECT * FROM pp_dw_gold.fda_all_ndcs
WHERE ndc_11 = '00002322702'
  AND '2024-01-01' BETWEEN active_from AND active_to;

-- Composite key query (mappings)
SELECT * FROM pp_dw_gold.rxnorm_ndc_mappings
WHERE rxcui = '392409' AND status = 'current';

-- Composite key query (classifications)
SELECT * FROM pp_dw_gold.rxnorm_product_classifications
WHERE rxcui = '392409' AND class_type = 'ATC' AND status = 'current';
```

### Benefits
- **95% cost reduction** in DynamoDB writes (~380K → ~500 writes/day)
- **Historical queries** - view data as it existed at any point in time
- **Audit trail** - complete change history
- **Fast queries** - status partitioning enables partition pruning

---

## 🔥 RAWS Delta Lake Invariants (Production-Hardened)

**CRITICAL: November 19, 2025 Incident Prevention**

The following invariants are **non-negotiable** for all Gold Delta Lake implementations. They prevent the catastrophic false-change explosion that occurred on November 19, 2025, where including volatile metadata in `content_hash` caused all ~80K records to be expired and re-inserted, destroying temporal history.

### The 5 Commandments

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                        RAWS DELTA INVARIANTS                                 ║
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  1. Business content_hash MUST ONLY include stable source business          ║
║     attributes. NEVER hash: run_date, run_id, source_run_id, source_file,  ║
║     meta_run_id, ingestion_timestamp, or any RAWS metadata.                 ║
║                                                                              ║
║  2. First-ever Delta write on S3 MUST use .mode("append"), never            ║
║     "overwrite". Subsequent writes MUST use MERGE for SCD2.                 ║
║                                                                              ║
║  3. Always set spark.delta.logStore.class=S3SingleDriverLogStore via       ║
║     Glue job --conf (AWS best practice for S3 reliability).                 ║
║                                                                              ║
║  4. Schema evolution MUST be enabled (mergeSchema=true) on all writes.      ║
║                                                                              ║
║  5. Use refuse_accidental_overwrite() guard with binaryFile check as        ║
║     final seatbelt against orphan Parquet / flaky first-commit issues.      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### Invariant 1: Business-Only Content Hash

**The November 19 Incident (Root Cause):**
```python
# ❌ WRONG - Caused production incident
business_columns = [
    "ndc_11", "proprietary_name", "dosage_form",
    "run_date",        # 🔥 VOLATILE - Changes every run
    "run_id",          # 🔥 VOLATILE - Changes every run
    "source_run_id"    # 🔥 VOLATILE - Changes every run
]
# Result: ALL rows appeared CHANGED → all expired + re-inserted → history destroyed
```

**Production-Hardened Fix:**
```python
# ✅ CORRECT - Explicit business columns only
business_columns = [
    "ndc_11", "ndc_5", "proprietary_name", "dosage_form",
    "marketing_category", "product_type", "application_number",
    "dea_schedule", "package_description", "active_numerator_strength",
    "active_ingredient_unit", "spl_id", "marketing_start_date",
    "marketing_end_date", "billing_unit", "nsde_flag"
]
# Library automatically filters out volatile metadata using VOLATILE_METADATA_COLUMNS denylist
```

**Explicit Denylist (Enforced by Library):**
```python
VOLATILE_METADATA_COLUMNS = {
    # RAWS ETL metadata
    "run_date", "run_id", "source_run_id", "meta_run_id",
    "source_file", "ingestion_timestamp",

    # Temporal versioning mechanics
    "version_id", "content_hash", "active_from", "active_to", "status",

    # Audit timestamps
    "created_at", "updated_at", "loaded_at", "processed_at"
}
```

**Validation Behavior:**
The `business_columns_only()` validator in `temporal_versioning_delta.py`:
1. ✅ Filters volatile columns using explicit denylist
2. ✅ Fails fast if all columns are filtered (empty business columns)
3. ✅ Logs which columns were removed and which are used for hashing
4. ✅ Returns only stable business attributes for content_hash calculation

### Invariant 2: First Write Must Be Append

**AWS/Delta.io Best Practice:**
```python
# ✅ CORRECT - Initial load
result_df.write \
    .format("delta") \
    .mode("append") \          # APPEND, not overwrite
    .partitionBy("status") \
    .option("mergeSchema", "true") \
    .save(gold_path)

# ❌ WRONG - Can cause orphan Parquet and flaky _delta_log commits
result_df.write.mode("overwrite").save(gold_path)
```

**Reference:** https://delta.io/blog/delta-lake-s3/

### Invariant 3: S3SingleDriverLogStore Configuration

**CDK Configuration (Required):**
```javascript
// In GoldStack.js defaultArguments
'--datalake-formats': 'delta',
'--conf': 'spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore'
```

**Why This Matters:**
- Prevents transaction log conflicts in S3
- Avoids race conditions during concurrent writes
- Ensures consistent `_delta_log/` state
- AWS Glue best practice for Delta Lake on S3

**Reference:** https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html

### Invariant 4: Schema Evolution Always Enabled

```python
# In library: apply_temporal_versioning_delta()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# On every write
.option("mergeSchema", "true")
```

**Benefit:** Adding new columns becomes a non-event, not a production incident.

### Invariant 5: Orphan Parquet Guard

**The Glue Foot-Gun:**
Orphan Parquet files can exist without `_delta_log/`, causing Delta to treat the location as empty and overwrite existing data.

**Protection:**
```python
def refuse_accidental_overwrite(spark: SparkSession, gold_path: str):
    """Guard against orphan Parquet files"""
    orphan_files = spark.read.format("binaryFile").load(gold_path).limit(1).count()
    if orphan_files > 0 and not DeltaTable.isDeltaTable(spark, gold_path):
        raise RuntimeError(f"ORPHAN PARQUET DETECTED at {gold_path}")
```

**When It Runs:** Before any initial Delta write operation.

### Production Validation Checklist

Before deploying Gold Delta Lake jobs:

- [ ] business_columns contains ONLY stable source attributes
- [ ] No volatile metadata in business_columns (run_date, run_id, etc.)
- [ ] CDK has `--conf` for S3SingleDriverLogStore
- [ ] Initial write uses `.mode("append")`
- [ ] All writes have `.option("mergeSchema", "true")`
- [ ] `refuse_accidental_overwrite()` guard is called before first write
- [ ] `DeltaTable.isDeltaTable()` used for first-time detection
- [ ] SparkConf configured BEFORE SparkContext creation

### References

- **AWS Glue Delta Lake Best Practices (2025):**
  https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html

- **Delta Lake S3 Reliability:**
  https://delta.io/blog/delta-lake-s3/

- **Incident Report:**
  See `DELTA_TABLE_REGISTRATION.md` for complete November 19, 2025 post-mortem

- **Reference Implementation:**
  `infra/etl/datasets/gold/fda-all-ndcs/` - Canonical Gold Delta pattern

---

## Reading from Glue Data Catalog

**CRITICAL: Always use `glueContext.create_dynamic_frame.from_catalog()` to read tables from the Glue Data Catalog.**

### Correct Pattern
```python
# ✅ CORRECT - Use GlueContext to read from catalog
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read table from Glue catalog and convert to DataFrame
rxnsat_df = glueContext.create_dynamic_frame.from_catalog(
    database="pp_dw_bronze",
    table_name="rxnsat"
).toDF()
```

### Incorrect Pattern
```python
# ❌ INCORRECT - spark.table() will fail with "TABLE_OR_VIEW_NOT_FOUND"
rxnsat_df = spark.table("pp_dw_bronze.rxnsat")  # DON'T DO THIS!

# ❌ INCORRECT - Even with catalog specified
rxnsat_df = spark.table(f"{database}.{table}")  # DON'T DO THIS!
```

### Why This Matters
- `spark.table()` requires proper Hive metastore configuration and catalog settings
- `glueContext.create_dynamic_frame.from_catalog()` is the AWS-native method that works reliably with Glue Data Catalog
- The GlueContext method handles all catalog configuration automatically
- This is the recommended pattern in AWS Glue documentation

### Common Usage Patterns

**Single table read:**
```python
df = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name="rxnsat"
).toDF()
```

**Multiple table reads with job arguments:**
```python
# Stack passes table names as job arguments
rxnconso = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name=args['rxnconso_table']
).toDF()

rxnrel = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name=args['rxnrel_table']
).toDF()
```

**Reading from different layers:**
```python
# Read from bronze
bronze_df = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name="fda_nsde"
).toDF()

# Read from silver
silver_df = glueContext.create_dynamic_frame.from_catalog(
    database=args['silver_database'],
    table_name="rxnorm_ndc_mappings"
).toDF()
```

---

## Delta Lake Configuration for Gold Layer (AWS Glue 5.0)

**CRITICAL: Proper Delta Lake configuration requires specific setup in both CDK and Python.**

**📋 See `DELTA_TABLE_REGISTRATION.md` for full architectural decision record.**

### CDK Stack Configuration (GoldStack.js)

#### 1. Glue Job Configuration

```javascript
defaultArguments: {
  // Enable Delta Lake support - loads libraries and modules
  '--datalake-formats': 'delta',

  // Deploy temporal versioning library
  '--extra-py-files': `${temporalLibPath}temporal_versioning_delta.py`,

  // DO NOT use --conf here! It causes "Invalid input to --conf" errors in Glue
  // Spark configs must be set in Python code BEFORE SparkContext creation
}
```

#### 2. Delta Table Registration (Glue Crawler Pattern)

**✅ USE GLUE CRAWLERS with schema change policies for Delta Lake tables**

Modern Glue Crawlers properly detect Delta Lake format and support automatic schema evolution.

**Historical Note**: Earlier implementations used `AwsCustomResource` (Lambda-backed custom resource), which added procedural complexity and IAM overhead. This pattern is now deprecated.

**Production Pattern: `glue.CfnCrawler` with Native Delta Lake Support:**

```javascript
const cdk = require("aws-cdk-lib");
const glue = require("aws-cdk-lib/aws-glue");

// Build paths using convention
const goldBasePath = `s3://${bucketName}/gold/${dataset}/`;

// Create Glue Crawler for Delta Lake table discovery
// Uses native deltaTargets (AWS Glue 2025+ best practice)
const goldCrawler = new glue.CfnCrawler(this, 'GoldCrawler', {
  name: `${etlConfig.etl_resource_prefix}-gold-${dataset}-crawler`,
  role: glueRole.roleArn,
  databaseName: goldDatabase,
  description: `Crawler for Gold Delta table ${dataset}`,

  // Native Delta Lake target - automatically detects Delta format
  // Prevents garbage tables from _delta_log/ directory
  targets: {
    deltaTargets: [{
      deltaTables: [goldBasePath],  // s3://.../gold/fda-all-ndcs/
      writeManifest: false
    }]
  },

  // Schema change policies enable automatic evolution
  schemaChangePolicy: {
    updateBehavior: 'UPDATE_IN_DATABASE',      // Add new columns
    deleteBehavior: 'DEPRECATE_IN_DATABASE'    // Mark removed columns
  },

  // Crawler configuration for Delta Lake
  configuration: JSON.stringify({
    Version: 1.0,
    CrawlerOutput: {
      Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
      Tables: { AddOrUpdateBehavior: "MergeNewColumns" }
    }
  })
});

// Crawler depends on bucket (not job - they're peers)
goldCrawler.node.addDependency(dataWarehouseBucket);

// Export for Step Functions orchestration
this.goldCrawler = goldCrawler;
```

### ✅ Native Delta Lake Crawling (2025 Best Practice)

**Modern Approach**: Use `deltaTargets` instead of `s3Targets` + classifiers.

**Why `deltaTargets` is better**:
- ✅ Purpose-built for Delta Lake tables
- ✅ Automatically detects Delta format (no classifier needed)
- ✅ Prevents garbage tables from `_delta_log/` directory
- ✅ Simpler configuration (one property vs two)
- ✅ Official AWS recommendation for Glue 5.0+

**Old Pattern (deprecated)**:
```javascript
classifiers: ['delta-lake'],  // No longer needed
targets: {
  s3Targets: [{ path: goldBasePath }]  // Use deltaTargets instead
}
```

**New Pattern (current)**:
```javascript
targets: {
  deltaTargets: [{
    deltaTables: [goldBasePath],
    writeManifest: false
  }]
}
```

**Rule**: If your Glue job has `'--datalake-formats': 'delta'`, your crawler MUST use `deltaTargets`.

**Verification**:
```bash
# After deploying and running crawler, check Glue catalog
aws glue get-table --database-name pp_dw_gold --name fda_all_ndcs --query 'Table.Parameters'

# Expected output (Delta Lake correctly detected):
{
  "classification": "delta",
  "table_type": "DELTA",
  "EXTERNAL": "TRUE"
}
```

**Step Functions Orchestration** (control-plane, NOT data-plane):

Jobs write data to S3. Step Functions orchestrates catalog registration:
1. Run Glue job (writes Delta data to S3)
2. Check-tables Lambda (verifies if table exists)
3. Start-crawlers Lambda (invokes crawler if needed)
4. Wait for crawler completion

**Production Benefits:**
- ✅ Pure CloudFormation (no Lambda-backed custom resources)
- ✅ Automatic Delta format detection from `_delta_log/`
- ✅ Schema evolution via `UPDATE_IN_DATABASE` / `DEPRECATE_IN_DATABASE`
- ✅ Separation of concerns (jobs write data, orchestration manages catalog)
- ✅ Zero job-level Glue API calls (`boto3.client("glue")`)
- ✅ Consistent with Bronze/Silver layers (same pattern everywhere)
- ✅ Fully declarative infrastructure

**See `DELTA_TABLE_REGISTRATION.md` for complete architectural decision record and alternative patterns (CfnTable).**

### Python Job Configuration (gold_job.py)

**✅ CORRECT: Configure Spark BEFORE creating SparkContext**

```python
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Set Delta Lake configs BEFORE SparkContext creation
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Initialize with configured SparkConf
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
```

**❌ WRONG: Do NOT set configs after SparkContext creation**

```python
sc = SparkContext()  # SparkSession created here
spark.conf.set(...)  # TOO LATE! Causes "Cannot modify static config" error
```

### Common Delta Lake Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError: No module named 'delta'` | Missing `--datalake-formats` | Add `'--datalake-formats': 'delta'` in CDK |
| `Cannot modify the value of a static config` | Setting config after SparkContext | Use `SparkConf` before `SparkContext()` |
| `Invalid input to --conf` | Using `--conf` in CDK arguments | Remove `--conf`, use Python `SparkConf` instead |
| `Delta operation requires SparkSession to be configured` | Missing Spark extensions/catalog | Set via `SparkConf` in Python before context creation |
| Athena returns 0 rows despite data in S3 | Table not registered or wrong metadata | Run crawler via Step Functions orchestration |
| `TABLE_OR_VIEW_NOT_FOUND` | Table not registered in Glue catalog | Deploy CDK stack with Crawler, run orchestration |

### Stack Dependencies

Always ensure scripts are deployed before job creation:

```javascript
const scriptDeployment = new s3deploy.BucketDeployment(this, 'DeployGlueScript', {...});
const temporalDeployment = new s3deploy.BucketDeployment(this, 'DeployTemporalLib', {...});

// Prevent race conditions
goldJob.node.addDependency(scriptDeployment);
goldJob.node.addDependency(temporalDeployment);
```

---

## Data Flow

**Bronze Layer** (automated via factory + shared `bronze_http_job.py`):
1. Download ZIP from `source_url`
2. Extract files to `s3://.../raw/{dataset}/run_id={timestamp}/` (lineage tracking)
3. Read CSV files from raw layer
4. Apply `column_schema` transformations (rename, type cast, dates, etc.)
5. Add `meta_run_id` metadata column
6. Write to `s3://.../bronze/{dataset}/[{table}/]` as Parquet/ZSTD (kill-and-fill)
7. Crawler updates Glue catalog

**Key Features**:
- Runtime utilities (`etl_runtime_utils.py`) packaged via `--extra-py-files`
- Schema transformations support: `string`, `date`, `integer`, `long`, `float`, `double`, `decimal`, `boolean`
- Single-table datasets write to `bronze/{dataset}/`, multi-table to `bronze/{dataset}/{table}/`
- Raw layer preserves complete lineage with run_id partitions

**Silver Layer** (custom transformations):
- Read multiple bronze tables from Glue catalog
- Join/transform/aggregate as needed
- Write to `s3://.../silver/{dataset}/` as Parquet/ZSTD
- Crawler updates Glue catalog

**Gold Layer** (temporal versioning):
- Read silver tables from Glue catalog
- Apply temporal versioning via shared library
- Compare incoming data with existing gold table (MD5 content hash)
- Write new/changed/expired versions to `s3://.../gold/{dataset}/` as Parquet/ZSTD
- Partition by `status` (current/historical)
- Crawler updates Glue catalog

## Adding New Datasets

**Bronze - Pattern A (Factory-Based)**:
1. Create `datasets/bronze/{dataset}/config.json` with schema
2. Create `datasets/bronze/{dataset}/{Dataset}Stack.js` using factory pattern
3. Add to `index.js` under "// Import dataset stacks - Bronze"

**Bronze - Pattern B (Custom)**:
1. Create `datasets/bronze/{dataset}/config.json` (optional, for metadata)
2. Create custom `datasets/bronze/{dataset}/{Dataset}Stack.js` with manual Glue job setup
3. Create custom `datasets/bronze/{dataset}/glue/bronze_job.py` with specialized logic
4. Add to `index.js` under "// Import dataset stacks - Bronze"

**Silver** (always custom):
1. Create `datasets/silver/{dataset}/config.json` with source dependencies
2. Create custom `datasets/silver/{dataset}/{Dataset}Stack.js` (read source configs, pass table names)
3. Create `datasets/silver/{dataset}/glue/silver_job.py` with transformation logic
4. Add to `index.js` under "// Import dataset stacks - Silver"

**Gold** (always custom, uses temporal versioning):
1. Create `datasets/gold/{dataset}/config.json` with:
   - Source dependencies (silver or bronze tables)
   - `temporal_config`: `business_key` (string or array for composite keys), `business_columns` for change detection
   - **Use plural dataset names** (e.g., `fda-all-ndcs`, `rxnorm-ndc-mappings`)
2. Create custom `datasets/gold/{dataset}/{Dataset}Stack.js`
   - Deploy temporal library: `shared/runtime/temporal/temporal_versioning.py`
   - Pass library path via `--extra-py-files`
3. Create `datasets/gold/{dataset}/glue/gold_job.py`:
   - Import and use `apply_temporal_versioning()` from shared library
   - Drop layer-specific prefixes from column names
   - Support composite keys (pass array to `business_key` parameter)
4. Add to `index.js` under "// Import dataset stacks - Gold"

**RAWS Conventions**:
- All datasets MUST be organized into bronze/, silver/, or gold/ subdirectories based on their medallion layer
- Gold datasets use **plural names** for consistency
- Composite keys are supported for N:M relationships (e.g., rxcui + ndc_11, rxcui + class_id + class_type)
