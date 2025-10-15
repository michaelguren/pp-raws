# ETL Data Pipeline Architecture

**Read `../CLAUDE.md` first** for general infrastructure requirements.

## Principles
- **Convention over Configuration**: Predictable naming and structure
- **Factory Pattern**: Bronze layers use shared code via `EtlStackFactory`
- **Kill-and-Fill**: Complete data refresh each run
- **Config-Driven**: `config.json` files define all dataset behavior

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
└── etl/datasets/{dataset}/glue/   # Dataset-specific scripts
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
