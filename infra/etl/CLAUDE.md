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
├── datasets/{dataset}/
│   ├── config.json                   # Dataset config (tables, schema, schedule)
│   ├── {Dataset}Stack.js             # Stack definition
│   └── glue/*.py                     # Custom gold layer jobs (if needed)
└── shared/
    ├── deploytime/
    │   ├── factory.js                # CDK factory for bronze stacks
    │   └── index.js                  # Shared deployment utilities
    └── runtime/https_zip/
        ├── bronze_http_job.py        # Shared bronze job for HTTP/ZIP sources
        └── etl_runtime_utils.py      # Runtime utilities (packaged via --extra-py-files)
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
- Glue Jobs: `pp-dw-bronze-fda-nsde`, `pp-dw-silver-fda-all-ndc`
- Crawlers: `pp-dw-bronze-fda-products-crawler`
- Databases: `pp_dw_bronze`, `pp_dw_silver` (underscores, not hyphens)
- Glue Tables: Hyphens in `file_table_mapping` → underscores (`fda-products` → `fda_products`)

**S3 Paths**:
```
s3://pp-dw-{account}/
├── raw/{dataset}/run_id={timestamp}/
├── bronze/{dataset}/              # Single-table datasets
├── bronze/{dataset}/{table}/      # Multi-table datasets
├── silver/{dataset}/              # Silver layer tables
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
- Examples: `fda-nsde`, `fda-cder`, `fda-all-ndc`

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
- Example: `fda-all-ndc` joins `fda-nsde` + `fda-cder` tables

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

## Adding New Datasets

**Bronze - Pattern A (Factory-Based)**:
1. Create `datasets/{dataset}/config.json` with schema
2. Create `datasets/{dataset}/{Dataset}Stack.js` using factory pattern
3. Add to `index.js`

**Bronze - Pattern B (Custom)**:
1. Create `datasets/{dataset}/config.json` (optional, for metadata)
2. Create custom `datasets/{dataset}/{Dataset}Stack.js` with manual Glue job setup
3. Create custom `datasets/{dataset}/glue/bronze_job.py` with specialized logic
4. Add to `index.js`

**Silver** (always custom):
1. Create `datasets/{dataset}/config.json` with source dependencies
2. Create custom `{Dataset}Stack.js` (read source configs, pass table names)
3. Create `glue/silver_job.py` with transformation logic
4. Add to `index.js`
