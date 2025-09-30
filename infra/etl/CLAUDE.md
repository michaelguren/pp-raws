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
    ├── deploytime/factory.js         # CDK factory for bronze stacks
    └── runtime/https_zip/*.py        # Shared bronze job scripts
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
        "type": "string|date|integer|decimal|boolean",
        "format": "yyyyMMdd"        // dates only
      }
    }
  }
}
```

## Naming Conventions

**AWS Resources**: `{prefix}-{layer}-{dataset}[-{table}][-crawler]`
- Glue Jobs: `pp-dw-bronze-fda-nsde`, `pp-dw-gold-fda-all-ndc`
- Crawlers: `pp-dw-bronze-fda-products-crawler`
- Databases: `pp_dw_bronze`, `pp_dw_gold` (underscores, not hyphens)
- Glue Tables: Hyphens in `file_table_mapping` → underscores (`fda-products` → `fda_products`)

**S3 Paths**:
```
s3://pp-dw-{account}/
├── raw/{dataset}/run_id={timestamp}/
├── bronze/{dataset}/              # Single-table datasets
├── bronze/{dataset}/{table}/      # Multi-table datasets
├── gold/{dataset}/                # Gold layer tables
└── etl/datasets/{dataset}/glue/   # Dataset-specific scripts
```

## Stack Types

**Bronze Stacks** (use factory pattern):
```javascript
const EtlStackFactory = require("../../shared/deploytime/factory");
const datasetConfig = require("./config.json");
const factory = new EtlStackFactory(this, props);

factory.createDatasetInfrastructure({
  datasetConfig,
  options: { skipGoldJob: true }  // Bronze only
});
```

**Gold Stacks** (custom, no factory):
- Read source dataset configs at deploy time for table names
- Pass table names as Glue job arguments
- Custom transformation logic in `glue/*.py`
- Example: `fda-all-ndc` joins `fda-nsde` + `fda-cder` tables

## Data Flow

**Bronze Layer** (automated via factory + shared job):
1. Download ZIP from `source_url` → Extract to `s3://.../raw/{dataset}/`
2. Apply `column_schema` transformations (rename, type cast, format dates)
3. Add `meta_run_id` metadata column
4. Write to `s3://.../bronze/{dataset}/[{table}/]` as Parquet/ZSTD (kill-and-fill)
5. Crawler updates Glue catalog

**Gold Layer** (custom transformations):
- Read multiple bronze tables from Glue catalog
- Join/transform/aggregate as needed
- Write to `s3://.../gold/{dataset}/` as Parquet/ZSTD
- Crawler updates Glue catalog

## Adding New Datasets

**Bronze**:
1. Create `datasets/{dataset}/config.json` with schema
2. Create `datasets/{dataset}/{Dataset}Stack.js` using factory pattern
3. Add to `index.js`

**Gold**:
1. Create `datasets/{dataset}/config.json` with source dependencies
2. Create custom `{Dataset}Stack.js` (read source configs, pass table names)
3. Create `glue/gold_job.py` with transformation logic
4. Add to `index.js`
