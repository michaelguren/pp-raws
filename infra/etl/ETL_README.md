# RAWS ETL Design Patterns

ETL pipelines for Pocket Pharmacist following Databricks Medallion architecture.

## Project Structure (v0.1)

**Layer-first scalable structure with co-located scripts:**

```
infra/etl/
├── bronze/nsde/           # Minimal ingestion (✅ v0.1)
│   ├── stack.js
│   └── nsde-bronze-etl.py
└── silver/nsde/           # Cleansing, DQ checks (✅ v0.1)
    ├── stack.js
    └── nsde-silver-etl.py
```

**Design principles:**
- **Co-location**: Each dataset's stack and script live together for clarity
- **Layer-first**: Bronze and Silver layers are top-level organizational units
- **Simplicity**: Flat structure with minimal nesting for v0.1

**Future:** `bronze/cder/`, `silver/cder/` (v0.2), `silver/fda_ndc_combined/` (v0.3)

## Medallion Architecture

- **Raw**: Unedited FDA source files in `s3://.../raw/nsde/{YYYY-MM-DD}/`
- **Bronze**: Minimal ingestion—download, convert to Parquet+ZSTD, basic metadata, partition by version_date
- **Silver**: Full cleansing, NDC normalization, DQ checks/tripwires, partition by version + version_date
- **Gold**: Business aggregations (future)

**Trust boundary**: Silver layer contains validated, clean data with full lineage.

**Data History Paradigm:**
- **Raw**: Immutable source snapshots by date
- **Bronze**: Append-mode preserves ingestion history 
- **Silver**: Append-mode preserves transformation history
- **Lineage**: SHA256 checksums track data from source through all layers

## Workflow Chaining (v0.1)

Uses AWS Glue Workflows for conditional Bronze → Silver execution:

- **Workflow**: `pp-raws-nsde-etl-workflow` 
- **Trigger**: Bronze starts 3 AM UTC daily
- **Conditional**: Silver only runs if Bronze succeeds
- **Safety**: Silver validates Bronze manifest exists before processing

## S3 Structure

```
s3://pp-raws-data-lake-{account}-{region}/
├── raw/nsde/{YYYY-MM-DD}/              # FDA source CSVs
├── bronze/nsde/data/version_date={date}/ # Parquet, basic metadata
└── silver/nsde/data/version={date}/version_date={date}/ # Clean, validated
```

## Stack Architecture

### DataLakeStack (Shared Infrastructure)
Provides shared resources for all datasets:
- S3 buckets (data lake + Glue scripts)
- IAM role for Glue jobs
- Glue database (`pp_raws_data_lake`)

### Dataset Stacks (NSDE-specific)
**Bronze Stack** contains:
- Bronze tables/crawler
- Workflow and scheduled start trigger
- Bronze ETL job

**Silver Stack** contains:
- Silver tables/crawler  
- Conditional trigger (runs when Bronze succeeds)
- Silver ETL job

### Resources by Stack
- **Shared**: `pp_raws_data_lake` database, S3 buckets, IAM role
- **Bronze**: `bronze_nsde*` tables, Bronze crawler, workflow, Bronze job
- **Silver**: `silver_nsde*` tables, Silver crawler, conditional trigger, Silver job

## ETL Patterns

### Bronze (Steps 0-2) - Minimal Ingestion
0. **Idempotency check** (SHA256 checksums)
1. **Download/extract** to raw layer
2. **Convert to Parquet** with basic metadata, ZSTD compression

**Key Bronze paradigms:**
- **Append mode** for data writes (preserves history on re-runs)
- **Infer schema** from CSV (no fixed schema)
- **Basic metadata only** (ingest timestamp, source SHA256, URLs)
- **Version_date partitioning** for time-based queries

### Silver (Steps 3-4) - Full Cleansing & DQ
3. **Transform/cleanse** (NDC normalization, flexible date parsing, text trimming)
4. **DQ checks/tripwires** (fail on null NDCs, log business field nulls)

**Key Silver paradigms:**
- **Append mode** for data writes (preserves versioned history - aligned with Bronze)
- **Caching** Bronze data for multiple transformations
- **Flexible date parsing** (YYYYMMDD primary, yyyy-MM-dd fallback)
- **NDC normalization** to 11-digit format with validation
- **Duplicate logging** (preserve all, log samples for debugging)
- **Bronze SHA256 traceability** in metadata
- **Version + version_date partitioning** for granular queries

## Testing Commands

```bash
# Run entire workflow (recommended for production)
aws glue start-workflow-run --name pp-raws-nsde-etl-workflow

# Monitor workflow status
aws glue get-workflow-runs --name pp-raws-nsde-etl-workflow --max-results 5

# Manual job testing (with force flag for Bronze re-runs)
aws glue start-job-run --job-name pp-raws-nsde-bronze-etl --arguments '{"--force":"true"}'
aws glue start-job-run --job-name pp-raws-nsde-silver-etl

# Run crawlers (required after first deployment)
aws glue start-crawler --name pp-raws-nsde-bronze-crawler
aws glue start-crawler --name pp-raws-nsde-silver-crawler

# Monitor job execution with insights
aws glue get-job-runs --job-name pp-raws-nsde-bronze-etl --max-results 3
aws glue get-job-runs --job-name pp-raws-nsde-silver-etl --max-results 3

# View job logs (CloudWatch)
aws logs describe-log-groups --log-group-name-prefix "/aws-glue/jobs/"
```

## Data Quality (v0.1)

**Built-in DQ Tripwires:**
- **Critical failures** (null NDCs, date parse errors) → Job fails
- **Business warnings** (null proprietary names, dosage forms) → Log only
- **Malformed NDC filtering** → Remove invalid formats

Monitoring relies on job logs and CloudWatch metrics. Advanced SQL queries available in script comments.

## Adding New Datasets

1. **Create directories**: `bronze/{dataset}/` and `silver/{dataset}/`
2. **Add files to each directory**:
   - `stack.js` - CDK infrastructure stack
   - `{dataset}-bronze-etl.py` or `{dataset}-silver-etl.py` - Glue ETL script
3. **Configure stacks**:
   - Bronze: Tables, crawler, workflow, scheduled trigger, ETL job
   - Silver: Tables, crawler, conditional trigger, ETL job
4. **Update `index.js`**: Add stacks with proper dependencies
5. **Reference shared resources**: Use DataLakeStack CloudFormation exports

**ETL Development Paradigms (v0.1):**
- **Append-first approach** for both Bronze and Silver (preserves history)
- **SHA256 lineage** tracking through all layers
- **Explicit CDK dependencies** for proper resource creation order

## Quick Start

```bash
# 1. Deploy infrastructure
cdk deploy PP-RAWS-DataLake PP-RAWS-NSDE-Bronze-ETL PP-RAWS-NSDE-Silver-ETL

# 2. Run initial crawlers (creates tables/partitions)
aws glue start-crawler --name pp-raws-nsde-bronze-crawler
aws glue start-crawler --name pp-raws-nsde-silver-crawler

# 3. Run the workflow
aws glue start-workflow-run --name pp-raws-nsde-etl-workflow

# 4. Monitor progress
aws glue get-job-runs --job-name pp-raws-nsde-bronze-etl --max-results 1
aws glue get-job-runs --job-name pp-raws-nsde-silver-etl --max-results 1

# 5. Query results in Athena
# SELECT * FROM pp_raws_data_lake.silver_nsde LIMIT 10;
```

## Deployment Notes

**Infrastructure Dependencies:**
- DataLakeStack → Bronze Stack → Silver Stack
- CloudFormation exports for cross-stack resource sharing

**Job Configuration:**
- **Timeouts**: 30min each (handles current NSDE volumes)
- **Workers**: 2x G.1X (cost-effective for v0.1)
- **Compression**: ZSTD throughout

## Future Roadmap

- **v0.2+**: Additional FDA datasets (CDER), combined Silver layers
- **Later**: Gold layer business aggregations