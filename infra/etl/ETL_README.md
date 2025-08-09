# RAWS ETL Design Patterns and Strategies

This document outlines the design patterns, conventions, and strategies used for ETL pipelines in the RAWS (Rails + AWS) framework for Pocket Pharmacist.

## Project Structure (v0.1)

The ETL codebase follows a scalable, dataset-oriented structure:

```
infra/
├── index.js                     # Main CDK app entry point
├── shared/
│   ├── data-lake-stack.js        # Shared infrastructure (S3, Glue DB, IAM)
│   └── lib/cdk/
│       ├── tables.js             # makeParquetTable() helper
│       └── crawler.js            # makeCrawler() helper
└── etl/
    └── datasets/
        └── nsde/                 # NSDE (FDA drug data) v0.1
            └── bronze/           # Bronze layer processing
                ├── stack.js      # NSDE bronze CDK stack
                └── glue-scripts/
                    └── nsde-bronze-etl.py  # ETL job script
```

**Scalability**: Adding new datasets (RxNorm, SNOMED) or layers (silver, gold) follows the same pattern: `etl/datasets/<dataset>/<layer>/`

## Architecture Overview

### Medallion Architecture
We implement a **medallion data architecture** for FDA drug data processing:

- **Raw Layer** (`s3://bucket/raw/`): Unprocessed data exactly as received from FDA sources
- **Bronze Layer** (`s3://bucket/bronze/`): Cleaned, validated, and normalized data with standardized NDC11 format
- **Silver Layer** (`s3://bucket/silver/`): **FDA NDC combined** - Inner join of NSDE + CDER datasets on normalized NDC11
- **Gold Layer** (`s3://bucket/gold/`): Aggregated, business-ready pharmaceutical data tables

### FDA Dataset Pipeline (v0.1 - v0.3 Roadmap)

```
┌─────┐    ┌────────┐    ┌──────────────────┐    ┌──────────────┐
│ RAW │───▶│ BRONZE │───▶│      SILVER      │───▶│     GOLD     │
└─────┘    └────────┘    └──────────────────┘    └──────────────┘
             ▲                      ▲                     ▲
    FDA      │              FDA NDC combined             Business
  Sources    │             NSDE + CDER                   Analytics
             │            (inner join)
        ┌─────────┐
        │  NSDE   │  ✅ v0.1 COMPLETE
        │  CDER   │  📅 v0.2 planned
        └─────────┘
```

**Current Status**: v0.1 has NSDE bronze layer complete. v0.2 will add CDER, v0.3 will implement the silver layer inner join.

### Shared Infrastructure Pattern
All ETL jobs share common infrastructure to reduce duplication and costs:

- **Shared S3 Data Lake**: Single bucket with dataset prefixes
- **Shared Glue Database**: `pp_raws_data_lake` for all datasets
- **Shared IAM Role**: `pp-raws-shared-glue-role` for all Glue jobs
- **Dataset-specific Crawlers**: `pp-raws-nsde-crawler` for NSDE (v0.1), `pp-raws-cder-crawler` (v0.2)

## Naming Conventions

### Stack Names
- Pattern: `PP-RAWS-[Component]-[Dataset]`
- Examples: `PP-RAWS-DataLake`, `PP-RAWS-NSDE-ETL`, `PP-RAWS-CDER-ETL`, `PP-RAWS-RxNorm-ETL`

### Resource Names
- **S3 Buckets**: `pp-raws-[purpose]-[account]-[region]`
  - `pp-raws-data-lake-550398958311-us-east-1`
  - `pp-raws-glue-scripts-550398958311-us-east-1`
- **Glue Jobs**: `pp-raws-[dataset]-etl`
  - `pp-raws-nsde-etl` ✅ v0.1
  - `pp-raws-cder-etl` (v0.2)
  - `pp-raws-rxnorm-etl` (future)
- **Glue Database**: `pp_raws_data_lake`
- **Tables**: `[layer]_[dataset]` and `[layer]_[dataset]_metadata`
  - `bronze_nsde` (partitioned) ✅ v0.1
  - `bronze_nsde_metadata` (unpartitioned) ✅ v0.1
  - `bronze_cder` (v0.2)
  - `bronze_cder_metadata` (v0.2)
  - `silver_fda_ndc_combined` (v0.3)
  - `bronze_rxnorm` (future)

### S3 Path Structure
```
s3://pp-raws-data-lake-{account}-{region}/
├── raw/                          # Unprocessed FDA source data
│   ├── nsde/
│   │   └── {YYYY-MM-DD}/
│   │       └── Comprehensive_NDC_SPL_Data_Elements_File.csv
│   ├── cder/                     # v0.2
│   │   └── {YYYY-MM-DD}/
│   │       └── cder_source.zip
│   └── rxnorm/                   # Future
│       └── {YYYY-MM-DD}/
│           └── rxnorm_source.zip
├── bronze/                       # Cleaned, normalized FDA data
│   ├── nsde/                     # ✅ v0.1 COMPLETE
│   │   ├── data/
│   │   │   └── version={YYYY-MM-DD}/version_date={YYYY-MM-DD}/
│   │   │       └── *.zstd.parquet
│   │   └── metadata/
│   │       ├── data/
│   │       │   └── *.zstd.parquet
│   │       └── summary_version={YYYY-MM-DD}/
│   │           └── _SUCCESS.json
│   ├── cder/                     # v0.2 planned
│   │   ├── data/
│   │   └── metadata/
│   └── rxnorm/                   # Future
│       ├── data/
│       └── metadata/
├── silver/                       # v0.3 - Combined FDA datasets
│   └── fda_ndc_combined/
│       └── data/
│           └── version={YYYY-MM-DD}/
│               └── *.zstd.parquet
└── gold/                         # Business-ready pharmaceutical data
    └── (future aggregations)
```

## ETL Job Patterns

### Standard Glue Job Structure
All ETL jobs follow this pattern:

0. **Idempotency Check** - Read existing manifest, compare SHA256 of source
1. **Download & Extract** - Retrieve source data to raw layer  
2. **Transform & Cleanse** - Normalize, validate, and standardize
3. **Data Quality Checks** - Apply DQ tripwires and monitoring
4. **Write Bronze Data** - Store as Parquet with ZSTD compression
5. **Write Metadata** - Record ETL run statistics (unpartitioned table)
6. **Write SUCCESS Manifest** - Create idempotency marker with SHA256
7. **Error Handling** - Log errors and fail gracefully

### Data Quality Standards
Every ETL job must implement:

**DQ Tripwires** (fail job if triggered):
- **Null checks** on critical fields (e.g., normalized_ndc11 > 0 nulls)
- **Date parse errors** during transformation
- **Source data integrity** (e.g., empty ZIP files, missing CSV)

**DQ Monitoring** (log but don't fail):
- **Malformed data filtering** (e.g., NDCs with letters/special chars are logged and filtered out)
- **Duplicate detection** for informational purposes
- **Record count verification** against historical baselines
- **Schema consistency checks** across runs

### Partitioning Strategy
- **Bronze data tables**: Partition by `version` and `version_date` (YYYY-MM-DD format) with **partition projection enabled**
- **Metadata tables**: Unpartitioned to avoid tiny partition proliferation
- **Idempotency manifests**: Stored in separate S3 prefix with version partitioning
- **Partition projection**: Auto-discovers partitions from S3 paths, eliminates manual repair steps
- Avoid deep partitioning (year/month/day) that creates bad table names

## Metadata Pattern

### ETL Observability
Every ETL run creates metadata records with:
```python
metadata = {
    "dataset": "nsde",
    "version": "2025-08-09",
    "version_date": "2025-08-09",
    "row_count": 634700,
    "unique_ndc_codes": 634700,
    "null_ndc_count": 0,
    "duplicate_ndc_count": 0,
    "malformed_ndc_count": 114,
    "date_parse_errors": 0,
    "processed_at_utc": "2025-08-09T03:17:44.804216",
    "source_url": "https://...",
    "sha256": "abc123...",
    "job_name": "pp-raws-nsde-etl"
}
```

### Metadata Queries
Monitor ETL health with:
```sql
-- Recent ETL runs for NSDE
SELECT version, row_count, processed_at_utc, sha256 
FROM pp_raws_data_lake.bronze_nsde_metadata 
ORDER BY processed_at_utc DESC;

-- Data quality trends for NSDE
SELECT version, null_ndc_count, duplicate_ndc_count, malformed_ndc_count
FROM pp_raws_data_lake.bronze_nsde_metadata 
ORDER BY version DESC;
```

## Deployment Patterns

### Stack Dependencies
1. Deploy `PP-RAWS-DataLake` first (shared resources)
2. Deploy dataset-specific stacks (`PP-RAWS-NSDE-ETL`, etc.)
3. Each dataset stack references shared resources via CloudFormation exports

### CDK Cross-Stack References
```javascript
// Import shared resources
const dataLakeBucketName = Fn.importValue('PP-RAWS-DataLake-DataLakeBucket');
const sharedGlueRoleArn = Fn.importValue('PP-RAWS-DataLake-SharedGlueRoleArn');
```

### Glue Script Deployment
Scripts are deployed via CDK `BucketDeployment` to organize by dataset:
```
s3://pp-raws-glue-scripts-{account}-{region}/
├── nsde/
│   └── nsde-bronze-etl.py        # v0.1 bronze layer script
├── rxnorm/
│   └── rxnorm-bronze-etl.py      # Future
└── temp/
    └── (temporary files)
```

## Scheduling and Triggers

### Current Pattern (v0.1)
- **Manual triggers** for testing and validation
- **Daily schedules** disabled (`startOnCreation: false`)
- **Crawler runs** manually after ETL jobs using `pp-raws-nsde-crawler`
- **Idempotency** via `_SUCCESS.json` manifests with SHA256 checksums

### Production Pattern (Future)
- **ETL jobs**: Run at 3 AM UTC daily
- **Crawler**: Run at 4 AM UTC (after ETL jobs complete)
- **Monitoring**: CloudWatch alarms on job failures

## CDK Helper Usage

### Creating Tables
Use the `makeParquetTable()` helper for consistent table creation:
```javascript
const { makeParquetTable } = require('../../../../shared/lib/cdk/tables');

const bronzeTable = makeParquetTable(this, 'TableBronzeRxnorm', {
  dbName: glueDatabase.ref,
  s3Location: `s3://${dataLakeBucket.bucketName}/bronze/rxnorm/data/`,
  tableName: 'bronze_rxnorm',
  partitionKeys: [
    { name: 'version', type: 'string' },
    { name: 'version_date', type: 'date' }
  ],
  enableProjection: true  // Auto-discovers partitions, eliminates MSCK REPAIR
});
```

### Creating Crawlers
Use the `makeCrawler()` helper for consistent crawler setup:
```javascript
const { makeCrawler } = require('../../../../shared/lib/cdk/crawler');

const rxnormCrawler = makeCrawler(this, 'RxNormCrawler', {
  name: 'pp-raws-rxnorm-crawler',
  roleArn: sharedGlueRole.roleArn,
  dbName: glueDatabase.ref,
  s3Targets: [{
    path: `s3://${dataLakeBucket.bucketName}/bronze/rxnorm/data/`,
    exclusions: ["**/_SUCCESS*", "**/*.json", "**/.*/**", "**/_temporary/**", "**/*.crc"]
  }],
  configuration: {
    Version: 1,
    CrawlerOutput: { 
      Partitions: { AddOrUpdateBehavior: 'InheritFromTable' },
      Tables: { AddOrUpdateBehavior: 'MergeNewColumns' }
    },
    Grouping: { TableGroupingPolicy: 'CombineCompatibleSchemas' }
  }
});
```

## Data Transformation Standards

### NDC Normalization (NSDE Example)
```python
# Normalize NDC codes to 11-digit format without dashes
df = df.withColumn("normalized_ndc11",
    when(col("NDC11").isNotNull(), trim(col("NDC11")))
    .otherwise(
        concat(
            lpad(col("item_code_parts")[0], 5, "0"),  # Labeler: 5 digits
            lpad(col("item_code_parts")[1], 4, "0"),  # Product: 4 digits  
            lpad(col("item_code_parts")[2], 2, "0")   # Package: 2 digits
        )
    )
)
```

### Date Standardization
```python
# Parse FDA date format (YYYYMMDD) to proper dates
df = df.withColumn("marketing_start_date",
    to_date(col("Marketing Start Date"), "yyyyMMdd")
)
```

### Column Naming
- **Source columns**: Keep original names during initial read
- **Renamed columns**: Use snake_case for consistency
- **Added columns**: Standard metadata columns (`version`, `processing_date`, etc.)

## Testing and Validation

### Data Quality Validation
```python
# Standard checks in every ETL job
null_count = df.filter(col("primary_key").isNull()).count()
duplicate_count = df.groupBy("primary_key").count().filter(col("count") > 1).count()
```

### ETL Testing Commands
```bash
# Run ETL job with optional force flag
aws glue start-job-run --job-name pp-raws-nsde-etl
aws glue start-job-run --job-name pp-raws-nsde-etl --arguments='--force=true'

# Check job status and get run ID
aws glue get-job-runs --job-name pp-raws-nsde-etl --max-items 1 --query "JobRuns[0].{Id:Id,State:JobRunState,ExecutionTime:ExecutionTime}"

# Monitor job logs in real-time
aws logs get-log-events --log-group-name "/aws-glue/jobs/output" --log-stream-name "<JOB_RUN_ID>" --start-time $(date -d '5 minutes ago' +%s)000

# Verify data landed (check for ZSTD compression)
aws s3 ls s3://pp-raws-data-lake-{account}-{region}/bronze/nsde/data/version=2025-08-09/ --recursive

# Check idempotency manifest 
aws s3 cp s3://pp-raws-data-lake-{account}-{region}/bronze/nsde/metadata/summary_version=2025-08-09/_SUCCESS.json - | jq .

# Run crawler to catalog NSDE data
aws glue start-crawler --name pp-raws-nsde-crawler

# Check crawler status
aws glue get-crawler --name pp-raws-nsde-crawler --query "Crawler.{State:State,LastCrawl:LastCrawl.Status}"

# Partition projection eliminates the need for repair commands
# bronze_nsde table auto-discovers partitions - queries work immediately

# Query results in Athena
aws athena start-query-execution --query-string "SELECT COUNT(*) FROM pp_raws_data_lake.bronze_nsde WHERE version = '2025-08-09'" --result-configuration OutputLocation=s3://pp-raws-data-lake-{account}-{region}/athena-results/ --work-group primary

# Check ETL run metadata
aws athena start-query-execution --query-string "SELECT * FROM pp_raws_data_lake.bronze_nsde_metadata ORDER BY processed_at_utc DESC LIMIT 5" --result-configuration OutputLocation=s3://pp-raws-data-lake-{account}-{region}/athena-results/ --work-group primary
```

## Scalability Considerations

### Adding New Datasets

To add a new dataset (e.g., RxNorm):

1. **Create directory structure**:
   ```
   infra/etl/datasets/rxnorm/bronze/
   ├── stack.js                    # CDK stack for RxNorm bronze
   └── glue-scripts/
       └── rxnorm-bronze-etl.py    # ETL script
   ```

2. **Update shared resources** in `data-lake-stack.js`:
   - Add `bronze_rxnorm` table using `makeParquetTable()` helper
   - Add `bronze_rxnorm_metadata` table (unpartitioned)
   - Add `pp-raws-rxnorm-crawler` using `makeCrawler()` helper

3. **Reference shared infrastructure** via CloudFormation exports

4. **Add stack to** `index.js`:
   ```javascript
   const { RxNormEtlStack } = require('./etl/datasets/rxnorm/bronze/stack');
   ```

5. **Follow naming conventions**:
   - Stack: `PP-RAWS-RxNorm-ETL` 
   - Job: `pp-raws-rxnorm-etl`
   - Tables: `bronze_rxnorm`, `bronze_rxnorm_metadata`
   - Crawler: `pp-raws-rxnorm-crawler`

### Cost Optimization
- **Glue Jobs**: Use G.1X workers, 30-minute timeout
- **S3 Lifecycle**: Expire raw data after 30 days, transition bronze to IA
- **Partitioning**: Simple version-based partitioning reduces scan costs
- **Parquet Format**: Columnar storage with ZSTD compression

## Troubleshooting

### Common Issues
1. **Bad Table Names**: Pre-create tables via CDK, crawlers only update schema
2. **CloudFormation Updates**: Some resources (like database names) require deletion/recreation
3. **Partitioning**: Avoid complex partitioning that creates year/month folders
4. **Permissions**: Use shared Glue role to avoid IAM proliferation
5. ~~**Athena Partition Lag**: After crawler runs, Athena may not immediately see new partitions. Run `MSCK REPAIR TABLE` to sync the metastore.~~ **RESOLVED** - Partition projection eliminates this issue.

### Partition Management (v0.1 - RESOLVED)

**Previous Issue**: Athena queries returned 0 results immediately after crawler completion due to timing delays between Glue metastore updates and Athena's partition cache.

**Solution Implemented**: **Athena Partition Projection** (CDK-based)
- **bronze_nsde table** now uses partition projection via CDK `enableProjection: true`
- **Partitions auto-discovered** based on S3 path patterns
- **No more MSCK REPAIR TABLE** steps needed
- **Immediate query availability** after ETL runs

**Current Status**: All bronze tables with date-based partitioning (`version`, `version_date`) use partition projection by default. New ETL runs are queryable immediately in Athena without manual intervention.

### Monitoring
- **CloudWatch Logs**: `/aws-glue/jobs/output` log group
- **Job Metrics**: Monitor in Glue console for runtime/costs
- **Data Quality**: Query dataset-specific metadata tables for trends
- **S3 Costs**: Monitor storage costs by layer (raw vs bronze vs gold)
- **Partition Health**: Monitor partition count growth and query performance

## Future Enhancements

### Planned Features

**v0.2 - CDER Bronze Layer**
- **CDER ETL Pipeline**: Similar to NSDE pattern for FDA CDER data
- **Standardized NDC11**: Consistent normalization across FDA datasets
- **CDER Tables**: `bronze_cder` and `bronze_cder_metadata`

**v0.3 - Silver Layer (FDA NDC Combined)**
- **Inner Join Logic**: NSDE + CDER on `normalized_ndc11` field
- **Silver Tables**: `silver_fda_ndc_combined` with enriched FDA data
- **Data Quality**: Cross-dataset validation and reconciliation

**Future Expansions**
- **RxNorm Integration**: Add NIH RxNorm terminology data
- **SNOMED Support**: Clinical terminology standardization  
- **Gold Layer**: Business aggregations and reporting tables
- **Stream Processing**: Real-time data ingestion via Kinesis
- **Data Lineage**: Track data flow between layers
- **Automated Testing**: Schema validation and regression tests

### Governance
- **Schema Evolution**: Handle source data changes gracefully  
- **Data Retention**: Automatic cleanup policies by layer
- **Access Control**: Fine-grained permissions by dataset and layer
- **Audit Logging**: Track all data access and modifications