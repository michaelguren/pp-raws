# ETL Data Warehouse Architecture Guide

## Strategic Paradigms & Decisions

### Medallion Architecture
We implement a **Bronze → Silver → Gold** medallion architecture for all datasets:

- **Raw Layer**: S3 prefix `raw/{dataset}/run_id={run_id}/` - Original source data (zip, csv) with run-based organization
- **Bronze Layer**: S3 prefix `bronze/bronze_{dataset}/partition_datetime={timestamp}/` - Cleaned, typed parquet with metadata
- **Silver Layer**: S3 prefix `silver/silver_{dataset}/` - Business logic, SCD Type 2, deduplication  
- **Gold Layer**: (Future) Analytics-ready aggregations and marts

### Resource Naming Conventions
All resources follow consistent `pp-dw-{layer}-{dataset}` naming:

**Glue Jobs:**
- `pp-dw-bronze-{dataset}`
- `pp-dw-silver-{dataset}`

**Crawlers:**
- `pp-dw-bronze-{dataset}-crawler`
- `pp-dw-silver-{dataset}-crawler`

**Lambda Functions:**
- `pp-dw-raw-fetch-{dataset}`

**Databases:**
- `pp_dw_bronze` (shared across all datasets)
- `pp_dw_silver` (shared across all datasets)
- `pp_dw_gold` (future)

**Tables:**
- `{dataset}` (same name in both bronze and silver databases)

### Partitioning Strategy
- **Raw data**: Partitioned by `run_id` for lineage tracking
- **Bronze data**: Partitioned by `partition_datetime=YYYY-MM-DD-HHMMSS` for query performance
- **Silver data**: Partitioned by business-relevant dimensions (dates, categories, etc.)

### Data Quality & Lineage
**Bronze Layer Standards:**
- Clean column names (lowercase, underscores, no spaces)
- Proper data types (dates as DATE, not strings)
- Metadata columns: `meta_ingest_timestamp`, `meta_run_id`
- Compression: ZSTD for optimal storage/performance balance

**Silver Layer Standards:**
- **SCD Type 2 Implementation**: Full historical change tracking with:
  - `record_effective_date` / `record_end_date` for versioning
  - `is_current` boolean flag for active records  
  - `change_type` (INSERT/UPDATE/DELETE) for audit trails
  - `record_hash` MD5 fingerprint for change detection
  - Business key normalization and validation as needed
- Smart merge logic handles new, changed, and deleted records
- Partitioned by `effective_year_month` for query performance
- Complete audit trail for regulatory compliance

### Infrastructure Pattern
**Core + Dataset Stack Separation:**
- **EtlCoreStack**: Shared S3 bucket, Glue databases, IAM roles
- **Dataset Stacks**: Dataset-specific Lambda, Glue jobs, crawlers, Step Functions
- Clean separation of concerns with CloudFormation cross-stack references
- Core infrastructure deployed once, dataset stacks can deploy independently

### S3 Organization
```
s3://pp-dw-{account}/
├── raw/{dataset}/run_id={run_id}/                     # Original source files
├── bronze/bronze_{dataset}/partition_datetime={ts}/   # Cleaned parquet, datetime partitioned
├── silver/silver_{dataset}/                           # Business logic applied
└── gold/{dataset}/                                    # (Future) Analytics marts
```

**Note**: ETL configuration is now handled entirely at deployment time via CDK arguments - no config files are deployed to S3.

### Configuration Architecture
**Deployment-Time Configuration** - All configuration is now handled via CDK and passed as Glue job arguments:

**ETL-level config** (`./config.json`) - Read at CDK deployment time:
- Warehouse naming conventions and prefixes
- Shared database names (`pp_dw_bronze`, `pp_dw_silver`, `pp_dw_gold`)
- S3 path patterns for each layer
- Worker configurations by data size (small, medium, large, xlarge)
- Default Glue settings (version, python, timeouts)

**Dataset-specific config** (`./fda-{dataset}/config.json`) - Read at CDK deployment time:
- Dataset name, source URL, description
- `data_size_category`: Determines Glue worker allocation
- Business key for SCD Type 2 tracking
- Dataset-specific settings (schedules, data quality rules)

**Glue Job Arguments** - All configuration passed via `defaultArguments`:
- Pre-computed S3 paths (no runtime string manipulation)
- Database names, crawler names, timeouts
- Spark settings, compression codecs, SCD constants
- Business keys and validation rules

**Benefits**:
- ✅ **Faster startup**: No S3 config reads during job execution
- ✅ **More reliable**: Eliminates NoSuchKey configuration errors
- ✅ **Explicit dependencies**: All configuration visible in CDK
- ✅ **Deployment-time validation**: Config errors caught at deploy time

### Adding New Datasets
To add a new dataset (e.g., `fda-rxnorm`):

1. **Create dataset config**: `./fda-rxnorm/config.json`
   - Set appropriate `data_size_category` based on expected data volume
2. **Add Lambda function**: `./fda-rxnorm/lambdas/fetch/app.py`
3. **Create Glue jobs**: `./fda-rxnorm/glue/{bronze,silver}_job.py`
   - Copy from existing jobs - they're now pure data processing (no config loading)
4. **Create dataset stack**: `./fda-rxnorm/FdaRxnormStack.js`
   - Copy from FdaNsdeStack pattern
   - All configuration automatically passed as Glue job arguments
5. **Update index.js**: Add new stack instantiation with dependency on EtlCoreStack
6. **Deploy**: `cdk deploy --all` creates all resources with consistent naming

**Note**: Glue jobs are now standardized and require minimal customization - configuration is handled entirely by CDK.

### Key Decisions & Rationale

**Why Datetime Partitioning?**
- Enables multiple runs per day without conflicts
- Query performance for time-based analysis
- Natural partition pruning for operational queries

**Why Core + Dataset Stack Pattern?**
- Shared infrastructure deployed once, reused across datasets
- Dataset stacks can be deployed independently 
- Clear separation of shared vs dataset-specific concerns
- Scales cleanly as new datasets are added

**Why Bronze-First Approach?**
- Raw data preserved immutably for reprocessability
- Bronze handles technical transformations (typing, cleaning)
- Silver handles business transformations (SCD, validation)
- Clear separation of concerns

**Why Prefix-Based Organization?**
- Single bucket simplifies permissions
- Clear data lifecycle visibility
- Cost-effective storage management
- Scales naturally with new datasets

### Data Processing Flow
```
Step Function (auto-generates run_id) → Fetch Lambda → S3 Raw → Bronze Job → S3 Bronze → 
Silver Job → S3 Silver → Athena Tables → Analytics
```

- **No manual input required**: Step Function auto-generates `run_id` timestamp
- **Each step maintains lineage** through `meta_run_id` for full traceability
- **Worker sizing is automatic** based on dataset's `data_size_category`

### Autonomous Table Creation & Partition Management

**Smart Crawler Integration**: Both Bronze and Silver jobs implement intelligent table management:

1. **Table Existence Check**: Jobs first attempt to get table metadata from Glue catalog
2. **Automatic Crawler Trigger**: If table doesn't exist (first run), job automatically:
   - Starts the appropriate crawler (`pp-dw-bronze-{dataset}-crawler` or `pp-dw-silver-{dataset}-crawler`)
   - Waits for crawler completion (max 10 minutes)
   - Retrieves newly created table metadata
3. **Direct Partition Registration**: Once table exists, jobs register new partitions directly via API

**Benefits:**
- ✅ **Fully Autonomous**: No manual intervention required on first run
- ✅ **Self-Healing**: Handles table creation automatically
- ✅ **Efficient**: Crawlers only run when needed (once per dataset)
- ✅ **Immediate Availability**: Athena can query new data immediately after job completion
- ✅ **Robust Error Handling**: Graceful fallbacks if crawler fails

**Crawler Role**: 
- Bronze/Silver crawlers exist for **initial table creation only**
- Never triggered by Step Functions or schedules
- Only invoked automatically by jobs when tables don't exist
- After first run, all partition management is direct API calls

### Performance Optimization
**Worker Configuration by Data Size (AWS Best Practices):**
- **Small** (<100MB): G.1X with 2 workers - Minimum viable configuration
- **Medium** (100MB-1GB): G.1X with 5 workers - Optimal for 5-10 DPU recommendation
- **Large** (1GB-5GB): G.1X with 10 workers - Horizontal scaling for parallelism
- **XLarge** (>5GB): G.2X with 10 workers - Vertical + horizontal scaling for memory-intensive workloads

### Future Considerations
- **Cost optimization**: Lifecycle rules for old raw data
- **Security**: Least-privilege IAM, encryption at rest/transit
- **Monitoring**: CloudWatch metrics and alerts for job failures
- **Real-time**: Consider Kinesis/Lambda for streaming data sources