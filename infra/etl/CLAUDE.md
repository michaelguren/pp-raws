# ETL Data Warehouse Architecture Guide

## Strategic Paradigms & Decisions

### Simplified Data Architecture
We implement a **Raw → Bronze** approach for initial datasets:

- **Raw Layer**: S3 prefix `raw/{dataset}/run_id={run_id}/` - Original source data (zip, csv) with run-based organization
- **Bronze Layer**: S3 prefix `bronze/bronze_{dataset}/` - Cleaned, typed parquet with basic transformations (date formatting, column naming)
- **Silver/Gold Layers**: (Future) Advanced transformations and analytics marts as needed

### Resource Naming Conventions
All resources follow consistent `pp-dw-{layer}-{dataset}` naming:

**Glue Jobs:**
- `pp-dw-bronze-{dataset}`

**Crawlers:**
- `pp-dw-bronze-{dataset}-crawler`


**Databases:**
- `pp_dw_bronze` (shared across all datasets)

**Tables:**
- `{dataset}` for single-table datasets (e.g., `fda_nsde`)
- `{dataset}_{table_type}` for multi-table datasets (e.g., `fda_cder_products`, `fda_cder_packages`, `rxnorm_rxnconso`, `rxnorm_rxnsat`)

### Data Storage Strategy
- **Raw data**: Partitioned by `run_id` for lineage tracking
- **Bronze data**: Simple kill-and-fill approach - full overwrite each run for simplicity

### Data Quality & Lineage
**Bronze Layer Standards:**
- Explicit column mappings from source to target names (no auto-cleaning)
- Proper data types (dates as DATE, not strings)
- Metadata columns: `meta_run_id` (human-readable timestamp format)
- Compression: ZSTD for optimal storage/performance balance
- Kill-and-fill approach: complete overwrite each run for maximum simplicity

### Infrastructure Pattern
**Core + Dataset Stack Separation:**
- **EtlCoreStack**: Shared S3 bucket, Glue database, shared IAM roles
- **Dataset Stacks**: Dataset-specific Glue job, crawler, script deployment
- Clean separation of concerns with direct stack references (not CloudFormation exports)
- Core infrastructure deployed once, dataset stacks can deploy independently
- **Glue Script Deployment**: Scripts automatically uploaded to S3 via CDK BucketDeployment during stack deployment

### IAM Role Strategy (Least Privilege)
**Shared Glue Role (Default):**
- Created in EtlCoreStack for basic ETL operations
- Permissions: S3 read/write, Glue catalog access, CloudWatch logs
- Used by most datasets (FDA NSDE, CDER, etc.)
- **No sensitive permissions** (no Secrets Manager, no external APIs)

**Custom Glue Roles (When Needed):**
- Created in individual dataset stacks for sensitive operations
- **Required for**: Secrets Manager access, external API calls, special permissions
- Example: RxNORM needs UMLS API key from Secrets Manager
- Inherits basic Glue permissions + specific sensitive permissions

**Implementation Pattern:**
```javascript
// ❌ BAD - Adding secrets access to shared role affects all datasets
const sharedRole = props.etlCoreStack.glueRole;
sharedRole.addToPolicy(secretsPolicy); // DON'T DO THIS

// ✅ GOOD - Create dataset-specific role for sensitive operations
const datasetGlueRole = new iam.Role(this, "DatasetGlueRole", {
  assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
  managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")],
});
dataWarehouseBucket.grantReadWrite(datasetGlueRole);
datasetGlueRole.addToPolicy(secretsPolicy); // Only this dataset gets secrets access
```

### S3 Organization
```
s3://pp-dw-{account}/
├── etl/{dataset}/glue/                                # Deployed Glue job scripts
├── raw/{dataset}/run_id={run_id}/                     # Original source files
├── bronze/{dataset}/                                  # Cleaned parquet, kill-and-fill
└── silver/{dataset}/                                  # (Future) Business logic and analytics
```

**Note**: ETL configuration is now handled entirely at deployment time via CDK arguments - no config files are deployed to S3. Glue job scripts are automatically deployed to `etl/{dataset}/glue/` during CDK deployment.

### Configuration Architecture
**Deployment-Time Configuration** - All configuration is now handled via CDK and passed as Glue job arguments:

**ETL-level config** (`./config.json`) - Read at CDK deployment time:
- Warehouse naming conventions and prefixes
- Shared database name (`pp_dw_bronze`)
- S3 path patterns for raw and bronze layers
- Worker configurations by data size (small, medium, large, xlarge)
- Default Glue settings (version, python, timeouts)

**Dataset-specific config** (`./fda-{dataset}/config.json`) - Read at CDK deployment time:
- Dataset name, source URL, description
- `data_size_category`: Determines Glue worker allocation
- Dataset-specific settings (schedules, data quality rules)

**Glue Job Arguments** - All configuration passed via `defaultArguments`:
- Pre-computed S3 paths (no runtime string manipulation)
- Database names, crawler names, timeouts
- Spark settings, compression codecs
- Data transformation rules (date formatting, explicit column mappings)

**Benefits**:
- ✅ **Faster startup**: No S3 config reads during job execution
- ✅ **More reliable**: Eliminates NoSuchKey configuration errors
- ✅ **Explicit dependencies**: All configuration visible in CDK
- ✅ **Deployment-time validation**: Config errors caught at deploy time

### Adding New Datasets
To add a new dataset (e.g., `fda-rxnorm`):

1. **Create dataset config**: `./fda-rxnorm/config.json`
   - Set appropriate `data_size_category` based on expected data volume
   - Include `source_url` for data download
2. **Create complete Glue job**: `./fda-rxnorm/glue/bronze_job.py`
   - Copy from existing jobs - handles download + transform in single job
   - All configuration automatically passed as Glue job arguments
3. **Create dataset stack**: `./fda-rxnorm/FdaRxnormStack.js`
   - **For basic datasets**: Use shared Glue role from `props.etlCoreStack.glueRole`
   - **For sensitive datasets**: Create custom role with additional permissions
   - Include BucketDeployment for automatic Glue script upload to S3
4. **Update index.js**: Add new stack instantiation with `etlCoreStack` reference and dependency
5. **Deploy**: `cdk deploy --all` creates all resources with consistent naming

**Role Decision Matrix:**
- **Use Shared Role**: Public data sources, no authentication, standard S3/Glue operations
- **Create Custom Role**: Secrets Manager access, external APIs, special permissions, authentication required

**Note**: Single Glue jobs handle the complete ETL pipeline - no separate download/transform steps needed.

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

**Why Raw-to-Bronze Approach?**
- Raw data preserved immutably for reprocessability
- Bronze handles essential transformations (typing, cleaning, date formatting)
- Additional layers added as business needs evolve
- Simple starting point that can be enhanced

**Why Prefix-Based Organization?**
- Single bucket simplifies permissions
- Clear data lifecycle visibility
- Cost-effective storage management
- Scales naturally with new datasets

### Data Processing Flow
```
EventBridge → Glue Job (download + transform) → S3 Raw + Bronze → 
Crawler → Athena Tables → Analytics
```

The single Glue job handles:
1. **Download**: Fetch source data from URL with timeout handling
2. **Raw Storage**: Save original files to S3 for lineage and debugging  
3. **Transform**: Map columns explicitly, format dates, add metadata
4. **Bronze Storage**: Write parquet files (kill-and-fill approach)
5. **Schema Update**: Trigger crawler for Athena table updates

- **Complete autonomy**: One job does everything from download to Athena-ready data
- **Clear lineage**: Raw files preserved with run_id for full traceability
- **Worker sizing is automatic** based on dataset's `data_size_category`

### Critical Implementation Patterns

**✅ ALWAYS Follow These Patterns:**

**1. Memory-Efficient File Processing:**
- Use temporary files with `tempfile.NamedTemporaryFile()` for downloads
- Stream directly to S3 with `s3_client.upload_fileobj()` - never load entire files into memory
- Download once, upload to S3, then extract from temp file (avoid double downloads)
- Include retry logic with exponential backoff for network failures
- Stream extracted files directly to S3 using `zipfile` and `upload_fileobj()`

**2. Glue Job Structure:**
- Always include `'JOB_NAME'` in `getResolvedOptions()` - critical for job initialization
- Use `SparkContext()` not `SparkContext.getOrCreate()`
- Configure Spark compression immediately after initialization
- Read data from S3 paths with Spark - never from local filesystem

**3. Data Flow Philosophy:**
- **Raw Layer**: Preserve original files exactly as downloaded for lineage
- **S3-First**: All data processing happens via S3, never local files
- **Streaming**: Handle files of any size without memory constraints
- **Retry Resilience**: Network operations must handle transient failures

**❌ NEVER Do These (Common Mistakes):**
- ❌ Load entire files into memory with `response.read()`
- ❌ Read from local filesystem with `file://` paths
- ❌ Omit retry logic for network operations
- ❌ Try to create Glue catalog tables in jobs (let crawlers handle schema)
- ❌ Download the same file multiple times
- ❌ Leave undefined variable references when refactoring (always search for old variable names)

### Code Refactoring Checklist

When updating jobs to streaming approach:

1. **Before making changes**: Search entire file for `zip_data`, `csv_data`, `response.read()`
2. **Replace download logic**: Use temporary file + `shutil.copyfileobj()`
3. **Replace upload logic**: Use `s3_client.upload_fileobj()` instead of `put_object(Body=data)`
4. **Update size reporting**: Replace `len(zip_data)` with `content_length or 'unknown'`
5. **Test deployment**: Always deploy and test after refactoring
6. **Final verification**: Search again for old variable names to ensure none remain

### Autonomous Table Management

**Kill-and-Fill Approach**: Bronze jobs implement simple table management:

1. **Data Overwrite**: Each run completely overwrites the bronze table data
2. **Schema Updates**: Crawler runs after each job to update table schema automatically
3. **No Partition Management**: Simple single-table structure without partitions

**Benefits:**
- ✅ **Maximum Simplicity**: No complex partition or merge logic
- ✅ **Always Fresh**: Complete dataset refresh every run
- ✅ **Schema Evolution**: Crawler automatically detects schema changes
- ✅ **Easy Debugging**: Clear data lineage - one run = one complete dataset
- ✅ **Fast Development**: Perfect for testing and iteration

**Crawler Role**: 
- Bronze crawlers run after **every job execution**
- Automatically update table schema based on latest parquet files
- Handle column additions, type changes, and schema evolution
- Ensure Athena queries work immediately after job completion

### Performance Optimization
**Worker Configuration by Data Size (AWS Best Practices):**
- **Small** (<100MB): G.1X with 2 workers - Minimum viable configuration
- **Medium** (100MB-1GB): G.1X with 5 workers - Optimal for 5-10 DPU recommendation
- **Large** (1GB-5GB): G.1X with 10 workers - Horizontal scaling for parallelism
- **XLarge** (>5GB): G.2X with 10 workers - Vertical + horizontal scaling for memory-intensive workloads

### CDK Development Guidelines

**IMPORTANT: No Console Logging During CDK Build**
- **NEVER** use `console.log()` statements in CDK stack constructors
- Console output during `cdk deploy` creates noise and clutters deployment logs
- Use CDK outputs (`CfnOutput`) for important information instead
- Debug information should be in comments or removed before commit

**Clean Deployment Output:**
```javascript
// ❌ BAD - Creates noise during deployment
console.log(`Creating stack for ${dataset}`);
console.log(`Worker config: ${JSON.stringify(config)}`);

// ✅ GOOD - Use CfnOutput for important info
new cdk.CfnOutput(this, "DatasetName", {
  value: dataset,
  description: "Dataset being processed"
});
```

### Future Considerations
- **Cost optimization**: Lifecycle rules for old raw data
- **Security**: Least-privilege IAM, encryption at rest/transit
- **Monitoring**: CloudWatch metrics and alerts for job failures
- **Real-time**: Consider Kinesis/Lambda for streaming data sources