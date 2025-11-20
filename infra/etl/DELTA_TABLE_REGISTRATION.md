# Decision Record: Delta Table Registration via Glue Crawlers (Glue 5.0 + Athena v3)

---

## 🔥 CRITICAL INCIDENT REPORT: November 19, 2025

### Incident Summary

**Production Impact:** Complete loss of temporal history for ~80,000 FDA NDC records in Gold Delta Lake table.

**Root Cause:** `content_hash` calculation included volatile metadata columns (`run_date`, `run_id`, `source_run_id`), causing **all rows to appear CHANGED on every run**.

**Cascading Effect:**
1. Every record appeared as CHANGED (hash differed due to new run metadata)
2. All ~80K current records were expired (set `active_to = 2025-11-19`)
3. All ~80K records re-inserted as NEW (with `active_from = 2025-11-19`)
4. All temporal history prior to 2025-11-19 was destroyed
5. Historical tracking completely invalidated

**The Bug:**
```python
# ❌ ORIGINAL BROKEN CODE
business_columns = [
    "marketing_category", "product_type", "proprietary_name",
    "dosage_form", "application_number", "dea_schedule",
    "package_description", "active_numerator_strength",
    "active_ingredient_unit", "spl_id", "marketing_start_date",
    "marketing_end_date", "billing_unit", "nsde_flag"
]

# BUT: silver_df had these columns ADDED by the job:
# - run_date (VOLATILE - changes every run)
# - run_id (VOLATILE - changes every run)
# - source_run_id (VOLATILE - renamed from meta_run_id)
# - source_file (VOLATILE - metadata)

# content_hash = MD5(business_columns + ALL OTHER COLUMNS)  ← BUG WAS HERE
# Every run: new run_date/run_id → new hash → false CHANGED
```

**The Fix:**
```python
# ✅ PRODUCTION-HARDENED FIX

# 1. Explicit denylist constant in temporal_versioning_delta.py
VOLATILE_METADATA_COLUMNS = {
    "run_date", "run_id", "source_run_id", "meta_run_id",
    "source_file", "ingestion_timestamp",
    "version_id", "content_hash", "active_from", "active_to", "status",
    "created_at", "updated_at", "loaded_at", "processed_at"
}

# 2. Strict validator function
def business_columns_only(df: DataFrame, provided_business_columns: list) -> list:
    """Filter out volatile metadata, fail fast if empty"""
    filtered = [col for col in provided_business_columns
                if col not in VOLATILE_METADATA_COLUMNS]
    if not filtered:
        raise ValueError("All columns are volatile metadata!")
    return filtered

# 3. Automatic filtering in apply_temporal_versioning_delta()
validated_business_columns = business_columns_only(incoming_df, business_columns)
incoming_df = incoming_df.withColumn("content_hash",
                                    compute_content_hash_expr(validated_business_columns))
```

### Prevention Mechanisms Implemented

1. **Explicit Denylist Validator** (`business_columns_only()` in library)
   - Filters volatile columns using `VOLATILE_METADATA_COLUMNS` constant
   - Fails fast if all columns filtered out
   - Logs which columns removed and which used for hashing

2. **Production-Hardened Library** (`temporal_versioning_delta.py`)
   - All 5 RAWS Delta Invariants enforced
   - Orphan Parquet guard (`refuse_accidental_overwrite()`)
   - S3SingleDriverLogStore support
   - `.mode("append")` for initial loads

3. **Comprehensive Documentation**
   - November 19 incident documented in all affected files
   - RAWS Delta Invariants prominently displayed
   - Reference implementation: `infra/etl/datasets/gold/fda-all-ndcs/`
   - Business columns reference: `FDA_ALL_NDCS_BUSINESS_COLUMNS.md`

4. **Testing & Validation**
   - Production validation checklist in CLAUDE.md
   - Explicit logging of business columns used for hashing
   - Early validation errors instead of silent corruption

### Recovery Plan

**If this incident recurs:**

1. **Immediate:** Stop all Gold job runs
2. **Verify:** Check CloudWatch logs for `[VALIDATION]` messages showing volatile columns
3. **Rollback:** Restore Delta table from S3 backup (or time travel to last good version)
4. **Fix:** Update job's `business_columns` list to exclude volatile metadata
5. **Test:** Run job against copy of data, verify UNCHANGED count > 0
6. **Deploy:** Redeploy with fixes, monitor for false-change explosions

### Lessons Learned

**What Worked:**
- ✅ Delta Lake time travel enabled incident discovery
- ✅ Delta MERGE semantics preserved data (no data loss, just history corruption)
- ✅ Clear logging in temporal versioning library

**What Failed:**
- ❌ No validation that `business_columns` excludes metadata
- ❌ Pattern matching (`*date*`, `*meta*`) unreliable - explicit denylist required
- ❌ Insufficient documentation of what belongs in `business_columns`

**Architectural Improvement:**
- 🎯 **Data-plane vs Control-plane separation** now extends to column categorization
- 🎯 **Explicit over implicit** - denylist beats pattern matching
- 🎯 **Fail fast, fail loud** - validator errors better than silent corruption

### References

- **Root Cause Analysis:** This document, November 19, 2025 section
- **Fixed Library:** `infra/etl/shared/runtime/temporal/temporal_versioning_delta.py`
- **Fixed Job:** `infra/etl/datasets/gold/fda-all-ndcs/glue/gold_job.py`
- **RAWS Invariants:** See "RAWS Delta Lake Invariants" section below

---

## Context

GOLD-layer Delta Lake tables require proper registration in the Glue Data Catalog to be queryable via Athena. We've evolved through three approaches:

1. **Original Crawlers** (deprecated): Early crawlers registered Delta tables as `classification=parquet`, breaking Athena queries
2. **AwsCustomResource** (deprecated): Lambda-backed custom resource with boto3 Glue API calls - added procedural complexity and IAM overhead
3. **Glue Crawlers with Schema Change Policies** (current): Modern crawlers properly detect Delta Lake format with schema evolution support

## Decision

We use **Glue Crawlers with schema change policies** for GOLD Delta Lake table registration.

This represents the **simplest, most AWS-native approach** to Delta Lake with Glue 5.0 + Athena v3:

### The Canonical Stack
- **S3** → Raw data + Delta transaction log (`_delta_log/`)
- **Glue ETL 5.0** → Spark 3.5 runtime with Delta Lake 3.0 built-in (`--datalake-formats=delta`)
- **Glue Crawlers** → Automatic Delta table discovery and schema evolution (with Delta classifier)
- **Athena v3 (Trino)** → Native Delta SQL engine (no manifests needed)
- **Step Functions** → Orchestrates "run job → check tables → run crawler" sequence
- **CloudFormation / CDK** → End-to-end IaC

✅ 100% serverless, pay-per-query, fully AWS-native
✅ No Databricks, EMR, Lake Formation permissions, or custom Lambda code
✅ Glue's built-in Delta runtime + Athena's native reader + automatic schema discovery
✅ Zero job-level catalog management (separation of data-plane and control-plane)

Each GOLD dataset stack now:
1. **CDK defines infrastructure**: Glue job writes Delta Lake data to S3 (`/gold/<dataset>/`), Crawler defined with schema change policies
2. **Step Functions orchestrates**: Runs job → check-tables Lambda → start-crawlers Lambda → wait for completion
3. **Crawler registers table**: Discovers Delta format automatically, sets metadata for Athena v3:
   ```json
   {
     "classification": "delta",
     "table_type": "DELTA",
     "EXTERNAL": "TRUE"
   }
   ```
4. **Schema evolution**: Crawler uses `UPDATE_IN_DATABASE` / `DEPRECATE_IN_DATABASE` policies to handle column additions

---

## ⚠️ RAWS Delta Lake Invariants (Production-Hardened)

**Post-November 19, 2025 Incident:** These 5 invariants are **mandatory** for all Gold Delta implementations.

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

**Enforcement:** `business_columns_only()` validator in `temporal_versioning_delta.py`

**Rule:** Only stable business attributes from source data may be included in `content_hash`. Volatile metadata is automatically filtered using explicit `VOLATILE_METADATA_COLUMNS` denylist.

**Denylist:**
```python
VOLATILE_METADATA_COLUMNS = {
    "run_date", "run_id", "source_run_id", "meta_run_id", "source_file",
    "ingestion_timestamp", "version_id", "content_hash",
    "active_from", "active_to", "status",
    "created_at", "updated_at", "loaded_at", "processed_at"
}
```

**Violation Consequences:** False-change explosions (November 19, 2025 incident).

### Invariant 2: First Write Must Be Append

**Enforcement:** `apply_temporal_versioning_delta()` uses `.mode("append")` for initial loads

**AWS/Delta.io Best Practice:**
- ✅ `.mode("append")` - Correct for first Delta write on S3
- ❌ `.mode("overwrite")` - Can cause orphan Parquet and flaky `_delta_log` commits

**Reference:** https://delta.io/blog/delta-lake-s3/ (Section: "Avoiding orphan files")

### Invariant 3: S3SingleDriverLogStore Configuration

**Enforcement:** Required `--conf` argument in CDK stack `defaultArguments`

**CDK Implementation:**
```javascript
'--conf': 'spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore'
```

**Why Required:**
- Prevents transaction log conflicts in S3 eventual consistency model
- Avoids race conditions during concurrent writes
- Ensures consistent `_delta_log/` state
- AWS Glue best practice for production Delta Lake workloads

**Reference:** https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html

### Invariant 4: Schema Evolution Always Enabled

**Enforcement:** `temporal_versioning_delta.py` sets both:
1. `spark.databricks.delta.schema.autoMerge.enabled=true` (for MERGE operations)
2. `.option("mergeSchema", "true")` (for write operations)

**Job-Level Contract:**
- All Delta writes use `.option("mergeSchema", "true")`
- MERGE operations use `spark.databricks.delta.schema.autoMerge.enabled=true`
- Automatic - no job code changes when adding columns

**Infrastructure-Level Contract (CDK):**
- Glue Crawlers use `schemaChangePolicy.updateBehavior: UPDATE_IN_DATABASE`
- Glue Crawlers use `schemaChangePolicy.deleteBehavior: DEPRECATE_IN_DATABASE`
- Crawlers automatically sync new columns to Glue catalog

**Outcome:** Adding columns becomes a non-event:
- Developer adds column to Silver layer
- Gold job writes it (mergeSchema handles schema mismatch)
- Crawler syncs it to catalog (UPDATE_IN_DATABASE)
- Athena sees it immediately
- Historical records show NULL for new column (correct SCD2 behavior)

**Testing:** See `DELTA_TEST_PLAN.md` Run 5 - validates end-to-end schema evolution.

### Invariant 5: Orphan Parquet Guard

**Enforcement:** `refuse_accidental_overwrite()` runs before any initial Delta write

**The Glue Foot-Gun:**
Orphan Parquet files can exist without `_delta_log/`, causing Delta to treat the location as empty and overwrite existing data.

**Protection Mechanism:**
```python
def refuse_accidental_overwrite(spark: SparkSession, gold_path: str):
    """Guard against orphan Parquet files"""
    orphan_files = spark.read.format("binaryFile").load(gold_path).limit(1).count()
    if orphan_files > 0 and not DeltaTable.isDeltaTable(spark, gold_path):
        raise RuntimeError(f"ORPHAN PARQUET DETECTED at {gold_path}")
```

**When It Runs:** Before initial load in `apply_temporal_versioning_delta()`, after detecting no Delta table exists.

**Manual Recovery:** If orphan files detected, operator must manually delete before job can run:
```bash
aws s3 rm s3://bucket/gold/dataset/ --recursive
```

### Production Validation Checklist

Before deploying new Gold Delta jobs, verify:

- [ ] business_columns contains ONLY stable source attributes
- [ ] No volatile metadata in business_columns (run_date, run_id, etc.)
- [ ] CDK has `--conf` for S3SingleDriverLogStore
- [ ] CDK has `--datalake-formats=delta`
- [ ] Initial write uses `.mode("append")`
- [ ] All writes have `.option("mergeSchema", "true")`
- [ ] `refuse_accidental_overwrite()` guard is called
- [ ] `DeltaTable.isDeltaTable()` used for first-time detection
- [ ] SparkConf configured BEFORE SparkContext creation
- [ ] Glue Crawler uses `deltaTargets` (not `s3Targets`)
- [ ] Crawler has schema change policies configured

### Official References

1. **AWS Glue Delta Lake Best Practices (2025):**
   https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html

2. **Delta Lake S3 Reliability Blog:**
   https://delta.io/blog/delta-lake-s3/

3. **RAWS Reference Implementation:**
   - Library: `infra/etl/shared/runtime/temporal/temporal_versioning_delta.py`
   - Job: `infra/etl/datasets/gold/fda-all-ndcs/glue/gold_job.py`
   - Stack: `infra/etl/datasets/gold/fda-all-ndcs/GoldFdaAllNdcsStack.js`
   - Business Columns: `infra/etl/datasets/gold/fda-all-ndcs/FDA_ALL_NDCS_BUSINESS_COLUMNS.md`

---

## Consequences

### Benefits
- **Automatic Discovery**: Crawlers detect Delta format and schema without manual definitions
- **Schema Evolution**: Automatic catalog updates via crawler schema change policies
- **Separation of Concerns**: Jobs write data, orchestration manages catalog (zero boto3.client("glue") in jobs)
- **CI/CD Ready**: Crawlers defined in CDK, invoked by Step Functions orchestration
- **No Lambda Complexity**: Pure CloudFormation resources (no custom Lambda-backed resources)
- **Athena v3 Native**: Direct Delta Lake reads without manifest generation
- **Version Control**: Crawler definitions tracked in Git like all other infrastructure

### Trade-offs
- Initial table registration requires running crawler after first job (handled by Step Functions)
- Schema changes may take 1-2 minutes for crawler to detect and update (vs immediate with CfnTable)
- Crawler costs: ~$0.44/hour while running (~2-5 minutes per run = pennies)

## Why This Pattern Is Optimal

### Architectural Simplicity

This is **the simplest Delta Lake implementation** using only AWS-native primitives. Compared to alternatives:

| Pattern                           | Pros                                      | Cons                                     | Status    |
|-----------------------------------|-------------------------------------------|------------------------------------------|-----------|
| **Glue Crawler (current)**        | Automatic discovery, schema evolution, no Lambda | Initial registration delay (~2 min)     | ✅ Active |
| **CDK CfnTable**                  | Immediate registration, zero runtime cost | Manual schema definition, no auto-evolution | Alternative |
| **AwsCustomResource**             | (none - deprecated)                       | Lambda complexity, IAM overhead, brittle | ❌ Deprecated |
| **Databricks / EMR Delta**        | Advanced features (streaming, Z-order)    | External control plane, higher costs     | Overkill  |
| **Lake Formation Delta**          | Fine-grained permissions                  | Over-engineered for most use cases       | Overkill  |

### Operational Characteristics

| Aspect            | Result                                                   |
|-------------------|----------------------------------------------------------|
| **Cost**          | Pennies/day for S3 + Glue catalog + occasional ETL runs |
| **Maintenance**   | Near-zero; schema evolves automatically via Delta log    |
| **Portability**   | Full; deploys identically across dev/stage/prod         |
| **Observability** | CloudWatch + Spark UI enabled out-of-box                |
| **Time-to-prod**  | Minutes; no cluster bootstrap or external dependencies   |

### When You'd Outgrow This

Only move beyond this pattern if you need:
- **Streaming upserts** → Glue Streaming or Kinesis Data Analytics
- **Cross-table transactions** → Lake Formation governed tables
- **Fine-grained row/column permissions** → Lake Formation or Databricks Unity Catalog
- **Python UDF-heavy workloads** → EMR Serverless

Otherwise, this pattern scales to billions of rows and hundreds of datasets.

## Implementation Pattern

### CDK Stack (GoldStack.js) - Crawler Pattern

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

// Export crawler name for Step Functions orchestration
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

**Symptom (before fix)**: After running crawler on a valid Delta Lake table, you see garbage tables:
- `00000000000000000000_json` - Delta transaction log file treated as table
- `00000000000000000000_crc` - CRC checksum files treated as table
- `status_current` - Partition directories treated as separate Parquet tables

**Solution**: Use native `deltaTargets` property:

```javascript
targets: {
  deltaTargets: [{
    deltaTables: [goldBasePath],  // S3 path to Delta table root
    writeManifest: false          // No manifest generation needed
  }]
}
```

**Old Pattern (deprecated)**:
```javascript
classifiers: ['delta-lake'],  // No longer needed
targets: {
  s3Targets: [{ path: goldBasePath }]  // Use deltaTargets instead
}
```

**Verification**: After deploying and running crawler, check the table classification:

```bash
# Should show classification=delta for Delta Lake tables
aws glue get-table \
  --database-name pp_dw_gold \
  --name fda_all_ndcs \
  --query 'Table.Parameters' \
  --output json

# Expected (correct):
{
  "classification": "delta",
  "table_type": "DELTA",
  "EXTERNAL": "TRUE"
}
```

**Rule**: If your Glue job uses `'--datalake-formats': 'delta'`, your crawler MUST use `deltaTargets`.

### Step Functions Orchestration

Crawler invocation is handled by Step Functions (NOT by jobs):

```javascript
// In orchestration stack (e.g., DailyFdaOrchestrationStack.js)
const goldJobTask = new tasks.GlueStartJobRun(this, 'RunGoldJob', {
  glueJobName: goldStack.goldJob.name,
  integrationPattern: sfn.IntegrationPattern.RUN_JOB
});

const checkTablesTask = new tasks.LambdaInvoke(this, 'CheckTables', {
  lambdaFunction: checkTablesLambda,
  payload: sfn.TaskInput.fromObject({
    database: goldDatabase,
    expected_tables: [goldTableName]
  })
});

const startCrawlersTask = new tasks.LambdaInvoke(this, 'StartCrawlers', {
  lambdaFunction: startCrawlersLambda,
  payload: sfn.TaskInput.fromObject({
    crawler_names: [goldStack.goldCrawler.name]
  })
});

const waitForCrawler = new sfn.Wait(this, 'WaitForCrawler', {
  time: sfn.WaitTime.duration(cdk.Duration.seconds(30))
});

// Chain: Job → Check Tables → Start Crawler → Wait
goldJobTask
  .next(checkTablesTask)
  .next(startCrawlersTask)
  .next(waitForCrawler);
```

### Production Features

1. **Pure CloudFormation**: `glue.CfnCrawler` - first-class resource, no Lambda-backed logic
2. **Automatic Discovery**: Crawler detects Delta format and schema from `_delta_log/`
3. **Schema Evolution**: `UPDATE_IN_DATABASE` / `DEPRECATE_IN_DATABASE` handles column changes
4. **Separation of Concerns**: Jobs write data, orchestration manages catalog
5. **Zero Job Coupling**: Jobs never call `boto3.client("glue")` or know about crawlers
6. **On-Demand Only**: No crawler schedule; invoked by Step Functions only
7. **Testability**: Can run crawler manually for debugging: `aws glue start-crawler --name <name>`
8. **Environment Portable**: Same pattern across dev/stage/prod

## Validation

After deployment and first orchestration run, verify table registration:

```bash
# 1. Check crawler completed successfully
aws glue get-crawler --name pp-dw-gold-crawler-fda-all-ndcs \
  --query 'Crawler.LastCrawl.Status' --output text
# Expected: SUCCEEDED

# 2. Check Glue catalog table metadata
aws glue get-table --database-name pp_dw_gold --name fda_all_ndcs \
  --query 'Table.Parameters' --output json
# Expected output:
{
  "classification": "delta",
  "table_type": "DELTA",
  "EXTERNAL": "TRUE"
}

# 3. Query in Athena (Engine v3)
SELECT COUNT(*) FROM pp_dw_gold.fda_all_ndcs WHERE status = 'current';
```

## Historical Note: Migration from AwsCustomResource Pattern

**Context**: Earlier implementations used `AwsCustomResource` (Lambda-backed custom resource) to register Delta tables via boto3 Glue API calls. This added procedural complexity, IAM overhead, and non-deterministic behavior.

**Migration Path** (if migrating from AwsCustomResource):

1. **Update Stack**: Replace `AwsCustomResource` block with `glue.CfnCrawler` pattern (see above)
2. **Remove Lambda IAM Policies**: Delete Glue API permissions for custom resource Lambda
3. **Remove Job Coupling**: Delete any `crawler_name` arguments from job definitions
4. **Update Job Scripts**: Remove any crawler-related print statements or boto3 calls
5. **Delete Old Table** (optional): `aws glue delete-table --database pp_dw_gold --name <old_table>`
6. **Deploy Stack**: Crawler will be created via CloudFormation
7. **Run Orchestration**: Step Functions will invoke crawler automatically after job
8. **Validate**: Verify table exists and Athena queries work

**Why Crawlers Are Better**:
- ✅ No Lambda complexity (pure CloudFormation)
- ✅ No IAM policy juggling
- ✅ Automatic schema discovery from Delta transaction log
- ✅ Built-in schema evolution support
- ✅ Consistent with Bronze/Silver layers (same pattern everywhere)
- ✅ Separation of concerns (data-plane vs control-plane)

---

## Delta Lake Maintenance Strategy

**Status**: 📋 Not Yet Implemented (Future Enhancement)

Delta Lake tables require periodic maintenance operations to optimize query performance and manage storage costs. These operations should be run outside of the main ETL pipeline.

### Required Operations

#### 1. OPTIMIZE (File Compaction)

**Purpose**: Combine small files into larger ones to improve query performance.

**When to run**:
- After significant data changes (>1000 small files)
- Monthly maintenance window
- When query performance degrades

**Implementation** (future Lambda or Glue maintenance job):
```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://pp-dw-{account}/gold/fda-all-ndcs/")
delta_table.optimize().executeCompaction()
```

**Expected benefit**: 2-5x faster queries by reducing file open overhead

---

#### 2. ZORDER BY (Read Optimization)

**Purpose**: Co-locate related data to minimize data scanned during queries.

**When to run**:
- After OPTIMIZE (requires compacted files)
- Monthly maintenance window
- When query patterns stabilize

**Implementation**:
```python
# For gold tables with temporal versioning
delta_table.optimize().executeZOrderBy("ndc_11")  # Business key

# For tables with common filter columns
delta_table.optimize().executeZOrderBy("status", "run_date")
```

**Expected benefit**: 30-70% reduction in data scanned for filtered queries

**Column selection strategy**:
- **fda-all-ndcs**: `ndc_11` (primary key for point lookups)
- **rxnorm-products**: `rxcui` (primary key)
- **rxnorm-ndc-mappings**: `ndc_11`, `rxcui` (composite key)
- **rxnorm-product-classifications**: `rxcui`, `class_id` (composite key)

---

#### 3. VACUUM (Remove Old Files)

**Purpose**: Permanently delete files that are no longer referenced by any Delta table version.

**⚠️ CRITICAL**: VACUUM removes historical data. Time travel queries beyond the retention period will fail.

**When to run**:
- After determining appropriate retention period
- Monthly maintenance window
- When S3 storage costs become significant

**Implementation**:
```python
# Default: Remove files older than 7 days (Delta default retention)
delta_table.vacuum()

# Custom retention: 168 hours = 7 days
delta_table.vacuum(168)

# Longer retention for audit/compliance (30 days)
delta_table.vacuum(720)
```

**Retention recommendations**:
- **Development**: 7 days (default)
- **Production**: 30-90 days (balance time travel vs storage cost)
- **Compliance/Audit**: 365+ days (if required by regulations)

**Cost calculation**:
```
Storage per version ≈ (% of data changed) × (table size)
Example: 0.2% daily change × 100 MB table × 30 days ≈ 6 MB historical versions
At $0.023/GB/month → ~$0.14/month/table for 30-day retention
```

---

### Automation Strategy (Future)

#### Phase 1: Manual Operations (Current)
Run maintenance commands via ad-hoc Glue notebooks or local Spark.

#### Phase 2: Scheduled Glue Jobs (Next)
Create dedicated maintenance jobs:
- `pp-dw-maintenance-optimize-gold` (weekly)
- `pp-dw-maintenance-vacuum-gold` (monthly)
- EventBridge rules for scheduling

#### Phase 3: Intelligent Automation (Future)
Lambda function that:
1. Monitors Delta table metrics (file count, query performance)
2. Triggers OPTIMIZE when file count > threshold
3. Triggers VACUUM based on retention policy
4. Sends SNS alerts on completion/failure

**Metrics to monitor** (via CloudWatch):
- Number of files per table (trigger: >1000)
- Average file size (trigger: <10 MB)
- Query execution time trends (trigger: >2x baseline)
- S3 storage costs per table

---

### Implementation Checklist

When implementing maintenance operations:

- [ ] Create `infra/etl/maintenance/` directory
- [ ] Write maintenance Glue job(s) with OPTIMIZE/VACUUM logic
- [ ] Add CloudWatch alarms for file count thresholds
- [ ] Define retention policies per environment (dev/prod)
- [ ] Document runbook for manual maintenance
- [ ] Add EventBridge schedules for automation
- [ ] Test VACUUM retention periods in dev environment
- [ ] Validate time travel works within retention window

---

### References

- [Delta Lake OPTIMIZE documentation](https://docs.delta.io/latest/optimizations-oss.html)
- [Delta Lake VACUUM documentation](https://docs.delta.io/latest/delta-utility.html#vacuum)
- [ZORDER BY best practices](https://docs.delta.io/latest/optimizations-oss.html#z-ordering-multi-dimensional-clustering)

---

## Status

✅ **All GOLD stacks now use Crawler pattern (2025-11-17)**

- `fda-all-ndcs` - **Delta Lake** with Crawler (temporal versioning)
- `rxnorm-products` - **Parquet** with Crawler (temporal versioning)
- `rxnorm-ndc-mappings` - **Parquet** with Crawler (temporal versioning)
- `rxnorm-product-classifications` - **Parquet** with Crawler (temporal versioning)
- `drugs-sync` - DynamoDB sync (no crawler needed)

**Note**: Only `fda-all-ndcs` uses Delta Lake format currently. Other gold tables use Parquet with temporal versioning. All use Glue Crawlers for catalog registration (same pattern, different storage format).

---

## 📋 Canonical Reference: The FDA Pipeline

**NSDE → CDER → fda-all-ndcs** is the **reference implementation** for the complete RAWS ETL pattern:

**Bronze Layer** (Pattern A - Factory-based):
- `datasets/bronze/fda-nsde/FdaNsdeStack.js` - HTTP/ZIP ingestion via factory
- `datasets/bronze/fda-cder/FdaCderStack.js` - HTTP/ZIP ingestion via factory
- Uses shared `bronze_http_job.py` runtime
- Zero Glue API calls, pure data-plane

**Silver Layer** (Custom transformation):
- `datasets/silver/fda-all-ndcs/FdaAllNdcsStack.js` - Joins NSDE + CDER
- Custom `silver_job.py` with INNER JOIN logic
- Zero Glue API calls, pure data-plane

**Gold Layer** (Temporal versioning + Delta Lake):
- `datasets/gold/fda-all-ndcs/GoldFdaAllNdcsStack.js` - **Delta Lake with Crawler**
- Uses `temporal_versioning_delta.py` shared library
- Delta Lake 3.0 via Glue 5.0 (`--datalake-formats=delta`)
- Glue Crawler for automatic table registration
- Zero Glue API calls, pure data-plane

**Why This Pipeline Is Canonical:**
1. ✅ Complete Bronze → Silver → Gold flow
2. ✅ Demonstrates both factory pattern (Bronze) and custom stacks (Silver/Gold)
3. ✅ Shows data lineage across multiple sources (NSDE + CDER)
4. ✅ Implements temporal versioning (SCD Type 2) for incremental sync
5. ✅ Uses Delta Lake for ACID transactions and schema evolution
6. ✅ Zero crawler coupling - all jobs are pure data-plane
7. ✅ Step Functions orchestration handles all control-plane concerns
8. ✅ Deploy-time paths use centralized `paths.js` helper

**When building new pipelines, copy the FDA pipeline structure first.**

---

**Decision Date**: 2025-11-17 (Updated from 2025-10-25)
**Author**: RAWS Architecture Team
**Status**: ✅ Accepted & Implemented (Crawler Pattern)
**Previous Patterns**: AwsCustomResource (deprecated 2025-11-16), CfnTable (alternative, not implemented)
