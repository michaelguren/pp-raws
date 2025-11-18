# Decision Record: Delta Table Registration via Glue Crawlers (Glue 5.0 + Athena v3)

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

## ⚠️ RAWS Delta Invariant

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  ALL GOLD Delta jobs MUST enable mergeSchema=true to make column            ║
║  additions boring. This is enforced by temporal_versioning_delta.py.        ║
║                                                                              ║
║  - Initial writes: .option("mergeSchema", "true")                           ║
║  - MERGE operations: spark.databricks.delta.schema.autoMerge.enabled=true   ║
║                                                                              ║
║  Combined with Glue Crawler schema change policies, this makes schema       ║
║  evolution safe, automatic, and non-disruptive.                             ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Job-Level Contract** (enforced by `temporal_versioning_delta.py`):
1. All Delta writes use `.option("mergeSchema", "true")`
2. MERGE operations use `spark.databricks.delta.schema.autoMerge.enabled=true`
3. This is automatic - no job code changes needed when adding columns

**Infrastructure-Level Contract** (enforced by CDK):
1. Glue Crawlers use `schemaChangePolicy.updateBehavior: UPDATE_IN_DATABASE`
2. Glue Crawlers use `schemaChangePolicy.deleteBehavior: DEPRECATE_IN_DATABASE`
3. Crawlers automatically sync new columns to Glue catalog

**Outcome**: Adding columns becomes a non-event:
- Developer adds column to Silver layer
- Gold job writes it (mergeSchema handles schema mismatch)
- Crawler syncs it to catalog (UPDATE_IN_DATABASE handles new column)
- Athena sees it immediately
- Historical records show NULL for new column (correct behavior)

**Testing**: See `DELTA_TEST_PLAN.md` Run 5 - validates end-to-end schema evolution.

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
const goldCrawler = new glue.CfnCrawler(this, 'GoldCrawler', {
  name: `${etlConfig.etl_resource_prefix}-gold-crawler-${dataset}`,
  description: `Crawler for GOLD ${dataset} Delta Lake table`,
  role: glueRole.roleArn,
  databaseName: goldDatabase,

  // Target S3 path where Delta Lake data is written
  targets: {
    s3Targets: [{
      path: goldBasePath
    }]
  },

  // CRITICAL: Schema change policies enable automatic evolution
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
  }),

  // On-demand only (invoked by Step Functions)
  schedule: {
    scheduleExpression: ''  // Empty = no schedule
  }
});

// Crawler depends on bucket (not job - they're peers)
goldCrawler.node.addDependency(dataWarehouseBucket);

// Export crawler name for Step Functions orchestration
this.goldCrawler = goldCrawler;
```

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
