🧠 Claude Code Prompt: Delta Lake Transition (Greenfield Best Practices)

**⚠️ NOTE: This document has been superseded by the production-hardened pattern in `DELTA_TABLE_REGISTRATION.md`**

The canonical RAWS pattern for Delta Lake (Glue 5.0 + Athena v3) is now:
- **Glue Crawlers with schema change policies** (automatic Delta format detection + schema evolution)
- Pure CloudFormation via `glue.CfnCrawler` (not AwsCustomResource or manual CfnTable)
- Production-hardened with automatic catalog management via Step Functions orchestration
- See `fda-all-ndcs` stack for reference implementation

---

# Claude Code: Objective

Transition all Gold-layer datasets in this greenfield RAWS ETL project from Parquet writes to **Delta Lake** using modern best practices.
No migration is required — assume no prior data exists.

# Claude Code: Context

- AWS Glue 5.0 runtime (Python)
- Spark DataFrames as primary abstraction
- Gold layer follows SCD Type 2 temporal versioning
- All datasets (FDA, RxNorm, etc.) follow same schema conventions
- Architecture pattern: bronze → silver → gold (Delta)
- **Table Registration**: Glue Crawlers with schema change policies (see `DELTA_TABLE_REGISTRATION.md`)

# Claude Code: Implementation Tasks

1. Replace all `.write.mode("overwrite").parquet(...)` operations with **Delta Lake MERGE logic**.
2. Implement shared library:  
   `infra/etl/shared/runtime/temporal/temporal_versioning_delta.py`
3. Define function:
   ```python
   def apply_temporal_versioning_delta(
       spark, incoming_df, gold_path,
       business_key, business_columns,
       run_date, run_id
   )
   ```
   - Use Delta MERGE operations to perform SCD Type 2 updates.
   - Handle NEW, CHANGED, and EXPIRED records.
   - Skip UNCHANGED records (no rewrite).
   - Expire changed/deleted rows with `active_to = date_sub(run_date, 1)`.
   - Insert new rows with `active_from = run_date` and `active_to = 9999-12-31`.
   - Include standard audit fields:
     `version_id`, `active_from`, `active_to`, `status`, `run_id`, `updated_at`.
4. Optimize performance:
   - Pre-filter changed keys using `content_hash` to avoid full-table scans.
   - Use key-based joins (`inner` and `anti`) instead of full merges.
   - Append with `.option("mergeSchema", "true")`.
5. Maintain simple, one-path logic:
   - No branching for migration or `isDeltaTable()` checks.
   - Assume first job run initializes the table.
6. Include summary logging:
   ```
   ✅ Delta Merge Complete — New: X | Changed: Y | Expired: Z | Total: N
   ```
7. Implement optional maintenance utilities:
   ```python
   delta_table.vacuum(168)  # weekly cleanup
   delta_table.optimize().executeZOrderBy(business_key)
   ```
8. Reference the shared library in Glue job args:
   ```
   --extra-py-files s3://pp-dw-[env]/etl/shared/runtime/temporal/temporal_versioning_delta.py
   ```
9. Validate with Athena queries:
   ```sql
   SELECT * FROM pp_dw_gold.table WHERE status = 'current';
   SELECT * FROM pp_dw_gold.table VERSION AS OF 3;
   ```

# Claude Code: AWS Glue 5.0 Delta Lake Configuration (CRITICAL)

## CDK Stack Configuration

**In your Gold stack `defaultArguments`:**

```javascript
defaultArguments: {
  // Enable Delta Lake support - this loads libraries
  '--datalake-formats': 'delta',

  // Deploy temporal versioning library
  '--extra-py-files': 's3://bucket/etl/shared/runtime/temporal/temporal_versioning_delta.py',

  // DO NOT use --conf here! It causes "Invalid input to --conf" errors
  // Spark configs must be set in Python code BEFORE SparkContext creation
}
```

**❌ WRONG - Do NOT do this:**
```javascript
'--conf': 'spark.sql.extensions=... spark.sql.catalog...'  // FAILS in Glue!
```

## Python Job Configuration

**Configure Spark BEFORE creating SparkContext:**

```python
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# ✅ CORRECT: Set configs BEFORE SparkContext creation
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Initialize with configured SparkConf
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
```

**❌ WRONG - Do NOT do this:**
```python
sc = SparkContext()  # SparkSession created here
spark.conf.set(...)  # TOO LATE! Causes "Cannot modify static config" error
```

## Why This Matters

1. **`--datalake-formats: 'delta'`** loads Delta Lake JARs and Python modules
2. **SparkConf must be configured BEFORE** `SparkContext()` initialization
3. **CDK `--conf` syntax doesn't work** in Glue 5.0 (causes bootstrap errors)
4. **Timing is critical** - static configs cannot be changed after session creation

## Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError: No module named 'delta'` | Missing `--datalake-formats` | Add `'--datalake-formats': 'delta'` to CDK |
| `Cannot modify the value of a static config` | Setting config after SparkContext | Use `SparkConf` before `SparkContext()` |
| `Invalid input to --conf` | Using `--conf` in CDK | Remove `--conf`, use Python `SparkConf` |
| `Delta operation requires SparkSession to be configured` | Missing extensions/catalog | Set via `SparkConf` in Python |

## Stack Dependencies

```javascript
// Ensure scripts are deployed before job creation
const scriptDeployment = new s3deploy.BucketDeployment(...);
const temporalDeployment = new s3deploy.BucketDeployment(...);

goldJob.node.addDependency(scriptDeployment);
goldJob.node.addDependency(temporalDeployment);
```

# Claude Code: Style Guide

- Comment using imperative directives for AI comprehension:
  ```python
  # Claude: Perform Delta MERGE using SCD Type 2 semantics.
  # Claude: Only process changed or new business keys.
  ```
- Keep functions short, declarative, and dataset-agnostic.
- Favor Spark expressions over Python loops.
- Avoid unnecessary defensive code or legacy patterns.

# Claude Code: Operational Hygiene

- **VACUUM** weekly to remove old Delta files.
- **OPTIMIZE** monthly or when file fragmentation > 1000 files/partition.
- Use **ZORDER BY** on business key for read performance.
- **No overwrite mode** anywhere in Gold layer.

# Claude Code: Testing Checklist

- ✅ First run creates Delta table successfully.
- ✅ Subsequent runs MERGE correctly with no data loss.
- ✅ Time travel works in Athena.
- ✅ Write volume reduced by >90% vs. full overwrite.
- ✅ Audit fields populated consistently.

# Claude Code: Success Criteria

- Gold tables use Delta format exclusively.
- Writes are idempotent and ACID-safe.
- S3 PUT requests drop by 90%+.
- No gaps or overlaps in temporal validity.
- Schema evolution supported with `mergeSchema=true`.
- All logic is simple enough for AI regeneration across datasets.
