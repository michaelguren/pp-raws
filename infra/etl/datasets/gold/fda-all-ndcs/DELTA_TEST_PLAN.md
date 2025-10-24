# Delta Lake Testing Guide - FDA All NDCs

Complete validation protocol for Delta Lake temporal versioning with automated test harness and reproducible scenarios.

## Overview

The Delta Lake implementation replaces Parquet full-overwrite with efficient MERGE operations:
- **90%+ write reduction** - Only changed records are written
- **ACID transactions** - No partial writes or corruption
- **Time travel** - Query historical versions
- **Schema evolution** - Automatic with `mergeSchema=true`

## Test Scenarios

We'll run 4 controlled tests:
1. **Run 1** - Initial load (all NEW)
2. **Run 2** - Incremental changes (NEW + CHANGED + UNCHANGED)
3. **Run 3** - Record deletions (EXPIRED)
4. **Run 4** - Idempotency check (re-run Run 2 data)

---

## Quick Start (Automated)

```bash
# Option A: Fully automated test harness
cd /Users/michaelguren/Documents/pp_raws/infra/etl/datasets/gold/fda-all-ndcs
python3 tests/test_delta_gold_fda_all_ndcs.py

# Option B: Manual step-by-step (see below)
```

---

## Manual Testing Guide

### Prerequisites

```bash
# Install dependencies
pip install pyarrow pandas boto3

# Set test date for reproducibility (use same date for all runs)
export TEST_RUN_DATE="2025-10-21"
export AWS_REGION="us-east-1"
export BUCKET="pp-dw-550398958311"
```

---

## Step 1: Deploy & Run Initial Load

### 1a. Deploy the Updated Gold Stack

```bash
cd /Users/michaelguren/Documents/pp_raws/infra/etl
cdk deploy pp-dw-etl-gold-fda-all-ndcs
```

### 1b. Baseline File Count

```bash
# Check current silver file count
aws s3 ls s3://${BUCKET}/silver/fda-all-ndcs/ --recursive | grep ".parquet" | wc -l

# Gold should be empty initially
aws s3 ls s3://${BUCKET}/gold/fda-all-ndcs/ --recursive | grep ".parquet" | wc -l
```

### 1c. Run the Pipeline with Fixed run_date

```bash
# Run silver job (bronze should already have data)
aws glue start-job-run \
  --job-name pp-dw-silver-fda-all-ndcs \
  --region ${AWS_REGION}

# Wait for completion, get run ID
SILVER_RUN_ID=$(aws glue get-job-runs \
  --job-name pp-dw-silver-fda-all-ndcs \
  --max-items 1 \
  --region ${AWS_REGION} \
  --query 'JobRuns[0].Id' \
  --output text)

echo "Silver Run ID: ${SILVER_RUN_ID}"

# Poll for completion
aws glue get-job-run \
  --job-name pp-dw-silver-fda-all-ndcs \
  --run-id ${SILVER_RUN_ID} \
  --region ${AWS_REGION} \
  --query 'JobRun.JobRunState'

# Once SUCCEEDED, run gold with EXPLICIT run_date
GOLD_RUN1=$(aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs \
  --arguments "{\"--run_date\":\"${TEST_RUN_DATE}\"}" \
  --region ${AWS_REGION} \
  --query 'JobRunId' \
  --output text)

echo "Gold Run 1 ID: ${GOLD_RUN1}"

# Monitor logs (filter by job run ID)
aws logs tail /aws-glue/jobs/logs-v2 \
  --since 10m \
  --follow \
  --filter-pattern "Delta Merge Complete" \
  --log-stream-name-prefix pp-dw-gold-fda-all-ndcs \
  --region ${AWS_REGION}
```

### 1d. Run Crawler

```bash
aws glue start-crawler \
  --name pp-dw-gold-fda-all-ndcs-crawler \
  --region ${AWS_REGION}

# Wait for crawler completion
aws glue get-crawler \
  --name pp-dw-gold-fda-all-ndcs-crawler \
  --region ${AWS_REGION} \
  --query 'Crawler.State'
```

### 1e. Expected Result

**CloudWatch Logs:**
```
✅ Delta Merge Complete — New: ~380000 | Changed: 0 | Expired: 0 | Total: ~380000
```

**File Count Check:**
```bash
# Delta log should have version 0
aws s3 ls s3://${BUCKET}/gold/fda-all-ndcs/_delta_log/
# Expected: 00000000000000000000.json

# Count Parquet files
aws s3 ls s3://${BUCKET}/gold/fda-all-ndcs/ --recursive | grep ".parquet" | wc -l
# Expected: ~10-50 files (depends on parallelism)
```

### 1f. Athena Validation

```sql
-- All records should be 'current'
SELECT status, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs
GROUP BY status;
-- Expected: status='current', cnt=~380000

-- Verify run_date matches
SELECT DISTINCT run_date FROM pp_dw_gold.fda_all_ndcs;
-- Expected: 2025-10-21

-- Sample records (save NDCs for later)
SELECT ndc_11, proprietary_name, dosage_form, marketing_category,
       active_from, active_to, status, version_id
FROM pp_dw_gold.fda_all_ndcs
LIMIT 10;

-- Check Delta version history
DESCRIBE HISTORY pp_dw_gold.fda_all_ndcs ORDER BY version DESC LIMIT 1;
-- Expected: version=0, operation=WRITE
```

### 1g. Temporal Integrity Check

```sql
-- Should return 0 rows (no duplicates)
SELECT ndc_11, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs
WHERE status = 'current'
GROUP BY ndc_11
HAVING COUNT(*) > 1;

-- All active_to should be 9999-12-31
SELECT COUNT(*) FROM pp_dw_gold.fda_all_ndcs
WHERE status = 'current' AND active_to <> DATE '9999-12-31';
-- Expected: 0
```

---

## Step 2: Test CHANGED, NEW, and UNCHANGED (Run 2)

### 2a. Download Silver Data

```bash
# Backup entire silver dataset
mkdir -p /tmp/delta_test_silver_backup
aws s3 sync s3://${BUCKET}/silver/fda-all-ndcs/ /tmp/delta_test_silver_backup/

echo "Silver files backed up to /tmp/delta_test_silver_backup/"
ls -lh /tmp/delta_test_silver_backup/
```

### 2b. Create & Run Modification Script

Create `/tmp/delta_test_modify_silver.py`:

```python
#!/usr/bin/env python3
"""
Delta Lake Test - Silver Data Modifier
Generates controlled test scenarios for temporal versioning validation.
"""

import pyarrow.parquet as pq
import pandas as pd
import glob
import os
import json
import random

# Configuration
INPUT_DIR = '/tmp/delta_test_silver_backup'
OUTPUT_RUN2 = '/tmp/delta_test_silver_run2.parquet'
OUTPUT_RUN3 = '/tmp/delta_test_silver_run3.parquet'
METADATA_FILE = '/tmp/delta_test_metadata.json'
RANDOM_SEED = 42  # For deterministic test data selection

# Set random seed for reproducibility
random.seed(RANDOM_SEED)

print("=" * 70)
print("🧪 Delta Lake Test - Silver Data Modification")
print("=" * 70)
print(f"Random seed: {RANDOM_SEED} (for deterministic selection)")

# Read all Parquet files
parquet_files = sorted(glob.glob(f'{INPUT_DIR}/*.parquet'))
print(f"\n📂 Reading {len(parquet_files)} Parquet file(s)...")

dfs = [pq.read_table(f).to_pandas() for f in parquet_files]
df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

original_count = len(df)
print(f"   ✓ Original records: {original_count:,}")
print(f"   ✓ Columns: {len(df.columns)}")

# =============================================================================
# Run 2 Modifications (Deterministic Selection)
# =============================================================================

# Scenario 1: CHANGED - Modify 10 records (using fixed seed)
changed_indices = list(range(0, 10))  # Use first 10 for simplicity
changed_ndcs = df.loc[changed_indices, 'ndc_11'].tolist()
df.loc[changed_indices, 'proprietary_name'] = \
    df.loc[changed_indices, 'proprietary_name'] + ' [MODIFIED]'

# Scenario 2: NEW - Add 5 new records
new_records = df.iloc[10:15].copy()
new_records['ndc_11'] = new_records['ndc_11'].str[:9] + '99'
new_ndcs = new_records['ndc_11'].tolist()
df_run2 = pd.concat([df, new_records], ignore_index=True)

# Scenario 3: EXPIRED (for Run 3) - Remove 5 records
expired_indices = list(range(15, 20))
expired_ndcs = df.loc[expired_indices, 'ndc_11'].tolist()
df_run3 = df_run2.drop(expired_indices).reset_index(drop=True)

unchanged_count = original_count - 15

# Write Run 2 data
pq.write_table(pq.Table.from_pandas(df_run2), OUTPUT_RUN2, compression='snappy')

# Write Run 3 data
pq.write_table(pq.Table.from_pandas(df_run3), OUTPUT_RUN3, compression='snappy')

# Save metadata with expected results for validation
metadata = {
    'test_date': '2025-10-21',
    'random_seed': RANDOM_SEED,
    'original_count': original_count,
    'run2': {
        'total': len(df_run2),
        'changed': len(changed_ndcs),
        'changed_ndcs': changed_ndcs,
        'new': len(new_ndcs),
        'new_ndcs': new_ndcs,
        'unchanged': unchanged_count
    },
    'run3': {
        'total': len(df_run3),
        'expired': len(expired_ndcs),
        'expired_ndcs': expired_ndcs
    },
    'expected_results': {
        'run1': {'new': original_count, 'changed': 0, 'expired': 0, 'version': 0},
        'run2': {'new': 5, 'changed': 10, 'expired': 0, 'version': 1},
        'run3': {'new': 0, 'changed': 0, 'expired': 5, 'version': 2},
        'run4': {'new': 5, 'changed': 0, 'expired': 5, 'version': 3}
    }
}

with open(METADATA_FILE, 'w') as f:
    json.dump(metadata, f, indent=2)

# Summary
print("\n" + "─" * 70)
print("📊 Delta Test Metadata")
print("─" * 70)
print(f"Changed:   {len(changed_ndcs):>6,}")
print(f"New:       {len(new_ndcs):>6,}")
print(f"Expired:   {len(expired_ndcs):>6,}  (for Run 3)")
print(f"Unchanged: {unchanged_count:>6,}")
print("─" * 70)

print(f"\n✅ Test data generated:")
print(f"   Run 2: {OUTPUT_RUN2}")
print(f"   Run 3: {OUTPUT_RUN3}")
print(f"   Metadata: {METADATA_FILE}")
```

Run it:
```bash
python3 /tmp/delta_test_modify_silver.py

# Review metadata
cat /tmp/delta_test_metadata.json | jq .
```

### 2c. Upload Modified Silver Data (Run 2)

```bash
# IMPORTANT: Backup current silver to S3
aws s3 sync s3://${BUCKET}/silver/fda-all-ndcs/ \
  s3://${BUCKET}/silver/fda-all-ndcs-backup/ \
  --region ${AWS_REGION}

# Clear current silver
aws s3 rm s3://${BUCKET}/silver/fda-all-ndcs/ --recursive --region ${AWS_REGION}

# Upload Run 2 data
aws s3 cp /tmp/delta_test_silver_run2.parquet \
  s3://${BUCKET}/silver/fda-all-ndcs/part-00000.snappy.parquet \
  --region ${AWS_REGION}

# Verify file count (should be 1)
aws s3 ls s3://${BUCKET}/silver/fda-all-ndcs/ --recursive | grep ".parquet" | wc -l

# Update catalog
aws glue start-crawler \
  --name pp-dw-silver-fda-all-ndcs-crawler \
  --region ${AWS_REGION}

# Wait for crawler
aws glue get-crawler \
  --name pp-dw-silver-fda-all-ndcs-crawler \
  --region ${AWS_REGION} \
  --query 'Crawler.State'
```

### 2d. Run Gold Job (Run 2) with Same run_date

```bash
GOLD_RUN2=$(aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs \
  --arguments "{\"--run_date\":\"${TEST_RUN_DATE}\"}" \
  --region ${AWS_REGION} \
  --query 'JobRunId' \
  --output text)

echo "Gold Run 2 ID: ${GOLD_RUN2}"

# Monitor
aws logs tail /aws-glue/jobs/logs-v2 \
  --since 10m \
  --follow \
  --filter-pattern "Delta Merge Complete" \
  --log-stream-name-prefix pp-dw-gold-fda-all-ndcs \
  --region ${AWS_REGION}
```

### 2e. Expected Result (Run 2)

**CloudWatch Logs:**
```
✅ Delta Merge Complete — New: 5 | Changed: 10 | Expired: 0 | Total: 30
```

**File Count Check:**
```bash
# Delta log should now have version 1
aws s3 ls s3://${BUCKET}/gold/fda-all-ndcs/_delta_log/
# Expected: 00000000000000000000.json, 00000000000000000001.json

# Parquet file count should remain relatively stable (Delta compaction)
aws s3 ls s3://${BUCKET}/gold/fda-all-ndcs/ --recursive | grep ".parquet" | wc -l
```

### 2f. Athena Validation (Run 2)

```sql
-- Status distribution
SELECT status, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs
GROUP BY status;
-- Expected: current=~380005, historical=10

-- Verify changed records have 2 versions
SELECT ndc_11, proprietary_name, active_from, active_to, status, version_id
FROM pp_dw_gold.fda_all_ndcs
WHERE ndc_11 IN (
  SELECT ndc_11 FROM pp_dw_gold.fda_all_ndcs
  GROUP BY ndc_11 HAVING COUNT(*) > 1
)
ORDER BY ndc_11, active_from DESC;
-- Should show pairs with [MODIFIED] suffix in current version

-- Verify NEW records
SELECT * FROM pp_dw_gold.fda_all_ndcs
WHERE ndc_11 LIKE '%99'
  AND status = 'current';
-- Expected: 5 records

-- Check Delta version
DESCRIBE HISTORY pp_dw_gold.fda_all_ndcs ORDER BY version DESC LIMIT 1;
-- Expected: version=1, operation=MERGE

-- Temporal integrity: no duplicate current records
SELECT ndc_11, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs
WHERE status = 'current'
GROUP BY ndc_11
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Temporal integrity: verify validity ranges don't overlap
SELECT t1.ndc_11
FROM pp_dw_gold.fda_all_ndcs t1
JOIN pp_dw_gold.fda_all_ndcs t2
  ON t1.ndc_11 = t2.ndc_11
  AND t1.version_id <> t2.version_id
WHERE t1.active_from <= t2.active_to
  AND t2.active_from <= t1.active_to
  AND NOT (t1.active_to = DATE '9999-12-31' OR t2.active_to = DATE '9999-12-31');
-- Expected: 0 rows
```

---

## Step 3: Test EXPIRED Scenario (Run 3)

### 3a. Upload Run 3 Data (Records Removed)

```bash
# Clear current silver
aws s3 rm s3://${BUCKET}/silver/fda-all-ndcs/ --recursive --region ${AWS_REGION}

# Upload Run 3 data (5 fewer records)
aws s3 cp /tmp/delta_test_silver_run3.parquet \
  s3://${BUCKET}/silver/fda-all-ndcs/part-00000.snappy.parquet \
  --region ${AWS_REGION}

# Update catalog
aws glue start-crawler \
  --name pp-dw-silver-fda-all-ndcs-crawler \
  --region ${AWS_REGION}
```

### 3b. Run Gold Job (Run 3)

```bash
GOLD_RUN3=$(aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs \
  --arguments "{\"--run_date\":\"${TEST_RUN_DATE}\"}" \
  --region ${AWS_REGION} \
  --query 'JobRunId' \
  --output text)

echo "Gold Run 3 ID: ${GOLD_RUN3}"

aws logs tail /aws-glue/jobs/logs-v2 \
  --since 10m \
  --follow \
  --filter-pattern "Delta Merge Complete" \
  --log-stream-name-prefix pp-dw-gold-fda-all-ndcs \
  --region ${AWS_REGION}
```

### 3c. Expected Result (Run 3)

**CloudWatch Logs:**
```
✅ Delta Merge Complete — New: 0 | Changed: 0 | Expired: 5 | Total: 5
```

**File Count:**
```bash
aws s3 ls s3://${BUCKET}/gold/fda-all-ndcs/_delta_log/
# Expected: versions 0, 1, 2
```

### 3d. Athena Validation (Run 3)

```sql
-- Status distribution
SELECT status, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs
GROUP BY status;
-- Expected: current=~380000, historical=15

-- Verify expired records
SELECT ndc_11, proprietary_name, active_from, active_to, status
FROM pp_dw_gold.fda_all_ndcs
WHERE status = 'historical'
  AND active_to = DATE '2025-10-21'
ORDER BY ndc_11;
-- Expected: 5 newly expired records

-- Delta version
DESCRIBE HISTORY pp_dw_gold.fda_all_ndcs ORDER BY version DESC LIMIT 1;
-- Expected: version=2, operation=MERGE
```

---

## Step 4: Idempotency Test (Run 4)

### 4a. Re-Upload Run 2 Data

```bash
# Upload same Run 2 data again
aws s3 rm s3://${BUCKET}/silver/fda-all-ndcs/ --recursive --region ${AWS_REGION}

aws s3 cp /tmp/delta_test_silver_run2.parquet \
  s3://${BUCKET}/silver/fda-all-ndcs/part-00000.snappy.parquet \
  --region ${AWS_REGION}

aws glue start-crawler \
  --name pp-dw-silver-fda-all-ndcs-crawler \
  --region ${AWS_REGION}
```

### 4b. Run Gold Job (Run 4)

```bash
GOLD_RUN4=$(aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs \
  --arguments "{\"--run_date\":\"${TEST_RUN_DATE}\"}" \
  --region ${AWS_REGION} \
  --query 'JobRunId' \
  --output text)

echo "Gold Run 4 ID (Idempotency Test): ${GOLD_RUN4}"
```

### 4c. Expected Result (Run 4) - Idempotency

**CloudWatch Logs:**
```
✅ Delta Merge Complete — New: 5 | Changed: 0 | Expired: 5 | Total: 10

Explanation:
- 5 records from Run 3 that were EXPIRED are now back (NEW)
- 5 records from Run 2 that were removed in Run 3 need to be EXPIRED again
- No CHANGED (same data as Run 2)
```

### 4d. Athena Validation (Run 4)

```sql
-- Should return to Run 2 state
SELECT status, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs
GROUP BY status;
-- Expected: current=~380005, historical=20 (10 from Run 2, 5 from Run 3, 5 from Run 4)

-- Delta version
DESCRIBE HISTORY pp_dw_gold.fda_all_ndcs ORDER BY version DESC LIMIT 1;
-- Expected: version=3, operation=MERGE
```

---

## Step 5: Advanced Validation

### Time Travel Queries

```sql
-- Version 0: Initial load
SELECT COUNT(*) as v0_count
FROM pp_dw_gold.fda_all_ndcs VERSION AS OF 0;
-- Expected: ~380000

-- Version 1: After Run 2
SELECT status, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs VERSION AS OF 1
GROUP BY status;
-- Expected: current=~380005, historical=10

-- Version 2: After Run 3
SELECT status, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs VERSION AS OF 2
GROUP BY status;
-- Expected: current=~380000, historical=15

-- Version 3: After Run 4 (idempotency)
SELECT status, COUNT(*) as cnt
FROM pp_dw_gold.fda_all_ndcs VERSION AS OF 3
GROUP BY status;
-- Expected: current=~380005, historical=20
```

### Delta Transaction Log

```bash
# Download and inspect transaction logs
aws s3 cp s3://${BUCKET}/gold/fda-all-ndcs/_delta_log/00000000000000000001.json - | jq .

# Expected fields:
# - commitInfo.operation: "MERGE"
# - commitInfo.operationMetrics.numTargetRowsInserted
# - commitInfo.operationMetrics.numTargetRowsUpdated
```

---

## Step 6: Cleanup & Restore

### Restore Original Silver Data

```bash
# Clear test data
aws s3 rm s3://${BUCKET}/silver/fda-all-ndcs/ --recursive --region ${AWS_REGION}

# Restore from backup
aws s3 sync s3://${BUCKET}/silver/fda-all-ndcs-backup/ \
  s3://${BUCKET}/silver/fda-all-ndcs/ \
  --region ${AWS_REGION}

# Delete backup
aws s3 rm s3://${BUCKET}/silver/fda-all-ndcs-backup/ --recursive --region ${AWS_REGION}

# Update catalog
aws glue start-crawler \
  --name pp-dw-silver-fda-all-ndcs-crawler \
  --region ${AWS_REGION}

# Re-run gold to production state
aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs \
  --region ${AWS_REGION}

# Cleanup local test files
rm -rf /tmp/delta_test_*
```

### Optional: Reset Gold Table

```bash
# WARNING: Deletes all gold data and Delta history!
aws s3 rm s3://${BUCKET}/gold/fda-all-ndcs/ --recursive --region ${AWS_REGION}

# Re-run from scratch
aws glue start-job-run --job-name pp-dw-silver-fda-all-ndcs --region ${AWS_REGION}
aws glue start-job-run --job-name pp-dw-gold-fda-all-ndcs --region ${AWS_REGION}
```

---

## Success Criteria ✅

- [ ] **Run 1**: Initial load creates Delta table (version 0)
- [ ] **Run 2**: MERGE detects NEW (5), CHANGED (10), UNCHANGED (~380K)
- [ ] **Run 3**: MERGE detects EXPIRED (5)
- [ ] **Run 4**: Idempotency validated (deterministic results)
- [ ] **Time Travel**: All 4 versions queryable independently
- [ ] **Temporal Integrity**: No gaps, overlaps, or duplicate current records
- [ ] **File Stability**: Delta compaction keeps file count reasonable
- [ ] **Performance**: 90%+ write reduction vs full overwrite
- [ ] **ACID**: Transaction log shows proper MERGE operations

---

## Automated Test Harness

See `tests/test_delta_gold_fda_all_ndcs.py` for fully automated validation.

Run with:
```bash
cd /Users/michaelguren/Documents/pp_raws/infra/etl/datasets/gold/fda-all-ndcs
python3 tests/test_delta_gold_fda_all_ndcs.py --test-date 2025-10-21
```

---

## Next Steps

Once validated, apply the same Delta Lake pattern to:
1. `rxnorm-products`
2. `rxnorm-ndc-mappings`
3. `rxnorm-product-classifications`

Each follows the identical pattern - just update:
- Job: `temporal_versioning.py` → `temporal_versioning_delta.py`
- Stack: Add Delta Spark config, reference Delta library
