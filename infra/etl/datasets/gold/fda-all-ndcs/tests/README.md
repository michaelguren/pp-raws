# Delta Lake Confidence Test for FDA All NDCs Gold Layer

This directory contains a confidence test for validating the Gold layer's Delta Lake + temporal versioning implementation.

## Overview

**File**: `simplified_delta_test.py`
**Stack**: `FdaAllNdcsDeltaLakeTestStack.js`
**Design**: Simple. Focused. Reliable.

- ✅ No Delta transaction log parsing
- ✅ No complex version checking
- ✅ Uses Athena for all validations (simple SQL queries)
- ✅ Runs as Glue Python Shell job (fast, cheap)
- ✅ ~10 minute runtime

**Scenarios**:
1. **ADD**: 10 new canary NDCs
2. **UPDATE**: 5 modified + 5 unchanged
3. **TERM**: 3 deleted/expired

**Deploy**:
```bash
cd infra/etl

# Add to index.js (if not already there):
# const { FdaAllNdcsDeltaLakeTestStack } = require("./datasets/gold/fda-all-ndcs/tests/FdaAllNdcsDeltaLakeTestStack");
# new FdaAllNdcsDeltaLakeTestStack(app, "pp-dw-test-gold-fda-all-ndcs", {
#   env,
#   etlCoreStack
# });

cdk deploy pp-dw-test-gold-fda-all-ndcs
```

**Run Tests**:
```bash
# Scenario 1: ADDs (run first)
aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs-delta-test \
  --arguments '{"--scenario":"add","--run_date":"2050-01-01"}'

# Scenario 2: UPDATES (run after Scenario 1)
aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs-delta-test \
  --arguments '{"--scenario":"update","--run_date":"2050-01-02"}'

# Scenario 3: TERMS (run after Scenario 2)
aws glue start-job-run \
  --job-name pp-dw-gold-fda-all-ndcs-delta-test \
  --arguments '{"--scenario":"term","--run_date":"2050-01-03"}'
```

**Check Results**:
```bash
# Watch job progress
aws glue get-job-run \
  --job-name pp-dw-gold-fda-all-ndcs-delta-test \
  --run-id <run-id-from-start-job-run>

# View logs in CloudWatch
# Log group: /aws-glue/python-jobs/output
# Log stream: pp-dw-gold-fda-all-ndcs-delta-test
```

**Expected Output**:
```
================================================================================
🧪 Simplified Delta Lake Confidence Test
================================================================================

Scenario: ADD
Run Date: 2050-01-01
...

================================================================================
📊 Test Result
================================================================================
✅ TEST PASSED
Scenario 'add' validated successfully!
```

## Safety Features

The test uses:
- **Canary NDCs**: Starting with `99999` (never exist in real FDA data)
- **Far-future dates**: 2050-XX-XX (safe from production data)
- **Status filtering**: Only touch canary records during validation
- **Non-destructive**: Never drops or truncates tables

## Test Data Cleanup

**Option A**: Leave canary data (recommended)
- Safe because of `99999` prefix + future dates
- Allows re-running tests for comparison
- Minimal storage cost

**Option B**: Manual cleanup (if needed)
```sql
-- Delete canary records from Gold
DELETE FROM pp_dw_gold.fda_all_ndcs
WHERE ndc_11 LIKE '99999%';

-- Delete canary partitions from Silver
-- (Use S3 console or aws s3 rm)
```

## Validation Queries

Run these in Athena to inspect test results:

```sql
-- View canary records
SELECT *
FROM pp_dw_gold.fda_all_ndcs
WHERE ndc_11 LIKE '99999%'
ORDER BY ndc_11, active_from;

-- Status distribution
SELECT status, COUNT(*) as count
FROM pp_dw_gold.fda_all_ndcs
WHERE ndc_11 LIKE '99999%'
GROUP BY status;

-- Timeline view
SELECT
    ndc_11,
    proprietary_name,
    strength,
    active_from,
    active_to,
    status,
    version_id
FROM pp_dw_gold.fda_all_ndcs
WHERE ndc_11 LIKE '99999%'
ORDER BY ndc_11, active_from;
```

## Troubleshooting

### Test fails with "Table not found"
- Ensure Silver crawler has run and registered the test partition
- Check S3 for data: `s3://{bucket}/silver/fda-all-ndcs/run_date=2050-01-01/`

### Current count doesn't match expectations
- Check if canary data from previous test runs exists
- Review Gold job logs for errors
- Validate temporal versioning logic

### Gold job fails
- Check CloudWatch logs: `/aws-glue/jobs/output`
- Verify Delta Lake configuration (Glue 5.0, `--datalake-formats=delta`)
- Ensure temporal versioning library is deployed

### Athena queries timeout
- Use `LIMIT` clause for large tables
- Check S3 bucket permissions
- Verify Glue catalog is up to date

## Next Steps

After successful testing:
1. ✅ Integrate into CI/CD pipeline
2. ✅ Add to daily/weekly monitoring
3. ✅ Set up CloudWatch alarms for test failures
4. ✅ Create dashboard for test metrics
5. ✅ Document expected runtimes and costs

## Cost Estimate

**Per test run**:
- Glue Python Shell: $0.44/DPU-hour × 0.17 hours ≈ $0.07
- Athena queries: ~$0.01 (minimal data scanned)
- S3 storage: Negligible (< 1MB)
- **Total**: ~$0.10 per full test run

**Frequency**:
- Daily: ~$3/month
- Weekly: ~$0.50/month
- On-demand: Pay only when testing

## See Also

- `SIMPLIFIED_TEST_PLAN.md` - Test design philosophy
- `../DELTA_TABLE_REGISTRATION.md` - Delta Lake architecture
- `../../../CLAUDE.md` - ETL architecture overview
