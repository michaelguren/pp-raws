# Daily FDA Orchestration

**Daily ETL pipeline for FDA drug data** - Runs fda-nsde and fda-cder datasets through bronze → silver → gold layers with automated crawler management.

## Orchestration Architecture

### Control-Plane vs. Data-Plane Separation

**Core Principle**: Individual Glue jobs are ignorant of crawlers and Glue Catalog; orchestration coordinates the catalog.

- **Data-Plane** (Glue Jobs): Read data → Transform → Write to S3
- **Control-Plane** (Step Functions + Lambdas): Check catalog → Start crawlers → Coordinate dependencies

This separation ensures jobs are testable, portable, and catalog-agnostic.

### Step Functions Flow

```
START
  │
  ├─→ [BRONZE LAYER - Parallel]
  │   ├─→ Run fda-nsde Glue job
  │   │   └─→ Writes to s3://pp-dw-{account}/bronze/fda-nsde/
  │   │
  │   └─→ Run fda-cder Glue job
  │       └─→ Writes to s3://pp-dw-{account}/bronze/fda-cder/
  │
  ├─→ Wait for both jobs to complete
  │
  ├─→ Check Bronze Tables (Lambda: check-tables.js)
  │   └─→ Returns list of missing tables
  │
  ├─→ Start Bronze Crawlers (Lambda: start-crawlers.js)
  │   └─→ Invokes crawlers only for missing tables
  │
  ├─→ Wait 30 seconds for crawlers to complete
  │
  ├─→ [SILVER LAYER - Sequential]
  │   └─→ Run fda-all-ndcs Glue job
  │       └─→ Reads from bronze.fda_nsde + bronze.fda_cder (via Glue catalog)
  │       └─→ Writes to s3://pp-dw-{account}/silver/fda-all-ndcs/
  │
  ├─→ Check Silver Table (Lambda: check-tables.js)
  │   └─→ Returns list of missing tables
  │
  ├─→ Start Silver Crawler (Lambda: start-crawlers.js)
  │   └─→ Invokes crawler only if table missing
  │
  ├─→ Wait 30 seconds for crawler to complete
  │
  ├─→ [GOLD LAYER - Sequential]
  │   └─→ Run fda-all-ndcs Glue job (Delta Lake)
  │       └─→ Reads from silver.fda_all_ndcs (via Glue catalog)
  │       └─→ Writes to s3://pp-dw-{account}/gold/fda-all-ndcs/ (Delta format)
  │
  ├─→ Check Gold Table (Lambda: check-tables.js)
  │   └─→ Returns list of missing tables
  │
  ├─→ Start Gold Crawler (Lambda: start-crawlers.js)
  │   └─→ Invokes crawler only if table missing
  │
  ├─→ Wait 30 seconds for crawler to complete
  │
  └─→ SUCCESS
```

**Key Characteristics**:
1. Jobs write data to S3 and exit (no catalog awareness)
2. Lambdas check if tables exist in Glue catalog
3. Crawlers run only when tables are missing (not on every run)
4. Each layer waits for upstream tables to be registered before proceeding
5. Bronze runs in parallel; Silver and Gold are sequential (dependencies)

**Crawler Invocation Logic** (check-tables.js):
```javascript
// Check if tables exist in Glue catalog
const missingTables = await checkTables(database, expectedTables);

// Only start crawlers for missing tables
if (missingTables.length > 0) {
  await startCrawlers(missingTables);
}
```

This ensures crawlers run once (initial setup) and are skipped on subsequent runs.

## Deployment

```bash
cd infra/etl
cdk deploy pp-dw-daily-fda-orchestration
```

## Running

### Manual Execution

1. Navigate to Step Functions in AWS Console
2. Select `pp-dw-daily-fda-orchestration` state machine
3. Click "Start execution"
4. Use empty input: `{}`
5. Monitor progress in visual diagram

### AWS CLI

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:pp-dw-daily-fda-orchestration \
  --input '{}' \
  --region us-east-1
```

## Scheduling

To enable daily automatic execution, update `config.json`:

```json
{
  "schedule": {
    "enabled": true,
    "expression": "cron(0 6 * * ? *)",
    "description": "Daily at 6am UTC (1am EST, 10pm PST)"
  }
}
```

Then redeploy the stack. The EventBridge schedule will trigger the state machine daily.

## Datasets Processed

### Bronze Layer
- **fda-nsde** - FDA National Drug Code Directory (NSDE)
- **fda-cder** - FDA CDER NDC Database

### Silver Layer
- **fda-all-ndcs** - Unified FDA NDC dataset (joins NSDE + CDER)

### Gold Layer
- **fda-all-ndcs** - Temporal versioned NDCs with SCD Type 2 pattern

## Configuration

Edit `config.json` to adjust:

- **bronze_jobs**: Bronze datasets to process
- **silver_jobs_sequence**: Silver jobs (order matters for dependencies)
- **gold_jobs_sequence**: Gold jobs (order matters for dependencies)
- **schedule**: EventBridge schedule configuration
- **crawler_wait_time_seconds**: How long to wait for crawlers (default: 30s)
- **max_glue_job_wait_minutes**: Max time for job completion (default: 120 min)

## Monitoring

### Step Functions Console

Track execution in real-time:
- Visual state diagram shows current step
- Execution history with input/output for each state
- Error details if any step fails

### CloudWatch Logs

```bash
# View Lambda logs
aws logs tail /aws/lambda/pp-dw-daily-fda-check-tables --follow
aws logs tail /aws/lambda/pp-dw-daily-fda-start-crawlers --follow

# View Glue job logs
aws logs tail /aws-glue/jobs/output --follow --filter-pattern "fda-nsde"
```

## Redrive from Failure

If the orchestration fails (e.g., job error, crawler timeout):

1. Fix the underlying issue (code, data, permissions)
2. Redeploy if needed: `cdk deploy pp-dw-daily-fda-orchestration`
3. In Step Functions console, click **"Redrive from failure"**
4. Execution resumes from the failed state

No need to restart from the beginning!

## Features

✅ **Targeted crawler execution** - Only runs crawlers for missing tables
✅ **Parallel bronze jobs** - fda-nsde and fda-cder run simultaneously
✅ **Dependency handling** - Silver waits for bronze tables to exist
✅ **Temporal versioning** - Gold layer tracks historical changes
✅ **Optional scheduling** - EventBridge integration for daily runs
✅ **Redrive support** - Resume from failure without restarting

## Future Enhancements

- Add SNS notifications on success/failure
- CloudWatch metrics for execution times
- Retry logic for transient failures
- Partial re-runs (e.g., silver-only)
