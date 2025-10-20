# Daily FDA Orchestration

**Daily ETL pipeline for FDA drug data** - Runs fda-nsde and fda-cder datasets through bronze → silver → gold layers with automated crawler management.

## Data Flow

```
Bronze (Parallel):
  - fda-nsde job
  - fda-cder job
  → Check tables → Run crawlers if needed

Silver (Sequential):
  - fda-all-ndcs job (joins fda-nsde + fda-cder)
  → Check table → Run crawler if needed

Gold (Sequential):
  - fda-all-ndcs job (temporal versioning)
  → Check table → Run crawler if needed

Success
```

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
