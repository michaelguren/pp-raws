# NSDE (Comprehensive NDC SPL Data Elements) ETL Pipeline

Simplified ETL pipeline for FDA NSDE data with manual testing approach.

## Architecture

Simplified components for faster iteration:

1. **Lambda Function** - Downloads and extracts FDA data to S3
2. **Glue Jobs** - Process data transformations (Bronze → Silver)  
3. **Manual Orchestration** - Use AWS CLI to trigger components individually
4. **Config-driven** - All settings centralized in `config/dataset.json`

## Flow

```
Manual: aws lambda invoke → Download FDA data to S3
  ↓
Manual: aws glue start-job-run → Bronze Job (Raw CSV to Parquet)
  ↓  
Manual: aws glue start-job-run → Silver Job (Cleansing, normalization)
```

## Directory Structure

```
nsde/
├── NsdeStack.js                 # CDK stack definition
├── lambdas/
│   └── fetch_and_hash/
│       └── app.py               # Download & hash Lambda
├── glue/
│   ├── bronze_job.py            # Bronze ETL processor
│   └── silver_job.py            # Silver ETL processor
├── config/
│   └── dataset.json             # Pipeline configuration
└── README.md                    # This file
```

## Configuration

All pipeline settings are in `config/dataset.json`:

```json
{
  "dataset": "nsde",
  "source_url": "https://download.open.fda.gov/Comprehensive_NDC_SPL_Data_Elements_File.zip",
  "bronze_job_name": "nsde-bronze-etl",
  "silver_job_name": "nsde-silver-etl",
  "bronze_crawler_name": "nsde-bronze-crawler",
  "database_name": "nsde_db"
}
```

## Deployment

Deploy the simplified stack:

```bash
cdk deploy
```

## Testing

Test components individually using AWS CLI:

### 1. Invoke Fetch Lambda

```bash
# Get Lambda function name from CDK output
aws lambda invoke \
  --function-name nsde-fetch-lambda \
  --payload '{"dataset":"nsde","bucket":"BUCKET_NAME","force":false}' \
  output.json

# View the output
cat output.json
```

### 2. Run Bronze Job

```bash
# Use run_id from fetch output
aws glue start-job-run \
  --job-name nsde-bronze-etl \
  --arguments '{
    "--raw_path":"s3://BUCKET_NAME/raw/nsde/RUN_ID/",
    "--run_id":"RUN_ID", 
    "--dataset":"nsde",
    "--bronze_path":"s3://BUCKET_NAME/bronze/nsde/run=RUN_ID/"
  }'
```

### 3. Check Job Status

```bash
# Get job run ID from previous command output
aws glue get-job-run \
  --job-name nsde-bronze-etl \
  --run-id JOB_RUN_ID
```

### 4. Run Silver Job (after Bronze succeeds)

```bash
aws glue start-job-run \
  --job-name nsde-silver-etl \
  --arguments '{
    "--bronze_path":"s3://BUCKET_NAME/bronze/nsde/run=RUN_ID/",
    "--run_id":"RUN_ID",
    "--dataset":"nsde"
  }'
```

### Alternative: Use AWS Console

- Lambda: AWS Console → Lambda → Functions → nsde-fetch-lambda → Test
- Glue: AWS Console → Glue → Jobs → nsde-bronze-etl → Run job

## Data Processing

### Bronze Layer
- Minimal ingestion: Raw CSV → Parquet with ZSTD compression
- Schema inference
- Basic metadata: ingest timestamp, source SHA256, run ID
- Partitioned by `run`

### Silver Layer
- Full cleansing and normalization
- NDC11 normalization to 11-digit format
- Text field standardization (trim, uppercase)
- Date parsing and validation
- Deduplication based on record hash
- Data quality checks with configurable thresholds
- Partitioned by `version` and `version_date`