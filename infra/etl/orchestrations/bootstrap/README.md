# ETL Bootstrap Orchestration

**One-time initialization orchestration** for bootstrapping the data warehouse. This runs all bronze → silver → gold jobs sequentially, one time. NOT for daily production updates.

Automated orchestration for running ETL jobs across the bronze, silver, and gold medallion layers with intelligent table existence checking and crawler management.

## Architecture

The orchestration system uses AWS Step Functions to coordinate:

1. **Bronze Layer**: All jobs run in parallel, then crawlers are executed if needed
2. **Silver Layer**: Jobs run sequentially with table checks and crawlers between each job (handles dependencies)
3. **Gold Layer**: Jobs run in sequence (in a specific order), then crawlers are executed if needed

```
┌─────────────────────────────────────────────────────────────┐
│  Start ETL Orchestration                                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE: Parallel Job Execution                             │
│  - fda-nsde, fda-cder, rxnorm, rxnorm-spl-mappings          │
│  - rxclass                                                   │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Check Bronze Tables Exist                                  │
│  - Lambda: glue:GetTables                                   │
└────────┬──────────────────────────────┬─────────────────────┘
         │ Tables exist                 │ Missing tables
         │                              ▼
         │                    ┌─────────────────┐
         │                    │  Run Crawlers   │
         │                    └────────┬────────┘
         │                             │
         └─────────────────┬───────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER: Sequential Job Execution with Table Checks         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 1. rxnorm-products job                                │  │
│  │    ↓ Check table → Run crawler if needed             │  │
│  │ 2. rxclass-drug-members job (depends on #1)          │  │
│  │    ↓ Check table → Run crawler if needed             │  │
│  │ 3. rxnorm-ndc-mappings job                            │  │
│  │    ↓ Check table → Run crawler if needed             │  │
│  │ 4. fda-all-ndcs job                                   │  │
│  │    ↓ Check table → Run crawler if needed             │  │
│  └───────────────────────────────────────────────────────┘  │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD: Sequential Job Execution (in order)                  │
│  1. rxnorm-products                                         │
│  2. rxnorm-product-classifications                          │
│  3. rxnorm-ndc-mappings                                     │
│  4. fda-all-ndcs                                            │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Check Gold Tables Exist                                    │
│  - Lambda: glue:GetTables                                   │
└────────┬──────────────────────────────┬─────────────────────┘
         │ Tables exist                 │ Missing tables
         │                              ▼
         │                    ┌─────────────────┐
         │                    │  Run Crawlers   │
         │                    └────────┬────────┘
         │                             │
         └─────────────────┬───────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Success: All ETL jobs and crawlers completed              │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 1. Lambda: `check-tables.js`
Verifies if expected tables exist in the Glue Data Catalog.

**Input:**
```json
{
  "database": "pp_dw_bronze",
  "tables": ["fda_nsde", "fda_cder", "rxnconso", "rxnrel", "rxnsat"]
}
```

**Output:**
```json
{
  "statusCode": 200,
  "exists": true,
  "missingTables": [],
  "existingTables": ["fda_nsde", "fda_cder", "rxnconso", "rxnrel", "rxnsat"],
  "totalExpected": 5,
  "totalExisting": 5
}
```

### 2. Step Functions: `state-machine.json`
Orchestrates the entire ETL pipeline with:
- Parallel job execution for bronze layer
- Sequential job execution for silver layer with table checks and crawlers between each job (handles dependencies)
- Sequential job execution for gold layer (in specified order) with table checks and crawlers at the end
- Table existence validation after each job (silver) or after all jobs (bronze/gold)
- Automatic crawler invocation when tables are missing
- Synchronous job execution (waits for job completion)

### 3. CDK Stack: `EtlOrchestrationStack.js`
Deploys:
- Lambda function with Glue Data Catalog permissions
- Step Functions state machine
- IAM roles for Lambda and Step Functions
- Proper error handling and logging configuration

## Deployment

The bootstrap orchestration stack is deployed with all ETL infrastructure:

```bash
cd infra/etl
cdk deploy pp-dw-etl-bootstrap-orchestration
```

Or deploy all ETL infrastructure (this includes bootstrap):

```bash
npm run etl:deploy  # From repository root
```

## Running the Bootstrap Orchestration (One Time)

### Option 1: AWS Console (Recommended for first bootstrap)
1. Navigate to Step Functions in AWS Console
2. Select `pp-dw-etl-bootstrap-orchestration` state machine
3. Click "Start execution"
4. Paste the execution input JSON (see below)
5. Click "Start execution"
6. Monitor progress in the visual state diagram

### Option 2: AWS CLI
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:pp-dw-etl-bootstrap-orchestration \
  --input file://bootstrap-execution-input.json \
  --region us-east-1
```

### Option 3: Using Node.js/TypeScript (Recommended)
Create a helper script:

```javascript
const { StepFunctionsClient, StartExecutionCommand } = require("@aws-sdk/client-sfn");

const sfn = new StepFunctionsClient({ region: "us-east-1" });

// Import the orchestration stack to get the execution input helper
const { EtlOrchestrationStack } = require("./orchestration/EtlOrchestrationStack");

// In your Lambda or application code:
async function runEtlPipeline() {
  const orchestrationStack = new EtlOrchestrationStack(null, "temp", { etlCoreStack: {} });
  const executionInput = orchestrationStack.generateExecutionInput();

  const command = new StartExecutionCommand({
    stateMachineArn: process.env.STATE_MACHINE_ARN,
    input: JSON.stringify(executionInput),
    name: `etl-execution-${Date.now()}`
  });

  const result = await sfn.send(command);
  console.log("Execution started:", result.executionArn);
  return result;
}
```

## Execution Input Format

The state machine requires specific inputs for job and crawler names. Generate via the orchestration stack:

```javascript
const { EtlOrchestrationStack } = require("./orchestration/EtlOrchestrationStack");

// Inside your deployment context:
const input = new EtlOrchestrationStack(...).generateExecutionInput({
  // Optional overrides for specific job/crawler names
  bronzeJobName_fda_nsde: "custom-bronze-fda-nsde-job-name"
});

console.log(JSON.stringify(input, null, 2));
```

This generates:
```json
{
  "bronzeJobName_fda_nsde": "pp-dw-bronze-fda-nsde",
  "bronzeJobName_fda_cder": "pp-dw-bronze-fda-cder",
  "bronzeDatabase": "pp_dw_bronze",
  "bronzeExpectedTables": ["fda_nsde", "fda_cder", "rxnconso", "rxnrel", "rxnsat", ...],
  "silverJobName_rxnorm_products": "pp-dw-silver-rxnorm-products",
  "silverJobName_rxnorm_ndc_mappings": "pp-dw-silver-rxnorm-ndc-mappings",
  "silverJobName_fda_all_ndc": "pp-dw-silver-fda-all-ndc",
  "silverDatabase": "pp_dw_silver",
  "silverExpectedTables": ["rxnorm_products", "rxnorm_ndc_mappings", "fda_all_ndc"],
  "goldJobName_rxnorm_products": "pp-dw-gold-rxnorm-products",
  "goldJobName_rxnorm_product_classifications": "pp-dw-gold-rxnorm-product-classifications",
  "goldJobName_rxnorm_ndc_mappings": "pp-dw-gold-rxnorm-ndc-mappings",
  "goldJobName_fda_all_ndcs": "pp-dw-gold-fda-all-ndcs",
  "goldDatabase": "pp_dw_gold",
  "goldExpectedTables": ["rxnorm_products", "rxnorm_product_classifications", "rxnorm_ndc_mappings", "fda_all_ndcs"]
}
```

## Monitoring

### Step Functions Console
Track execution progress with visual state diagram:
- See which jobs are running in parallel
- Monitor job status transitions
- View error details if any step fails
- Check execution history

### CloudWatch Logs
All Lambda executions and Step Functions state transitions logged:
```bash
# View Lambda logs
aws logs tail /aws/lambda/pp-dw-etl-check-tables --follow

# View Step Functions logs
aws logs tail /aws/states/pp-dw-etl-orchestration --follow
```

### Error Handling

The state machine includes:
- **Job timeout handling**: Jobs wait up to 120 minutes (configurable in `orchestration-config.json`)
- **Crawler wait loops**: Polling every 10 seconds with 30-minute timeout
- **Missing table detection**: Automatically invokes crawlers when tables don't exist
- **Graceful retry**: Failed jobs produce detailed error messages in execution history

## Configuration

Edit `orchestration-config.json` to adjust:

```json
{
  "crawler_wait_time_seconds": 300,          // How long to wait before checking crawler status
  "max_crawler_wait_minutes": 30,            // Max time to wait for crawler completion
  "glue_job_poll_interval_seconds": 30,      // Job status poll interval (unused - using .sync)
  "max_glue_job_wait_minutes": 120           // Max time to wait for job completion
}
```

## Customization

### Adding New Datasets

To add new bronze/silver/gold jobs to orchestration:

1. **Update `config.json` in the orchestration directory:**
   ```json
   {
     "bronze_jobs": [..., "new-dataset"],
     "silver_jobs_sequence": [..., "new-silver-dataset"],
     "gold_jobs_sequence": [..., "new-gold-dataset"]
   }
   ```

2. **Deploy the updated stack:**
   ```bash
   cd infra/etl
   cdk deploy pp-dw-etl-bootstrap-orchestration
   ```

The state machine will be automatically regenerated with the new jobs in the correct execution order. No manual state machine edits required!

### Changing Job Execution Order

**Silver Layer:** Edit the `silver_jobs_sequence` array in `config.json` to change the order jobs execute. Jobs will run sequentially in the order listed, with table checks and crawlers between each job to handle dependencies.

**Gold Layer:** Edit the `gold_jobs_sequence` array in `config.json` to change the order jobs execute. Jobs will run sequentially in the order listed.

After changing the order, redeploy the stack:
```bash
cd infra/etl
cdk deploy pp-dw-etl-bootstrap-orchestration
```

## Troubleshooting

### State Machine Fails at Table Check
- Verify Lambda has `glue:GetTables` permission
- Ensure database names are correct in execution input
- Check CloudWatch Logs for Lambda errors

### Crawler Hangs
- Verify crawler has proper S3 path permissions
- Check Glue crawler IAM role has S3 read permissions
- Review crawler configuration for data format issues

### Jobs Fail
- Check Glue job CloudWatch logs
- Verify Glue IAM role has required S3 and Glue permissions
- Review job arguments in stack definitions

### Missing Tables After Job Completion
- Verify S3 data was actually written by jobs
- Check crawler S3 target paths match job output paths
- Manually run crawler to verify it can detect tables

## Cost Optimization

The orchestration system is cost-efficient:

- **Step Functions**: ~$0.000025 per state transition (minimal cost for orchestration)
- **Lambda**: Invoked only once per medallion layer (~3 invocations per full run)
- **No continuous monitoring**: Uses synchronous job execution, not polling
- **Parallel execution**: Reduces total runtime by running independent jobs together

## Future Enhancements

Potential improvements:

1. **Event-driven triggers**: Invoke via S3 events or EventBridge schedules
2. **Partial execution**: Start from specific medallion layer
3. **Retry logic**: Automatic retry on transient failures
4. **Notifications**: SNS alerts on success/failure
5. **Metrics**: CloudWatch metrics for execution times
6. **Rollback**: Automated cleanup on failure
7. **Multi-region**: Fan-out execution across regions
