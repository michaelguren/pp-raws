# ETL Strategy & Architecture

## Overview

Our ETL (Extract, Transform, Load) system follows a **domain-first, orchestrator-driven architecture** built on AWS serverless services. The design prioritizes cost efficiency, scalability, and maintainability while supporting multiple healthcare datasets with consistent patterns.

## Core Principles

### 1. Domain-First Organization
- Each dataset (NSDE, CDER, RxNorm) is self-contained in its own domain folder
- Independent deployment and scaling per dataset
- Shared infrastructure and libraries for consistency
- Easy onboarding of new datasets following established patterns

### 2. Orchestrator-Driven Processing
- **Step Functions** coordinate the entire ETL flow
- **Lambda functions** handle orchestration logic (download, hash, manifest)
- **Glue jobs** are "dumb processors" that only transform data
- Clear separation of concerns between orchestration and processing

### 3. Medallion Architecture (Bronze → Silver → Gold)
- **Raw Layer**: Original files stored in S3 with metadata
- **Bronze Layer**: Minimal ingestion with schema inference and compression
- **Silver Layer**: Cleansed, normalized, deduplicated data ready for analytics
- **Gold Layer**: Business-ready aggregations and joins (future)

### 4. Cost-Optimized Serverless
- Pay-per-use model: ~$0.44/hour for Glue, ~$5/TB for Athena queries
- No idle infrastructure costs
- ZSTD compression for storage efficiency
- Intelligent skip logic to avoid unnecessary processing

## Architecture Components

### Step Functions Orchestrator
```
Input → FetchAndHash → Choice → Bronze → Crawler → Silver → UpdateManifest → Success
                         ↓
                      Skip (unchanged)
```

**Responsibilities:**
- Coordinate ETL flow with error handling and retries
- Implement skip/force logic at the orchestration level
- Generate deterministic run IDs for data lineage
- Manage state transitions and notifications

### Lambda Functions

**FetchAndHash (`fetch_and_hash/app.py`)**
- Stream download source data with retry logic
- Compute SHA256 hash for change detection
- Upload raw files to S3 with proper partitioning
- Compare against manifest to determine if processing needed

**UpdateManifest (`update_manifest/app.py`)**
- Update dataset manifest after successful Silver processing
- Track last processed hash, timestamp, and run metadata
- Enable incremental processing and audit trails

### Glue Jobs

**Bronze Job (`bronze_job.py`)**
- **Input:** `--raw_path`, `--run_id`
- **Processing:** Schema inference, minimal metadata, ZSTD compression
- **Output:** Partitioned Parquet in `bronze/{dataset}/run={run_id}/`
- **Philosophy:** Minimal transformation, preserve source fidelity

**Silver Job (`silver_job.py`)**
- **Input:** `--bronze_path`, `--run_id`
- **Processing:** Cleansing, normalization, deduplication, data quality checks
- **Output:** Partitioned Parquet in `silver/{dataset}/version={date}/`
- **Philosophy:** Business-ready data with quality guarantees

### Shared Libraries

**Lambda Libraries (`_shared/lambdas/libs/`)**
- `s3_io.py`: JSON get/put, stream uploads, existence checks
- `hash_utils.py`: SHA256 computation and comparison
- `time_utils.py`: UTC timestamps, deterministic run ID generation

**Glue Libraries (`_shared/glue_lib/`)**
- `io.py`: Read/write helpers for bronze/silver layers
- `schema_utils.py`: Metadata addition, deduplication, validation

## Data Flow Patterns

### 1. Incremental Processing
- Hash-based change detection prevents unnecessary reprocessing
- Manifest tracking enables audit trails and rollback capability
- Run ID partitioning allows parallel processing of different versions

### 2. Schema Evolution
- Bronze layer preserves original schemas with inference
- Silver layer applies consistent normalization patterns
- Schema changes are tracked through version partitioning

### 3. Data Quality Gates
- Configurable quality thresholds in Silver processing
- Fail-fast on critical data quality issues
- Comprehensive logging and CloudWatch metrics

### 4. Error Handling & Recovery
- Automatic retries with exponential backoff
- Dead letter queues for failed processing
- Granular error tracking and alerting

## Configuration-Driven Deployment

Each dataset follows a consistent configuration pattern in `config/dataset.json`:

```json
{
  "dataset": "nsde",
  "source_url": "https://...",
  "bronze_job_name": "nsde-bronze-etl",
  "silver_job_name": "nsde-silver-etl",
  "bronze_crawler_name": "nsde-bronze-crawler",
  "database_name": "nsde_db",
  "glue_version": "4.0",
  "worker_type": "G.1X",
  "number_of_workers": 10,
  "data_quality": {
    "critical_null_threshold": 0.1,
    "required_columns": ["ndc11"]
  }
}
```

## Deployment & Operations

### Independent Dataset Deployment
```bash
# Deploy shared infrastructure once
cdk deploy PP-RAWS-DataLake-Base

# Deploy individual datasets independently
cdk deploy PP-RAWS-NSDE-Orchestrator
cdk deploy PP-RAWS-CDER-Orchestrator  # Future
cdk deploy PP-RAWS-RxNorm-Orchestrator  # Future
```

### Manual Execution
```bash
# Trigger via Lambda
aws lambda invoke \
  --function-name nsde-trigger \
  --payload '{"force": false}' \
  output.json

# Direct Step Functions execution
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:... \
  --input '{"dataset": "nsde", "source_url": "https://...", "force": false}'
```

### Monitoring & Observability
- CloudWatch metrics for job success/failure rates
- Step Functions execution history and state visualization
- Glue job logs with structured JSON output
- Data lineage tracking through run IDs and manifests

## Scaling Patterns

### Adding New Datasets
1. Create domain folder: `infra/etl/{organization}/{dataset}/`
2. Copy configuration pattern from existing dataset
3. Implement dataset-specific transformations in Bronze/Silver jobs
4. Deploy independent orchestrator stack

### Performance Optimization
- Adjust Glue worker count based on data volume
- Implement columnar partitioning for query optimization
- Add caching layers for frequently accessed data
- Scale compute resources dynamically based on file sizes

### Cost Management
- Automated lifecycle policies for S3 storage tiers
- Intelligent scheduling to avoid peak pricing
- Compression and format optimization (Parquet + ZSTD)
- Pay-per-use model eliminates idle infrastructure costs

## Future Enhancements

### Near-term
- Gold layer for business aggregations and joins
- Real-time streaming via Kinesis for low-latency updates
- Data catalog integration with AWS Glue for better discovery
- Advanced data quality rules and ML-based anomaly detection

### Long-term
- Multi-region replication for disaster recovery
- Data mesh architecture with domain ownership
- ML feature stores integration
- GraphQL APIs for flexible data access patterns

## Security & Compliance

- **Encryption**: KMS-managed keys for all data at rest and in transit
- **Access Control**: Least-privilege IAM roles with service-specific permissions
- **Audit**: Comprehensive logging of all data access and transformations
- **Data Lineage**: Complete traceability from source to consumption
- **Privacy**: PII handling patterns and data masking capabilities

This ETL strategy provides a foundation for scalable, cost-effective healthcare data processing while maintaining flexibility for future requirements and datasets.