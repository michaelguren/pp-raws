# NSDE (Comprehensive NDC SPL Data Elements) ETL Pipeline

This directory contains the complete ETL pipeline for FDA NSDE data using Step Functions orchestration.

## Architecture

The pipeline follows the domain-first, orchestrator-driven pattern:

1. **Step Functions Orchestrator** - Coordinates the entire ETL flow
2. **Lambda Functions** - Handle data fetching, hashing, and manifest updates
3. **Glue Jobs** - Process data transformations (Bronze → Silver)
4. **Config-driven** - All settings centralized in `config/dataset.json`

## Flow

```
Input: { "dataset": "nsde", "source_url": "https://...", "force": false }
  ↓
FetchAndHash Lambda → Download, compute SHA256, compare manifest
  ↓
Choice: Skip if unchanged & !force, else continue
  ↓
Bronze Glue Job → Raw CSV to Parquet (minimal processing)
  ↓
Bronze Crawler (optional) → Update Glue catalog
  ↓
Silver Glue Job → Cleansing, normalization, deduplication
  ↓
UpdateManifest Lambda → Update manifests/nsde.json
  ↓
Success
```

## Directory Structure

```
nsde/
├── cdk/
│   └── NsdeOrchestratorStack.ts  # CDK stack definition
├── lambdas/
│   ├── fetch_and_hash/
│   │   └── app.py               # Download & hash Lambda
│   └── update_manifest/
│       └── app.py               # Manifest update Lambda
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

Deploy the NSDE orchestrator stack:

```bash
cdk deploy PP-RAWS-NSDE-Orchestrator
```

## Execution

Trigger the pipeline manually via the trigger Lambda or Step Functions console:

```json
{
  "dataset": "nsde",
  "source_url": "https://download.open.fda.gov/Comprehensive_NDC_SPL_Data_Elements_File.zip",
  "force": false
}
```

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