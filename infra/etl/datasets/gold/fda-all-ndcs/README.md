# Gold FDA All NDC Dataset

**Layer**: Gold (Curated, Temporally Versioned)
**Source**: `pp_dw_silver.fda_all_ndc`
**Table**: `pp_dw_gold.fda_all_ndc`
**Partitioning**: By `status` (current/historical)

---

## Overview

This dataset applies **temporal versioning** (SCD Type 2 pattern) to FDA drug data from the silver layer. It tracks changes over time using `active_from` and `active_to` dates, enabling:

- **Change detection**: Identify what changed on any given date
- **Historical queries**: View drug information as it existed at any point in time
- **Incremental syncs**: Sync only changed records to DynamoDB
- **Audit trail**: Complete history of all drug record modifications

### Key Design Decisions

1. **End-of-time pattern**: Current records have `active_to = '9999-12-31'` (not NULL)
2. **Status partitioning**: Data split into `status=current/` and `status=historical/` for query efficiency
3. **Dropped prefixes**: Column names no longer include `fda_` prefix (table name indicates source)
4. **Change detection**: MD5 hash (`content_hash`) of business columns identifies modifications

---

## Schema

### Version Tracking Columns
| Column | Type | Description |
|--------|------|-------------|
| `version_id` | STRING | UUID for this specific version |
| `content_hash` | STRING | MD5 hash of business columns (for change detection) |
| `active_from` | DATE | When this version became active |
| `active_to` | DATE | When this version expired (`9999-12-31` = current) |
| `status` | STRING | Partition: `current` or `historical` |

### Business Key
| Column | Type | Description |
|--------|------|-------------|
| `ndc_11` | STRING | 11-digit NDC code (primary identifier) |

### Business Columns (FDA Data)
| Column | Type | Description |
|--------|------|-------------|
| `ndc_5` | STRING | 5-digit labeler code |
| `marketing_category` | STRING | NDA, ANDA, OTC, etc. |
| `product_type` | STRING | HUMAN PRESCRIPTION DRUG, etc. |
| `proprietary_name` | STRING | Brand/trade name |
| `dosage_form` | STRING | TABLET, CAPSULE, INJECTION, etc. |
| `application_number` | STRING | NDA/ANDA number |
| `dea_schedule` | STRING | DEA controlled substance schedule (I-V) |
| `package_description` | STRING | Package size and type |
| `active_numerator_strength` | STRING | Active ingredient strength |
| `active_ingredient_unit` | STRING | Unit of measurement (mg, mL, etc.) |
| `spl_id` | STRING | Structured Product Labeling ID |
| `marketing_start_date` | DATE | When drug started marketing |
| `marketing_end_date` | DATE | When drug stopped marketing |
| `billing_unit` | STRING | Billing unit code |
| `nsde_flag` | BOOLEAN | TRUE if in NSDE dataset |

### Metadata Columns
| Column | Type | Description |
|--------|------|-------------|
| `source_file` | STRING | Source table name (e.g., `silver.fda_all_ndc`) |
| `source_run_id` | STRING | Silver layer run ID (lineage tracking) |
| `run_date` | STRING | When this version was created (YYYY-MM-DD) |
| `run_id` | STRING | Gold layer run ID |

---

## Common Queries

### 1. Get All Current Drugs

```sql
SELECT *
FROM pp_dw_gold.fda_all_ndc
WHERE status = 'current';
```

**Performance**: Scans only the `status=current/` partition (~380K records)

---

### 2. Get Current Drugs with Specific Criteria

```sql
-- Active controlled substances (Schedule II-V)
SELECT ndc_11, proprietary_name, dea_schedule, marketing_start_date
FROM pp_dw_gold.fda_all_ndc
WHERE status = 'current'
  AND dea_schedule IS NOT NULL
  AND active_to = '9999-12-31'
ORDER BY dea_schedule, proprietary_name;
```

---

### 3. Point-in-Time Query (Historical View)

```sql
-- What did Prozac (NDC 00002322702) look like on January 1, 2024?
SELECT *
FROM pp_dw_gold.fda_all_ndc
WHERE ndc_11 = '00002322702'
  AND '2024-01-01' BETWEEN active_from AND active_to;
```

---

### 4. Get All Historical Versions of a Drug

```sql
-- See all changes to Prozac over time
SELECT
  version_id,
  ndc_11,
  proprietary_name,
  dosage_form,
  active_from,
  active_to,
  status,
  CASE
    WHEN active_to = '9999-12-31' THEN 'CURRENT'
    ELSE 'EXPIRED'
  END as version_status
FROM pp_dw_gold.fda_all_ndc
WHERE ndc_11 = '00002322702'
ORDER BY active_from DESC;
```

---

### 5. Get Changes for a Specific Date (Incremental Sync)

```sql
-- Get all records that changed on October 14, 2025
SELECT *
FROM pp_dw_gold.fda_all_ndc
WHERE run_date = '2025-10-14'
  AND (
    active_from = '2025-10-14'  -- New or updated records
    OR (active_to = '2025-10-14' AND active_to < '9999-12-31')  -- Expired records
  );
```

**Use case**: DynamoDB incremental sync - returns only ~50-500 records/day

---

### 6. Find Drugs That Were Discontinued (Expired)

```sql
-- Drugs that were discontinued in the last 30 days
SELECT
  ndc_11,
  proprietary_name,
  marketing_end_date,
  active_to as discontinued_date
FROM pp_dw_gold.fda_all_ndc
WHERE active_to >= CURRENT_DATE - INTERVAL '30' DAY
  AND active_to < '9999-12-31'
  AND status = 'historical'
ORDER BY active_to DESC;
```

---

### 7. Data Quality Check (Validate Temporal Integrity)

```sql
-- Check for drugs with multiple current versions (should be zero)
SELECT ndc_11, COUNT(*) as current_count
FROM pp_dw_gold.fda_all_ndc
WHERE active_to = '9999-12-31'
GROUP BY ndc_11
HAVING COUNT(*) > 1;
```

---

### 8. Summary Statistics

```sql
-- Count by status
SELECT
  status,
  COUNT(*) as record_count,
  COUNT(DISTINCT ndc_11) as unique_drugs
FROM pp_dw_gold.fda_all_ndc
GROUP BY status;
```

---

## Deployment

### Deploy Infrastructure

```bash
cd infra/etl
cdk deploy pp-dw-etl-gold-fda-all-ndc
```

### Run Glue Job

```bash
# Option 1: Via AWS CLI
aws glue start-job-run --job-name pp-dw-gold-fda-all-ndc

# Option 2: Via AWS Console
# Navigate to: Glue > ETL Jobs > pp-dw-gold-fda-all-ndc > Run job
```

### Run Crawler (After Job Completes)

```bash
aws glue start-crawler --name pp-dw-gold-fda-all-ndc-crawler

# Wait for completion
aws glue get-crawler --name pp-dw-gold-fda-all-ndc-crawler
```

---

## Data Flow

```
Silver Layer (pp_dw_silver.fda_all_ndc)
  │
  │ [Glue Job: pp-dw-gold-fda-all-ndc]
  ↓
Gold Layer (S3: s3://pp-dw-{account}/gold/fda-all-ndc/)
  ├── status=current/
  │   └── *.parquet (380K records)
  └── status=historical/
      └── *.parquet (grows over time)
  │
  │ [Crawler: pp-dw-gold-fda-all-ndc-crawler]
  ↓
Glue Catalog (pp_dw_gold.fda_all_ndc)
  │
  ↓
Queryable via Athena
```

---

## Performance

### Initial Load
- **Input**: 380K silver records
- **Output**: 380K gold records (all `status=current`)
- **Runtime**: ~5 minutes (10 G.2X workers)

### Incremental Run (Typical Daily)
- **Changed records**: ~50-500/day (0.1-0.2%)
- **Output**: ~100-1000 records (expired + new versions)
- **Runtime**: ~2-3 minutes

### Query Performance
- **Current records**: Fast (single partition scan)
- **Historical queries**: Depends on date range
- **Point-in-time**: Fast (filtered on business key + date)

---

## Cost Estimate

### Storage
- **Current**: ~100 MB (380K records @ 270 bytes/record)
- **Historical growth**: ~1 GB/year (assuming 0.2% daily change rate)

### Glue Job
- **Initial load**: $0.44 (10 workers × 5 min)
- **Daily incremental**: $0.18 (10 workers × 2 min)
- **Monthly**: ~$5.40

### Athena Queries
- **Current drugs**: ~$0.0005/query (scans 100 MB)
- **Historical queries**: Varies by date range

**Total monthly cost**: ~$6/month

---

## Maintenance

### Validate Temporal Integrity

Run validation queries periodically to ensure data quality:

```sql
-- 1. Check for duplicate current records
SELECT ndc_11, COUNT(*)
FROM pp_dw_gold.fda_all_ndc
WHERE status = 'current'
GROUP BY ndc_11
HAVING COUNT(*) > 1;

-- 2. Verify status matches active_to
SELECT COUNT(*)
FROM pp_dw_gold.fda_all_ndc
WHERE (active_to = '9999-12-31' AND status != 'current')
   OR (active_to < '9999-12-31' AND status != 'historical');

-- 3. Check for gaps in history
-- (No records should have overlapping or missing date ranges)
```

### Reprocess Historical Data (If Needed)

To rebuild the entire gold table:

```bash
# 1. Delete existing gold data
aws s3 rm s3://pp-dw-{account}/gold/fda-all-ndc/ --recursive

# 2. Delete Glue table
aws glue delete-table --database-name pp_dw_gold --name fda_all_ndc

# 3. Re-run gold job (will treat as initial load)
aws glue start-job-run --job-name pp-dw-gold-fda-all-ndc

# 4. Re-run crawler
aws glue start-crawler --name pp-dw-gold-fda-all-ndc-crawler
```

---

## Future Enhancements

- **Automated scheduling**: EventBridge rule to run after silver layer completes
- **Data quality alerts**: SNS notifications for validation failures
- **Partition pruning**: Add year/month sub-partitions if historical data grows large
- **Compaction**: Periodic S3 file consolidation to reduce small file overhead
