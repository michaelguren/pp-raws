# Drug Product Codesets (Gold Layer)

## Overview

The **drug-product-codesets** dataset is a **gold layer** table that combines FDA NDC data with RxNORM product information via NDC-to-RxCUI mappings. This provides a unified view of drug products with both regulatory (FDA) and clinical terminology (RxNORM) attributes.

## Architecture

```
Silver + Bronze → Gold Job → S3 (gold/) → Crawler → Athena
```

**Input Tables:**
- `pp_dw_silver.fda_all_ndc` - FDA NDC data (NSDE + CDER combined)
- `pp_dw_silver.rxnorm_products` - RxNORM prescribable products
- `pp_dw_bronze.rxnorm_rxnsat` - RXNSAT table for NDC-to-RxCUI mappings

**Output Table:**
- `pp_dw_gold.drug_product_codesets` - Combined FDA + RxNORM data

## Join Strategy

```sql
SELECT
  fda_all_ndc.*,
  rxnorm_products.*
FROM pp_dw_silver.fda_all_ndc
FULL OUTER JOIN pp_dw_bronze.rxnorm_rxnsat
  ON fda_all_ndc.fda_ndc_11 = rxnorm_rxnsat.atv
  AND rxnorm_rxnsat.sab = 'RXNORM'
  AND rxnorm_rxnsat.atn = 'NDC'
FULL OUTER JOIN pp_dw_silver.rxnorm_products
  ON rxnorm_rxnsat.rxcui = rxnorm_products.rxcui
```

**Key Points:**
- **Composite Key:** `fda_ndc_11` OR `rxnorm_rxcui` (either may be NULL)
- **FULL OUTER JOIN:** Preserves ALL FDA NDCs + ALL RxNORM products
- **Bridge Table:** RXNSAT provides NDC-to-RxCUI mappings
- **Match Rate:** ~60-80% of FDA NDCs match to RxNORM (estimated)
- **Result:** 3 categories of records:
  1. FDA only (no RxNORM match)
  2. Both FDA + RxNORM (matched via NDC)
  3. RxNORM only (no FDA NDC)

## Field Naming Conventions

All fields follow strict prefixing rules:

### FDA Fields (fda_* prefix)
All fields from FDA sources are prefixed with `fda_`:

| Field Name | Type | Description |
|------------|------|-------------|
| `fda_ndc_11` | string | 11-digit NDC (primary key) |
| `fda_ndc_5` | string | 5-digit labeler code |
| `fda_marketing_category` | string | Marketing category (e.g., NDA, ANDA) |
| `fda_product_type` | string | Product type (e.g., HUMAN PRESCRIPTION DRUG) |
| `fda_proprietary_name` | string | Brand/proprietary name |
| `fda_dosage_form` | string | Dosage form (e.g., TABLET, CAPSULE) |
| `fda_application_number` | string | FDA application number |
| `fda_dea_schedule` | string | DEA schedule (if controlled substance) |
| `fda_package_description` | string | Package description |
| `fda_active_numerator_strength` | string | Active ingredient strength |
| `fda_active_ingredient_unit` | string | Unit of strength measurement |
| `fda_spl_id` | string | Structured Product Label ID |
| `fda_marketing_start_date` | date | Marketing start date |
| `fda_marketing_end_date` | date | Marketing end date |
| `fda_billing_unit` | string | Billing unit |
| `fda_nsde_flag` | boolean | Presence in NSDE dataset |

### RxNORM Fields (rxnorm_* prefix)
All fields from RxNORM sources are prefixed with `rxnorm_`:

| Field Name | Type | Description |
|------------|------|-------------|
| `rxnorm_rxcui` | string | RxNORM Concept Unique Identifier |
| `rxnorm_tty` | string | Term type (SCD, SBD, GPCK, BPCK) |
| `rxnorm_str` | string | RxNORM concept string |
| `rxnorm_strength` | string | Drug strength |
| `rxnorm_ingredient_names` | string | Active ingredient names (sorted) |
| `rxnorm_brand_names` | string | Brand names (sorted) |
| `rxnorm_dosage_forms` | string | Dosage forms |
| `rxnorm_psn` | string | Prescribable name |
| `rxnorm_sbdf_rxcui` | string | Semantic Branded Dose Form RxCUI |
| `rxnorm_sbdf_name` | string | SBDF name |
| `rxnorm_scdf_rxcui` | string | Semantic Clinical Dose Form RxCUI |
| `rxnorm_scdf_name` | string | SCDF name |
| `rxnorm_sbd_rxcui` | string | Semantic Branded Drug RxCUI |
| `rxnorm_bpck_rxcui` | string | Branded Pack RxCUI |
| `rxnorm_multi_ingredient` | boolean | Multi-ingredient flag |

### Metadata Fields
| Field Name | Type | Description |
|------------|------|-------------|
| `meta_run_id` | string | Run ID from FDA source (lineage tracking) |

## Data Quality

**Expected Record Count:** 100,000+ (all FDA NDCs + all RxNORM products)

**Critical Quality Checks:**
- Each record must have EITHER `fda_ndc_11` OR `rxnorm_rxcui` (or both)
- No duplicate `fda_ndc_11` values (among records with FDA data)
- No duplicate `rxnorm_rxcui` values (among records with RxNORM data)
- RxNORM fields will be NULL for FDA-only records
- FDA fields will be NULL for RxNORM-only records
- `fda_nsde_flag` should be TRUE for all FDA records (INNER JOIN in silver layer)

## Use Cases

### 1. Clinical-Regulatory Mapping
Link FDA NDC codes to RxNORM clinical terminologies for EHR integration:
```sql
SELECT
  fda_ndc_11,
  fda_proprietary_name,
  rxnorm_rxcui,
  rxnorm_ingredient_names,
  rxnorm_psn
FROM pp_dw_gold.drug_product_codesets
WHERE rxnorm_rxcui IS NOT NULL;
```

### 2. Brand-Generic Analysis
Compare FDA brand names with RxNORM brand and generic names:
```sql
SELECT
  fda_ndc_11,
  fda_proprietary_name AS fda_brand,
  rxnorm_brand_names AS rxnorm_brands,
  rxnorm_ingredient_names AS generic_names
FROM pp_dw_gold.drug_product_codesets
WHERE rxnorm_tty IN ('SBD', 'SCD');
```

### 3. Dosage Form Reconciliation
Identify discrepancies between FDA and RxNORM dosage forms:
```sql
SELECT
  fda_ndc_11,
  fda_dosage_form,
  rxnorm_dosage_forms
FROM pp_dw_gold.drug_product_codesets
WHERE fda_dosage_form != rxnorm_dosage_forms
  AND rxnorm_dosage_forms IS NOT NULL;
```

### 4. Coverage Reporting
Measure matching rates across datasets:
```sql
SELECT
  COUNT(*) AS total_records,
  SUM(CASE WHEN fda_ndc_11 IS NOT NULL AND rxnorm_rxcui IS NULL THEN 1 ELSE 0 END) AS fda_only,
  SUM(CASE WHEN fda_ndc_11 IS NOT NULL AND rxnorm_rxcui IS NOT NULL THEN 1 ELSE 0 END) AS matched,
  SUM(CASE WHEN fda_ndc_11 IS NULL AND rxnorm_rxcui IS NOT NULL THEN 1 ELSE 0 END) AS rxnorm_only,
  ROUND(100.0 * SUM(CASE WHEN fda_ndc_11 IS NOT NULL AND rxnorm_rxcui IS NOT NULL THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN fda_ndc_11 IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS fda_match_pct
FROM pp_dw_gold.drug_product_codesets;
```

### 5. Identify Unmapped Products
Find RxNORM products without FDA NDC coverage:
```sql
SELECT
  rxnorm_rxcui,
  rxnorm_str,
  rxnorm_tty,
  rxnorm_ingredient_names
FROM pp_dw_gold.drug_product_codesets
WHERE fda_ndc_11 IS NULL
  AND rxnorm_tty IN ('SCD', 'SBD')
ORDER BY rxnorm_str;
```

## Deployment

### Prerequisites
1. Deploy core ETL infrastructure: `npm run etl:deploy:core`
2. Deploy and run bronze/silver dependencies:
   - `pp-dw-etl-fda-nsde` (bronze)
   - `pp-dw-etl-fda-cder` (bronze)
   - `pp-dw-etl-fda-all-ndc` (silver)
   - `pp-dw-etl-rxnorm` (bronze)
   - `pp-dw-etl-rxnorm-products` (silver)

### Deploy Gold Layer
```bash
# From project root
npm run etl:deploy

# Or deploy just this stack
cd infra/etl
cdk deploy pp-dw-etl-drug-product-codesets
```

### Run the Job
```bash
# Via AWS CLI
aws glue start-job-run --job-name pp-dw-gold-drug-product-codesets

# Via AWS Console
# Navigate to: AWS Glue → ETL Jobs → pp-dw-gold-drug-product-codesets → Run job
```

### Query the Results
```bash
# Via Athena CLI
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM pp_dw_gold.drug_product_codesets" \
  --result-configuration "OutputLocation=s3://pp-dw-{account}/athena-results/" \
  --query-execution-context "Database=pp_dw_gold"

# Via AWS Console
# Navigate to: Athena → Query Editor → Select database: pp_dw_gold
```

## Data Lineage

```
Bronze Layer (Raw Data):
├── fda-nsde (FDA NSDE CSV)
├── fda-cder (FDA CDER products/packages CSV)
└── rxnorm (RxNORM RRF files)
    └── RXNSAT (NDC attribute mappings)

↓

Silver Layer (Transformed):
├── fda-all-ndc (NSDE + CDER INNER JOIN)
└── rxnorm-products (Prescribable products with enrichments)

↓

Gold Layer (Analytics-Ready):
└── drug-product-codesets (FDA + RxNORM unified)
```

## Maintenance

### Schedule
- **Current:** On-demand only (manual execution)
- **Recommended:** Daily at 10 AM UTC (after silver layers complete)
- **Configuration:** Set `schedule.enabled: true` in `config.json`

### Data Refresh Strategy
- **Kill-and-Fill:** Complete table replacement on each run
- **No Partitions:** Gold layer is not partitioned
- **Run ID Tracking:** `meta_run_id` preserves lineage to bronze layer

### Monitoring
- **CloudWatch Logs:** `/aws-glue/jobs/pp-dw-gold-drug-product-codesets`
- **Spark UI:** Enabled (logs in `s3://pp-dw-{account}/glue-spark-logs/`)
- **Job Metrics:** Track record counts, join rates, null rates

## Troubleshooting

### Issue: Low RxNORM Enrichment Rate
**Symptoms:** Most `rxnorm_*` fields are NULL

**Causes:**
1. RXNSAT table missing NDC attributes
2. NDC format mismatch (11-digit vs 10-digit)
3. RxNORM data is stale

**Resolution:**
- Verify RXNSAT query: `SELECT COUNT(*) FROM pp_dw_bronze.rxnorm_rxnsat WHERE sab='RXNORM' AND atn='NDC'`
- Check NDC format consistency
- Re-run RxNORM bronze job with latest data

### Issue: Job Fails with "Table Not Found"
**Symptoms:** Glue job fails with catalog errors

**Causes:**
1. Silver/bronze tables not created
2. Crawler hasn't run
3. Database doesn't exist

**Resolution:**
- Run silver/bronze jobs first
- Manually trigger crawlers: `aws glue start-crawler --name <crawler-name>`
- Verify databases exist: `aws glue get-databases`

## References

- **FDA NDC Directory:** https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory
- **RxNORM Documentation:** https://www.nlm.nih.gov/research/umls/rxnorm/
- **RXNSAT Attribute Reference:** https://www.nlm.nih.gov/research/umls/rxnorm/docs/techdoc.html#s12_0
- **ETL Architecture:** See `infra/etl/CLAUDE.md`
