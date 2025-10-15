# Gold RxNORM Products Dataset

**Layer**: Gold (Curated, Temporally Versioned)
**Source**: `pp_dw_silver.rxnorm_products`
**Table**: `pp_dw_gold.rxnorm_products`
**Partitioning**: By `status` (current/historical)

---

## Overview

Temporal versioning (SCD Type 2) for prescribable drug products from RxNORM. Tracks changes over time with `active_from` and `active_to` dates.

**Key Features**:
- Current records: `active_to = '9999-12-31'`
- Status partitioning for efficient queries
- Dropped `rxnorm_` prefix (table name indicates source)
- MD5 hash for change detection

---

## Schema

### Version Tracking
| Column | Type | Description |
|--------|------|-------------|
| `version_id` | STRING | UUID for this version |
| `content_hash` | STRING | MD5 hash for change detection |
| `active_from` | DATE | When version became active |
| `active_to` | DATE | When version expired (`9999-12-31` = current) |
| `status` | STRING | Partition: `current` or `historical` |

### Business Key
| Column | Type | Description |
|--------|------|-------------|
| `rxcui` | STRING | RxNORM concept identifier (primary key) |

### Business Columns (RxNORM Data)
| Column | Type | Description |
|--------|------|-------------|
| `tty` | STRING | Term type (SCD, SBD, GPCK, BPCK) |
| `str` | STRING | Concept string/name |
| `strength` | STRING | Drug strength (e.g., "20 mg") |
| `ingredient_names` | STRING | Active ingredients (slash-separated) |
| `brand_names` | STRING | Brand names (slash-separated) |
| `dosage_forms` | STRING | Dosage forms (comma-separated) |
| `psn` | STRING | Prescribable name |
| `sbdf_rxcui` | STRING | Semantic branded dose form RXCUI |
| `sbdf_name` | STRING | Semantic branded dose form name |
| `scdf_rxcui` | STRING | Semantic clinical dose form RXCUI |
| `scdf_name` | STRING | Semantic clinical dose form name |
| `sbd_rxcui` | STRING | Semantic branded drug RXCUI |
| `bpck_rxcui` | STRING | Branded pack RXCUI |
| `multi_ingredient` | BOOLEAN | TRUE if multiple ingredients |

### Metadata
| Column | Type | Description |
|--------|------|-------------|
| `source_file` | STRING | Source table name |
| `run_date` | STRING | When version created (YYYY-MM-DD) |
| `run_id` | STRING | Gold layer run ID |

---

## Common Queries

### Get All Current Products
```sql
SELECT * FROM pp_dw_gold.rxnorm_products WHERE status = 'current';
```

### Get Multi-Ingredient Products
```sql
SELECT rxcui, str, ingredient_names, strength
FROM pp_dw_gold.rxnorm_products
WHERE status = 'current' AND multi_ingredient = TRUE;
```

### Find Products by Ingredient
```sql
SELECT rxcui, str, ingredient_names, brand_names
FROM pp_dw_gold.rxnorm_products
WHERE status = 'current'
  AND LOWER(ingredient_names) LIKE '%fluoxetine%';
```

### Get Changes for Incremental Sync
```sql
-- Get all records that changed today
SELECT *
FROM pp_dw_gold.rxnorm_products
WHERE run_date = '2025-10-14'
  AND (
    active_from = '2025-10-14'
    OR (active_to = '2025-10-14' AND active_to < '9999-12-31')
  );
```

### Point-in-Time Query
```sql
-- What did RXCUI 392409 look like on Jan 1, 2024?
SELECT *
FROM pp_dw_gold.rxnorm_products
WHERE rxcui = '392409'
  AND '2024-01-01' BETWEEN active_from AND active_to;
```

---

## Deployment

```bash
# Deploy infrastructure
cd infra/etl
cdk deploy pp-dw-etl-gold-rxnorm-products

# Run Glue job
aws glue start-job-run --job-name pp-dw-gold-rxnorm-products

# Run crawler
aws glue start-crawler --name pp-dw-gold-rxnorm-products-crawler
```

---

## Performance

### Initial Load
- **Input**: ~50K silver records
- **Output**: ~50K gold records (all `status=current`)
- **Runtime**: ~3-4 minutes (10 G.2X workers)

### Incremental Run
- **Changed records**: ~100-500/month (0.2-1%)
- **Output**: ~200-1000 records
- **Runtime**: ~2 minutes

---

## Cost Estimate

- **Storage**: ~20 MB current + ~200 MB/year historical
- **Glue Job**: $0.18/run (monthly schedule)
- **Monthly**: ~$2/month

---

See FDA gold dataset README for detailed temporal versioning concepts and maintenance procedures.
