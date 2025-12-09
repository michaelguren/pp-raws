# ETL Data Pipeline Architecture

**Read `../CLAUDE.md` first** for general infrastructure requirements (CDK structure, SDK v3, deployment patterns).

This file provides ETL-specific AI guidance and conventions.

---

## Architecture Summary

Five-layer stack (non-negotiable):

| Layer | Technology | Retention | Purpose |
|-------|-----------|-----------|---------|
| 1. Raw | S3 | Kill-and-fill (archived for replay) | Immutable source files |
| 2. Staging | Athena SQL + Parquet | Kill-and-fill | Normalized batches with fingerprints |
| 3. Conformed | S3 Tables (Iceberg) | Time travel via Iceberg | SCD Type 2 via MERGE — source of truth |
| 4. DynamoDB | Single-table design | Current + historical versions | Serving layer for API reads |
| 5. API | API Gateway + VTL | — | Thin facade over DynamoDB |

**ETL Engine:** Athena SQL only (CTAS, INSERT, MERGE). No Spark, no EMR, no Glue jobs.

**Replay capability:** Raw files are retained in S3 for a configurable period. If needed, ETL can be replayed from raw → staging → conformed.

---

## Directory Structure
```
etl/
├── CLAUDE.md                    # This file
├── index.js                     # CDK app entrypoint
├── cdk.json                     # CDK config
├── EtlCoreStack.js              # Shared infra (S3, databases, DynamoDB)
├── shared/                      # Cross-dataset utilities
│   ├── sql/                     # Reusable SQL fragments
│   └── lambdas/                 # Shared Lambda code
└── datasets/                    # One folder per dataset
    └── fda-ndcs/                # Reference implementation (start here)
        ├── config.json          # Dataset metadata and schema
        ├── FdaNdcsStack.js      # CDK stack for this dataset
        └── sql/
            ├── staging.sql      # Raw → Staging (CTAS with fingerprint)
            └── conformed.sql    # Staging → Conformed (MERGE + INSERT)
```

**Key principle:** Each dataset owns its complete pipeline (raw → staging → conformed → DDB sync). No separation by layer.

**Reference implementation:** `fda-ndcs` is the first dataset. All patterns will be established here before scaling to other datasets.

---

## S3 Layout
```
s3://pp-archive/
├── raw/<dataset>/<source>/<ingest_date>/            # Kill-and-fill, retained for replay
├── staging/<dataset>/<ingestion_batch_id>/          # Kill-and-fill Parquet
└── conformed/<dataset>/                             # S3 Tables (managed Iceberg)
```

---

## Key Patterns

### Fingerprint-Based Change Detection

Every staging row includes a deterministic fingerprint:
```sql
to_hex(md5(concat_ws('|',
  coalesce(field1, ''),
  coalesce(field2, ''),
  ...
))) AS fingerprint
```

This drives SCD Type 2 logic: same fingerprint = no change.

### SCD Type 2 Columns (Conformed Layer)
```sql
active_from        DATE        -- Version start
active_to          DATE        -- Version end (9999-12-31 = current)
is_current         BOOLEAN     -- Redundant but useful for queries
fingerprint        STRING      -- For change detection
ingestion_batch_id STRING      -- Links to staging batch
```

**Invariants:**
- Never physically delete rows
- Close old versions by setting `active_to` and `is_current = false`
- All mutations via Athena MERGE + INSERT

### DynamoDB Key Structure
```
Entity items:
  PK = "ITEM#<ENTITY_TYPE>#<entity_id>"
  SK = "V#CURRENT" | "V#<active_from>"

Search tokens:
  PK = "SEARCH#<ENTITY_TYPE>"
  SK = "token#<normalized_token>#<field>#<entity_type>#<entity_id>"
```

Sync from conformed layer uses `ingestion_batch_id` to identify changed rows.

---

## ⚠️ Critical Rules

### 1. Filesystem Paths = Deploy-Time Only
```
┌──────────────────────────────────────────────────────────────────┐
│ CDK stacks may use __dirname and path helpers.                   │
│ Runtime code (Lambda, Athena) works only with S3 URIs and /tmp. │
└──────────────────────────────────────────────────────────────────┘
```

### 2. No Glue Jobs or Crawlers

All ETL is Athena SQL. No `boto3.client("glue")` anywhere.

### 3. No Physical Deletes

Conformed tables are append-only with SCD Type 2 closures.

---

## Naming Conventions

**Datasets:** Lowercase, plural, with hyphens (e.g., `fda-ndcs`, `rxnorm-products`, `dailymed-spls`)

**S3 Paths:**
- Raw: `s3://pp-archive/raw/<dataset>/<source>/<ingest_date>/`
- Staging: `s3://pp-archive/staging/<dataset>/<ingestion_batch_id>/`
- Conformed: `s3://pp-archive/conformed/<dataset>/`

**Glue Databases:**
- `pp_dw_staging`
- `pp_dw_conformed`

**S3 Tables:** Match dataset names with underscores (e.g., `fda_ndcs`, `rxnorm_products`)

**DynamoDB Table:** `pp-dw-entities` (single-table design)

**CDK Stacks:** `pp-dw-etl-core`, `pp-dw-etl-<dataset>` (e.g., `pp-dw-etl-fda-ndcs`)

---

## Adding a New Dataset

**TBD** — Patterns will be established through the `fda-ndcs` reference implementation.

---

## What NOT to Do

- ❌ Spark, EMR, Glue jobs
- ❌ Delta Lake (use S3 Tables / Iceberg)
- ❌ Glue Crawlers (Athena manages catalog)
- ❌ Physical deletes in conformed tables
- ❌ OpenSearch or external search (use DynamoDB prefix tokens)
- ❌ Frameworks that obscure what's happening
- ❌ Organizing folders by layer (bronze/silver/gold) — organize by dataset

---

## AI Behavioral Notes

When working in this domain:

1. **Default to Athena SQL** for all data transformations
2. **Propose small, incremental changes** — one layer at a time
3. **Ask questions** when entity structure or key design could set precedents
4. **Update documentation** when patterns stabilize

This is RAWS v0.1 — you're helping build the paradigm, not just the product.