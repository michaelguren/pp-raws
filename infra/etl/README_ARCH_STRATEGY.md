──────────────────────
ARCHITECTURE DECISIONS
──────────────────────

We are building a simple, boring, long-term maintainable data + serving stack with the following components:

Core tech stack (fixed):
- Storage: S3
- Conformed tables: S3 Tables (managed Apache Iceberg)
- Metadata: AWS Glue Data Catalog
- ETL engine: Athena SQL (CTAS, INSERT, MERGE)
- Serving store: DynamoDB
- API: API Gateway + VTL or very thin Lambdas
- NO Spark, NO EMR, NO Redshift, NO Aurora for this flow

High-level data flow for each entity type (e.g., DRUG):

1) Raw Archive (S3, immutable)
2) Staging Tables (per batch, one row per entity, with fingerprint)
3) Conformed S3 Tables (S3 Tables + Iceberg + SCD Type 2)
4) DynamoDB serving cache (current + history + search tokens)
5) API endpoints reading from DynamoDB

These are not suggestions; they are non-negotiable design constraints.

────────────────────────────
1) RAW ARCHIVE (S3 IMMUTABLE)
────────────────────────────

Bucket layout pattern:

- s3://pp-archive/raw/<entity_type>/<source>/<ingest_date>/<original_files>

Example:
- s3://pp-archive/raw/DRUG/FDA_NDC/2025-01-03/fda-ndc-2025-01-03.xlsx

Invariants:
- Raw files are append-only and never mutated or deleted.
- They are NOT used directly for user-facing APIs.
- They exist to support reprocessing and lineage.

What I want from you:
- Glue DDL or crawlers only if needed.
- Keep this layer as dumb as possible: just files + minimal schema.

──────────────────────────────
2) STAGING TABLES (PER BATCH)
──────────────────────────────

Purpose:
- Normalize each ingestion batch into a clean, flat table.
- One row per entity per batch (e.g. one row per NDC).
- Compute a deterministic fingerprint over business fields for change detection.

Example staging table (Athena/Glue) for drugs:

- Table: staging.drug_batch
- Columns (conceptual):
  - ndc, proprietary_name, ingredient_list, strength, route
  - source_effective_date, source_file_date
  - ingestion_timestamp
  - ingestion_batch_id (ISO 8601 string, unique per run)
  - fingerprint (hex MD5 over business fields)

Fingerprint pattern (pseudo-SQL):

fingerprint = to_hex(
  md5(
    concat_ws(
      '|',
      coalesce(ndc, ''),
      coalesce(proprietary_name, ''),
      coalesce(ingredient_list, ''),
      coalesce(strength, ''),
      coalesce(route, '')
    )
  )
)

Staging layout:

- s3://pp-archive/staging/<entity_type>/<ingestion_batch_id>/part-*.parquet

Invariants:
- Staging is append-only (new batch → new prefix).
- Staging is the ONLY input to the conformed S3 Table for that batch.

What I want from you:
- Concrete Glue/Athena DDL for staging tables.
- CTAS/INSERT examples that populate staging from raw.

───────────────────────────────────────────────
3) CONFORMED S3 TABLES (S3 TABLES + SCD TYPE 2)
───────────────────────────────────────────────

We use S3 Tables (managed Iceberg) for the conformed model.

Example: `conformed.drug`

Schema (simplified but important):

Business fields:
- ndc, proprietary_name, ingredient_list, strength, route, etc.

SCD Type 2 columns:
- active_from (DATE)
- active_to (DATE, 9999-12-31 for current)
- is_current (BOOLEAN)

Ingestion / lineage:
- source_effective_date (DATE)
- source_file_date (DATE)
- ingestion_timestamp (TIMESTAMP)
- ingestion_batch_id (STRING, ISO 8601 per run)
- fingerprint (STRING, hex MD5 over business fields)
- source_system (STRING, e.g. "FDA_NDC")

Key invariants:
- We NEVER physically DELETE rows from conformed S3 Tables.
- SCD Type 2 encoding is the source of truth for business-time history:
  - Open interval:   active_to = 9999-12-31 and is_current = true
  - Closed interval: active_to < 9999-12-31 and is_current = false
- Only Athena SQL (MERGE + INSERT) mutates these tables.
- No Spark/EMR/Delta jobs.

Per-batch SCD2 behavior:
- Input: staging.<entity_type> for a specific ingestion_batch_id.
- For each entity:
  - If brand new → insert a new current row.
  - If fingerprint changed → close old current row (update active_to/is_current) and insert a new current row.
  - If entity disappeared from the staging batch → close old current row (logical delete) but never delete the row physically.

Implementation pattern:
- One MERGE to close old “current” rows that changed or disappeared.
- One INSERT (or MERGE variant) to insert new current rows for new/changed entities.
- All rows touched in a run share the same ingestion_batch_id for easy DDB sync.

Time travel:
- Business time: use SCD2 columns.
- System time: rely on Iceberg/S3 Tables snapshots (we do NOT manage snapshot folders manually).

What I want from you:
- Athena MERGE + INSERT SQL templates that implement this SCD2 logic.
- Comments and structure that make it obvious and safe to maintain.

────────────────────────────
4) DYNAMODB SERVING LAYER
────────────────────────────

DynamoDB is the **only** backing store for real-time API reads.

Table pattern (single table):

Entity items:
- PK = "ITEM#<ENTITY_TYPE>#<entity_id>"     (e.g. "ITEM#DRUG#00002322702")
- SK = "V#CURRENT"                          (current version)
- SK = "V#<active_from>"                    (historical SCD2 rows)

Attributes (for DRUG):
- entity_type = "DRUG"
- entity_id   = "<ndc>"

- effective_from          = active_from (from conformed table)
- effective_to            = active_to
- is_current              = is_current
- fingerprint             = fingerprint
- last_ingestion_batch_id = ingestion_batch_id

- proprietary_name, ingredient_list, strength, route, etc.

Search token items (for simple token search):
- PK = "SEARCH#<ENTITY_TYPE>"
- SK = "token#<normalized_token>#<field>#<entity_type>#<entity_id>"

Example:
- PK: "SEARCH#DRUG"
  SK: "token#prozac#proprietary_name#DRUG#00002322702"

Invariants:
- Only CURRENT SCD2 rows produce search tokens.
- All historical variants exist only under the ITEM#… partition.

Sync from conformed S3 Table to DynamoDB (per batch):

1) Query S3 Table:
   SELECT * FROM conformed.<entity_type>
   WHERE ingestion_batch_id = '<this_batch>';

2) Group rows by entity_id and classify:
   - INSERT: only a new current row present.
   - UPDATE: one closed row + one new current row.
   - DELETE: only a closed row present, no new current row.

3) Apply to DDB:
   - INSERT:
     - Put V#CURRENT + generate search tokens.
   - UPDATE:
     - Write a historical row for the closed version (e.g. SK="V#<active_from>").
     - Overwrite V#CURRENT with new current row.
     - Refresh search tokens.
   - DELETE:
     - Update V#CURRENT to is_current=false and set effective_to.
     - Optionally write a historical row.
     - Delete search tokens.

Idempotency:
- Condition writes on (last_ingestion_batch_id, fingerprint) so reprocessing the same batch is safe.

What I want from you:
- DDB table definition (CDK/CloudFormation).
- Concrete sync worker logic (pseudo-code or actual code in my language of choice).
- No extra databases. Dynamo only.

───────────────────────
5) API LAYER ON TOP OF DDB
───────────────────────

The APIs are thin facades over DynamoDB:

- GET /drug/{ndc}
  - Read: PK="ITEM#DRUG#<ndc>", SK="V#CURRENT".

- GET /drug/{ndc}/history
  - Query: PK="ITEM#DRUG#<ndc>" and return all V#* versions ordered by effective_from.

- GET /search/drug?q=...
  - Tokenize query.
  - For each token, query:
    PK="SEARCH#DRUG", SK begins_with "token#<token>#"
  - Aggregate entity IDs across tokens.
  - Batch get V#CURRENT from entity items.
  - Return results.

What I want from you:
- API Gateway configuration or OpenAPI describing these endpoints.
- Either VTL mappings or very thin Lambda handlers to wire DDB to HTTP.
- No frameworks that obscure what’s happening.

────────────────────────
HOW TO WORK WITH MY CODE
────────────────────────

When I paste repo structure or files:

1) First, restate in your own words how the current code aligns or conflicts with this architecture.
2) Then propose a SMALL, CONCRETE migration step focused on ONE layer at a time, for example:
   - “Let’s introduce the staging.<entity_type> tables and their DDL.”
   - “Let’s add the conformed.<entity_type> S3 Table and the MERGE SQL.”
   - “Let’s define the DynamoDB table and one sync worker.”
   - “Let’s wire one read-only API path to DDB.”

3) Always show:
   - Before/after diffs OR full replacement of the files you change.
   - Enough context that I can paste directly into my project.

4) Do NOT:
   - Suggest Spark/EMR/Redshift/Aurora as “better.”
   - Redesign the architecture unless I explicitly ask for that.
   - Get stuck in loops about alternatives; follow this plan.

Start by summarizing this architecture in your own words, then ask me for:
- Either my current repo layout, or
- The first layer I want to migrate (staging, conformed, DDB, or API).
