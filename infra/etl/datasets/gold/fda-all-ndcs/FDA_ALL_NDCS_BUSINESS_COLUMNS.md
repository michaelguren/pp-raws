# FDA All NDCs - Business Columns Reference

## Concrete Example for content_hash Calculation

**Critical Rule (November 19, 2025 Incident Prevention):**
Only include **stable business attributes** from source data.
**NEVER** include volatile RAWS metadata like `run_date`, `run_id`, `source_run_id`, etc.

## Complete Business Columns List

```python
business_columns = [
    # NDC Identifiers
    "ndc_11",           # Business key - 11-digit NDC code (no hyphens)
    "ndc_5",            # 5-digit labeler code

    # Product Information
    "proprietary_name",        # Brand/trade name
    "dosage_form",            # e.g., "TABLET", "CAPSULE", "INJECTION"
    "marketing_category",     # e.g., "NDA", "ANDA", "OTC"
    "product_type",           # e.g., "HUMAN PRESCRIPTION DRUG"
    "application_number",     # FDA application number (e.g., "NDA020151")
    "dea_schedule",           # DEA controlled substance schedule (if applicable)
    "package_description",    # Package size and type

    # Strength Information
    "active_numerator_strength",  # e.g., "20"
    "active_ingredient_unit",     # e.g., "mg"

    # Regulatory Information
    "spl_id",                 # Structured Product Labeling ID
    "marketing_start_date",   # When product entered market
    "marketing_end_date",     # When product left market (if applicable)
    "billing_unit",           # Billing/reimbursement unit

    # Source Flags
    "nsde_flag"              # Boolean: Present in FDA NSDE dataset
]
```

## What's EXCLUDED (Volatile Metadata)

These columns are **automatically filtered** by `business_columns_only()`:

```python
# ❌ NEVER include these in content_hash:
- run_date          # Changes every run
- run_id            # Changes every run
- source_run_id     # Changes every run (from silver/bronze)
- meta_run_id       # Legacy metadata
- source_file       # ETL lineage, not business data
- version_id        # SCD2 mechanics, not business data
- content_hash      # Self-referential
- active_from       # SCD2 mechanics
- active_to         # SCD2 mechanics
- status            # SCD2 mechanics
- updated_at        # Audit timestamp
- created_at        # Audit timestamp
```

## Why This Matters

**November 19, 2025 Incident:**
Including `run_date`, `run_id`, and `source_run_id` in the content_hash caused:
- Every row appeared CHANGED on every run
- All ~80K rows were expired + re-inserted each run
- All `active_from` dates reset to 2025-11-19
- Complete loss of temporal history

**The Fix:**
The `business_columns_only()` validator enforces strict separation of business data from metadata, preventing this incident from recurring.

## Validation Behavior

When you provide `business_columns` to `apply_temporal_versioning_delta()`:

1. **Validator runs**: `business_columns_only(df, business_columns)`
2. **Filters volatile columns**: Uses explicit `VOLATILE_METADATA_COLUMNS` denylist
3. **Fails fast**: Raises exception if all columns are filtered out
4. **Logs results**: Shows which columns were removed and which are used for hashing

Example log output:
```
[VALIDATION] ⚠️  Removed 3 volatile metadata columns from content_hash:
[VALIDATION]     - run_date
[VALIDATION]     - run_id
[VALIDATION]     - source_run_id
[VALIDATION] ✅ Using 14 business columns for content_hash:
[VALIDATION]     - ndc_11
[VALIDATION]     - proprietary_name
[VALIDATION]     - dosage_form
[VALIDATION]     ...
```

## Schema Evolution

When adding new business columns (e.g., `fda_labeler_name` from a schema update):

1. ✅ Add to `business_columns` list in gold_job.py
2. ✅ Delta Lake automatically handles schema evolution (`mergeSchema=true`)
3. ✅ Existing records get NULL for new column
4. ✅ New records populate the column
5. ✅ Change detection works correctly (hash includes new column)

## Complete Example

```python
# In gold_job.py
from temporal_versioning_delta import apply_temporal_versioning_delta

# Define ONLY stable business attributes
business_columns = [
    "ndc_11", "ndc_5", "proprietary_name", "dosage_form",
    "marketing_category", "product_type", "application_number",
    "dea_schedule", "package_description", "active_numerator_strength",
    "active_ingredient_unit", "spl_id", "marketing_start_date",
    "marketing_end_date", "billing_unit", "nsde_flag"
]

# Library validates and filters automatically
stats = apply_temporal_versioning_delta(
    spark=spark,
    incoming_df=silver_df,
    gold_path="s3://bucket/gold/fda-all-ndcs/",
    business_key="ndc_11",
    business_columns=business_columns,  # Validator removes volatile columns
    run_date=run_date,
    run_id=run_id
)

print(f"NEW: {stats['new']}")
print(f"CHANGED: {stats['changed']}")  # Only real business changes
print(f"UNCHANGED: {stats['unchanged']}")  # Correctly skipped
```

---

**Last Updated:** 2025-11-19
**Status:** Production-hardened after November 19 incident
