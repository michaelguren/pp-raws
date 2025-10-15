# RxNORM Products Silver Dataset

## Overview

**Layer**: Silver (enriched/denormalized from bronze)
**Purpose**: Create a prescribable drug product table by enriching RxNORM bronze data with denormalized ingredient names, brand names, strengths, dosage forms, and semantic relationships.

This dataset transforms the normalized RxNORM relational model (RXNCONSO, RXNREL, RXNSAT) into a single denormalized table optimized for drug product queries and downstream application use.

## Background: RxNORM Data Model

### What is RxNORM?

RxNORM is a standardized nomenclature from the National Library of Medicine (NLM) for clinical drugs and drug delivery devices. It provides:

- Normalized names for clinical drugs
- Unique concept identifiers (RXCUIs)
- Relationships between drug concepts across vocabularies
- Standard term types (TTY) for different drug representations

**Documentation**:

- [RxNORM Technical Documentation](https://www.nlm.nih.gov/research/umls/rxnorm/docs/techdoc.html)
- [RxNORM Appendix (Relationships)](https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix1.html)
- [RxNORM Entity Diagram](https://www.nlm.nih.gov/research/umls/rxnorm/RxNorm_entity_diagram.pdf)

### Bronze Tables (Source Data)

Our bronze layer contains three core RxNORM tables from the RRF files:

#### 1. **RXNCONSO** (Concept Names and Attributes)

- **Purpose**: Contains all RxNORM concepts with their names and term types
- **Key Columns**:
  - `rxcui`: Unique concept identifier
  - `tty`: Term Type (SCD, SBD, GPCK, BPCK, IN, PIN, MIN, BN, DF, PSN, etc.)
  - `str`: String name of the concept
  - `sab`: Source abbreviation (we filter to `SAB='RXNORM'`)
  - `cvf`: Content View Flag (`CVF='4096'` = prescribable drugs)

#### 2. **RXNREL** (Concept Relationships)

- **Purpose**: Defines relationships between RxNORM concepts
- **Key Columns**:
  - `rxcui1`: Source concept
  - `rxcui2`: Target concept
  - `rela`: Relationship attribute (see below)
  - `sab`: Source abbreviation
  - `cvf`: Content View Flag

#### 3. **RXNSAT** (Concept Attributes)

- **Purpose**: Additional attributes for concepts (e.g., strength)
- **Key Columns**:
  - `rxcui`: Concept identifier
  - `atn`: Attribute name (e.g., `RXN_AVAILABLE_STRENGTH`)
  - `atv`: Attribute value
  - `cvf`: Content View Flag

### Key Term Types (TTY)

We focus on **prescribable products** (`CVF='4096'`):

| TTY      | Description                      | Example                                      |
| -------- | -------------------------------- | -------------------------------------------- |
| **SCD**  | Semantic Clinical Drug (generic) | "Acetaminophen 325 MG Oral Tablet"           |
| **SBD**  | Semantic Branded Drug (brand)    | "Tylenol 325 MG Oral Tablet"                 |
| **GPCK** | Generic Pack                     | "Acetaminophen 325 MG Oral Tablet [12 Pack]" |
| **BPCK** | Branded Pack                     | "Tylenol 325 MG Oral Tablet [12 Pack]"       |
| **SCDF** | Semantic Clinical Dose Form      | "Acetaminophen Oral Tablet"                  |
| **SBDF** | Semantic Branded Dose Form       | "Tylenol Oral Tablet"                        |
| **IN**   | Ingredient                       | "Acetaminophen"                              |
| **PIN**  | Precise Ingredient               | "Acetaminophen Anhydrous"                    |
| **MIN**  | Multiple Ingredients             | "Acetaminophen / Codeine"                    |
| **BN**   | Brand Name                       | "Tylenol"                                    |
| **DF**   | Dose Form                        | "Oral Tablet"                                |
| **PSN**  | Prescribable Name                | "Tylenol Oral Product"                       |

### Key Relationships (RELA)

Relationships define how concepts connect:

| RELA                     | Description                    | Example    |
| ------------------------ | ------------------------------ | ---------- |
| `has_ingredient`         | Product → Ingredient           | SCD → IN   |
| `has_ingredients`        | Product → Multiple Ingredients | SCD → MIN  |
| `has_precise_ingredient` | Component → Precise Ingredient | SCDC → PIN |
| `has_tradename`          | Generic → Brand                | SCD → SBD  |
| `tradename_of`           | Brand → Generic                | SBD → SCD  |
| `has_dose_form`          | Product → Dose Form            | SCD → DF   |
| `isa`                    | Instance → Parent Class        | SCD → SCDF |
| `consists_of`            | Product → Component            | SCD → SCDC |
| `contains`               | Pack → Product                 | GPCK → SCD |

### CVF Flag (Content View Flag)

- **`CVF='4096'`**: **Prescribable drugs** (current, active concepts in the US market)
- This is the **only filter we care about** for this dataset
- Excludes obsolete, non-prescribable, or historical concepts

## Silver Dataset: rxnorm_products

### Purpose

Create a **single denormalized table** with one row per prescribable drug product (SCD, SBD, GPCK, BPCK), enriched with:

- Ingredient names (prioritizing MIN > PIN > IN)
- Brand names (from BN concepts)
- Strengths (from RXNSAT)
- Dosage forms (from DF concepts)
- Semantic relationships (SCDF, SBDF, SBD, BPCK)
- Multi-ingredient flag

### Source: Legacy Rails ETL Logic

The original PostgreSQL-based ETL (from `app/models/rxnorm_product.rb`) performed **13+ sequential UPDATE statements**:

1. **Base Insert**: Select SCD/SBD/GPCK/BPCK from RXNCONSO
2. **Ingredient Names**: Join via `consists_of` → `has_precise_ingredient`/`has_ingredient`/`has_ingredients`
3. **Strength**: Join to RXNSAT for `RXN_AVAILABLE_STRENGTH`, strip "EXPRESSED AS" text
4. **Brand Names**: Join via `has_tradename` → `has_ingredient` → BN
5. **Prescribable Name (PSN)**: Join to RXNCONSO for `TTY='PSN'`
6. **Dosage Forms**: Join via `has_dose_form` → DF; special logic for packs
7. **SCDF/SBDF Fields**: Join via `isa` relationships
8. **SBD/BPCK RXCUI**: Join via `has_tradename` relationships
9. **Multi-Ingredient Flag**: Check for `has_ingredients` → MIN existence

**Problem**: Sequential UPDATEs work well in PostgreSQL but are **inefficient in Spark/Glue** (13+ full table scans).

**Solution**: Consolidate into a **single JOIN-heavy Spark transformation** leveraging lazy evaluation and columnar processing.

## Output Schema

### Columns (After Cleanup)

| Column                    | Type      | Source          | Description                                  |
| ------------------------- | --------- | --------------- | -------------------------------------------- |
| `tty`                     | string    | RXNCONSO        | Term type (SCD, SBD, GPCK, BPCK)             |
| `rxcui`                   | string    | RXNCONSO        | RxNORM concept unique identifier             |
| `str`                     | text      | RXNCONSO        | Concept string name                          |
| `rxnorm_strength`         | text      | RXNSAT          | Drug strength (e.g., "325 mg")               |
| `rxnorm_ingredient_names` | text      | RXNCONSO+RXNREL | Active ingredients (MIN > PIN > IN priority) |
| `rxnorm_brand_names`      | text      | RXNCONSO+RXNREL | Brand names (from BN concepts)               |
| `rxnorm_dosage_forms`     | string    | RXNCONSO+RXNREL | Dosage forms (e.g., "Oral Tablet")           |
| `rxnorm_psn`              | text      | RXNCONSO        | Prescribable Semantic Name                   |
| `sbdf_rxcui`              | string    | RXNREL          | Semantic Branded Dose Form RXCUI             |
| `sbdf_name`               | text      | Computed        | `brand_names + ' ' + dosage_forms`           |
| `scdf_rxcui`              | string    | RXNREL          | Semantic Clinical Dose Form RXCUI            |
| `scdf_name`               | text      | RXNCONSO        | SCDF string name                             |
| `sbd_rxcui`               | string    | RXNREL          | Semantic Branded Drug RXCUI (for SCDs)       |
| `bpck_rxcui`              | string    | RXNREL          | Branded Pack RXCUI (for GPCKs)               |
| `multi_ingredient`        | boolean   | RXNREL          | True if has MIN relationship                 |
| `created_at`              | timestamp | Glue            | Job run timestamp                            |
| `updated_at`              | timestamp | Glue            | Job run timestamp                            |

### Removed Columns (Deferred)

These columns existed in the legacy schema but are **removed** for now:

- ~~`drug_rxnorm_mapping_id`~~ (application-specific foreign key)
- ~~`fda_dea_schedule`~~ (from fda_all_ndcs, not needed)
- ~~`fda_product_type`~~ (from fda_all_ndcs, not needed)
- ~~`fda_brand_names`~~ (from fda_all_ndcs, not needed)
- ~~`viewer_drug_id`~~ (application-specific, unclear purpose)

**Note**: We may add these back later if downstream systems require them.

## Transformation Logic (Spark Strategy)

### High-Level Approach

Instead of 13+ sequential UPDATEs, we perform **one large Spark transformation** with multiple joins and window functions.

### Step-by-Step Logic

#### 1. **Base Selection**

```sql
SELECT tty, rxcui, str
FROM rxnconso
WHERE tty IN ('SCD', 'GPCK', 'SBD', 'BPCK')
  AND sab = 'RXNORM'
  AND cvf = '4096'
```

#### 2. **Ingredient Names** (Priority: MIN > PIN > IN)

**For SCD**:

```
SCD --[consists_of]--> SCDC --[has_precise_ingredient]--> PIN (Precise Ingredient)
SCD --[has_ingredients]--> MIN (Multiple Ingredients)
SCD --[consists_of]--> SCDC --[has_ingredient]--> IN (Ingredient)
```

**For SBD**:

```
SBD --[tradename_of]--> SCD --[same as above]
```

**For GPCK/BPCK**:

```
GPCK/BPCK --[contains]--> SCD --[same as above]
```

**Priority Logic** (from legacy lines 722-727):

```python
CASE
  WHEN MIN IS NOT NULL THEN MIN
  WHEN PIN IS NOT NULL THEN PIN
  ELSE IN
END
```

#### 3. **Strength** (from RXNSAT)

```sql
JOIN rxnsat ON rxnsat.rxcui = products.rxcui
WHERE rxnsat.atn = 'RXN_AVAILABLE_STRENGTH'
  AND rxnsat.cvf = '4096'
```

**Post-processing** (legacy line 273):

- Remove `(EXPRESSED AS <ingredient>)` patterns
- Convert to lowercase
- Example: `"325 MG (EXPRESSED AS ACETAMINOPHEN)"` → `"325 mg"`

#### 4. **Brand Names**

**For SCD**:

```
SCD --[has_tradename]--> SBD --[has_ingredient]--> BN
```

**For SBD**:

```
SBD --[has_ingredient]--> BN
```

**Fallback** (legacy lines 381-405):

- First try with `cvf='4096'`
- If NULL, retry without `cvf` filter

#### 5. **Dosage Forms**

**For SCD/SBD**:

```
SCD/SBD --[has_dose_form]--> DF
```

**For GPCK/BPCK** (legacy lines 441-454):

```
GPCK/BPCK --[contains]--> SCD --[has_dose_form]--> DF
```

Format as: `"Pack (Oral Tablet / Oral Capsule)"`

#### 6. **Semantic Forms** (SCDF/SBDF/SBD/BPCK)

**SCDF for SCD**:

```
SCD --[isa]--> SCDF
```

**SCDF for SBD**:

```
SBD --[tradename_of]--> SCD --[isa]--> SCDF
```

**SBDF for SBD**:

```
SBD --[isa]--> SBDF
```

**SBDF for SCD**:

```
SCD --[has_tradename]--> SBD --[isa]--> SBDF
```

**SBDF Name** (legacy line 547):

```python
sbdf_name = rxnorm_brand_names + ' ' + rxnorm_dosage_forms
```

**SBD RXCUI for SCD**:

```
SCD --[has_tradename]--> SBD
```

**BPCK RXCUI for GPCK**:

```
GPCK --[has_tradename]--> BPCK
```

#### 7. **Multi-Ingredient Flag**

**For SCD**:

```sql
EXISTS (
  SELECT 1 FROM rxnrel
  WHERE rxnrel.rxcui2 = scd.rxcui
    AND rxnrel.rela = 'has_ingredients'
    AND rxnrel.sab = 'RXNORM'
)
```

**For SBD**:

```sql
EXISTS (
  SELECT 1 FROM rxnrel scd_rel
  JOIN rxnrel min_rel ON scd_rel.rxcui1 = min_rel.rxcui2
  WHERE scd_rel.rxcui2 = sbd.rxcui
    AND scd_rel.rela = 'tradename_of'
    AND min_rel.rela = 'has_ingredients'
    AND scd_rel.sab = 'RXNORM'
    AND min_rel.sab = 'RXNORM'
)
```

#### 8. **Prescribable Name (PSN)**

```sql
JOIN rxnconso psn ON psn.rxcui = products.rxcui
WHERE psn.tty = 'PSN'
  AND psn.sab = 'RXNORM'
  AND psn.cvf = '4096'
```

### Spark Implementation Notes

1. **Use PySpark DataFrames** with joins and window functions
2. **Broadcast small tables** (if applicable) for join optimization
3. **Use `array_agg` equivalent** (`collect_list` + `array_distinct`) for aggregations
4. **Leverage lazy evaluation**: Define all transformations first, then execute once
5. **Filter early**: Apply `cvf='4096'` and `sab='RXNORM'` filters as early as possible
6. **Expected output size**: ~100K rows (based on prescribable drug count)

## Dependencies

### Bronze Layer (Source)

- `pp_dw_bronze.rxnconso` (RxNORM concepts)
- `pp_dw_bronze.rxnrel` (RxNORM relationships)
- `pp_dw_bronze.rxnsat` (RxNORM attributes)

### Removed Dependency

- ~~`pp_dw_gold.fda_all_ndc`~~ (previously used for `fda_product_type`, `fda_dea_schedule`, `fda_brand_names` - all removed)

## Output

### S3 Path

```
s3://pp-dw-{account}/silver/rxnorm-products/
```

### Format

- **Parquet** with **ZSTD compression**
- **Kill-and-fill** pattern (full refresh each run)

### Glue Catalog

- **Database**: `pp_dw_silver` (NEW - we'll need to create this database)
- **Table**: `rxnorm_products`

## Deferred Features

These features exist in the legacy Rails code but are **out of scope** for initial implementation:

1. **Search Terms** (`refresh_search_terms` - lines 178-208)

   - Creates separate `rxnorm_product_search_terms` table
   - Tokenizes ingredient/brand/dosage form names for search
   - **Decision**: Defer to future silver dataset if needed

2. **ATC5 Mappings** (`refresh_atc5` - lines 870-895)

   - Joins to `rxclass_drug_members` table
   - Maps products to ATC5 classification codes
   - **Decision**: Defer (no bronze `rxclass` dataset exists yet)

3. **FDA-related columns** (removed entirely)
   - `fda_product_type`, `fda_dea_schedule`, `fda_brand_names`
   - **Decision**: May add back later if downstream systems require

## Implementation Plan

### Files to Create

```
infra/etl/datasets/rxnorm-products/
├── README.md (this file)
├── config.json
├── RxnormProductsStack.js
└── glue/
    └── silver_job.py
```

### Stack Pattern

**Custom stack** (similar to gold layer):

1. Read bronze RxNORM table metadata from Glue catalog
2. Create Glue job with `silver_job.py` script
3. Schedule via EventBridge (or run on-demand)
4. Create crawler to update `pp_dw_silver.rxnorm_products` table

### Testing Strategy

1. **Deploy to AWS** (no local Spark simulation)
2. **Run Glue job** against real bronze RxNORM tables
3. **Query via Athena** to validate output
4. **Compare row counts** with legacy PostgreSQL table
5. **Spot-check sample RXCUIs** for data quality

## Open Questions

1. **Silver database naming**: Use `pp_dw_silver` or reuse `pp_dw_gold`?
   - **Recommendation**: Create new `pp_dw_silver` database to distinguish layer semantics
2. **Metadata columns**: Use `run_id` timestamp for `created_at`/`updated_at`?
   - **Recommendation**: Yes, consistent with bronze/gold patterns
3. **Performance tuning**: Any Glue worker size recommendations for ~100K output rows?
   - **Recommendation**: Start with `Standard` workers, monitor execution time

## References

- **Legacy Rails Code**: `/Users/michaelguren/Documents/pocketpharmacist4_localRails_20211114/app/models/rxnorm_product.rb`
- **RxNORM Docs**: https://www.nlm.nih.gov/research/umls/rxnorm/docs/techdoc.html
- **Relationship Reference**: https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix1.html
- **ETL Architecture**: `infra/etl/CLAUDE.md`

---

**Status**: Ready for implementation after plan approval.
