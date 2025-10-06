# RxClass Drug Members Dataset

## Overview
Maps RxClass drug classifications to specific RxNORM drug concepts (RXCUIs). Each row represents a drug that belongs to a specific class, with the relationship type captured in `rela` and `rela_source`.

**Source:** NLM RxNav RxClass API - `classMembers` endpoint
**Update Frequency:** Monthly (after rxclass completes)
**Processing Mode:** Distributed Spark (`mapPartitions`) across 10 workers
**Dependencies:** Requires `pp_dw_bronze.rxclass` table

---

## Schema

```sql
rxclass_drug_members (
  class_id        STRING,   -- RxClass identifier (FK to rxclass.class_id)
  rxcui           STRING,   -- RxNORM drug concept ID
  name            STRING,   -- Drug name
  tty             STRING,   -- Term type (IN, SCD, SBD, etc.)
  source_id       STRING,   -- Source-specific identifier
  rela_source     STRING,   -- Relationship source (DAILYMED, MEDRT, ATC, VA, etc.)
  rela            STRING,   -- Relationship type (has_EPC, may_treat, CI_with, etc.)
  meta_run_id     STRING    -- ETL run identifier for lineage
)
```

---

## API Parameter Mapping by Class Type

The RxNav `classMembers` API requires different parameters based on `class_type`. Our `get_api_params()` function maps:

### Working Mappings ✅

| class_type | relaSource | rela | Success Rate | Notes |
|------------|------------|------|--------------|-------|
| **EPC** | DAILYMED | has_EPC | 88% | Established Pharmacologic Class |
| **MOA** | DAILYMED | has_MoA | 50% | Mechanism of Action |
| **ATC1-4** | ATC | (none) | 54% | Anatomical Therapeutic Chemical |
| **DISEASE** | MEDRT | (varies) | 18% | Disease/indication relationships |
| **SCHEDULE** | RXNORM | (none) | 67% | DEA schedules (only 6 classes total) |

### Broken/Problematic Mappings ❌

| class_type | relaSource | rela | Success Rate | Issue |
|------------|------------|------|--------------|-------|
| **VA** | VA | (none) | 1.6% | ⚠️ Wrong parameters - needs investigation |
| **CHEM** | DAILYMED | has_chemical_structure | 2.5% | ⚠️ Most classes return empty |
| **PE** | DAILYMED | has_PE | 6.8% | ⚠️ Physiologic Effect - low success |
| **TC** | TC | (none) | 0% | ❌ No results - wrong source |
| **CVX** | CVX | (none) | 0% | ❌ No results - wrong source |
| **STRUCT** | STRUCT | (none) | 0% | ❌ No results - wrong source |
| **DISPOS** | MEDRT | (none) | 0% | ❌ No results - needs rela? |
| **PK** | PK | (none) | 0% | ❌ No results - wrong source |

---

## Code Implementation

### API Parameter Function
```python
def get_api_params(class_type):
    """Map class_type to RxNav API parameters"""
    class_type_upper = class_type.upper() if class_type else ""

    # DAILYMED relationships
    if class_type_upper == 'EPC':
        return {'relaSource': 'DAILYMED', 'rela': 'has_EPC', 'trans': '1'}
    if class_type_upper == 'MOA':
        return {'relaSource': 'DAILYMED', 'rela': 'has_MoA', 'trans': '1'}
    if class_type_upper == 'PE':
        return {'relaSource': 'DAILYMED', 'rela': 'has_PE', 'trans': '1'}
    if class_type_upper == 'CHEM':
        return {'relaSource': 'DAILYMED', 'rela': 'has_chemical_structure', 'trans': '1'}

    # ATC classifications
    if 'ATC' in class_type_upper:
        return {'relaSource': 'ATC', 'trans': '1'}

    # VA classifications
    if 'VA' in class_type_upper:
        return {'relaSource': 'VA', 'trans': '1'}

    # MESH/Disease classifications
    if class_type_upper in ['DISEASE', 'DISPOS']:
        return {'relaSource': 'MEDRT', 'trans': '1'}

    # MESH
    if 'MESH' in class_type_upper:
        return {'relaSource': 'MESH', 'trans': '1'}

    # SNOMED
    if 'SNOMED' in class_type_upper:
        return {'relaSource': 'SNOMEDCT', 'trans': '1'}

    # Schedule
    if 'SCHEDULE' in class_type_upper:
        return {'relaSource': 'RXNORM', 'trans': '1'}

    # Default fallback
    return {'relaSource': class_type, 'trans': '1'}
```

### Example API Call
```bash
# Working example (EPC)
curl "https://rxnav.nlm.nih.gov/REST/rxclass/classMembers.json?classId=N0000175413&relaSource=DAILYMED&rela=has_EPC&trans=1"

# Broken example (VA)
curl "https://rxnav.nlm.nih.gov/REST/rxclass/classMembers.json?classId=AD000&relaSource=VA&trans=1"
# Returns: {}  (empty - wrong parameters!)
```

---

## Data Quality Metrics

**Current Results (as of 2025-10-06):**
- **Total Classes:** 22,430
- **Classes with Members:** 3,203 (14.3%)
- **Total Drug Members:** 21,785
- **Average Members per Class:** 6.8

### Breakdown by Class Type

| Class Type | Total Classes | With Members | Total Members | Avg/Class |
|------------|--------------|--------------|---------------|-----------|
| DISEASE | 5,983 | 1,078 (18%) | 7,778 | 7.22 |
| ATC1-4 | 1,316 | 711 (54%) | 3,703 | 5.21 |
| EPC | 715 | 630 (88%) | 2,499 | 3.97 |
| CHEM | 10,261 | 256 (2.5%) | 2,401 | 9.38 |
| PE | 1,873 | 128 (6.8%) | 2,245 | 17.54 |
| SCHEDULE | 6 | 4 (67%) | 1,601 | 400.25 |
| MOA | 770 | 387 (50%) | 1,542 | 3.98 |
| VA | 576 | 9 (1.6%) | 16 | 1.78 |
| TC | 66 | 0 (0%) | 0 | - |
| CVX | 230 | 0 (0%) | 0 | - |
| STRUCT | 274 | 0 (0%) | 0 | - |
| DISPOS | 301 | 0 (0%) | 0 | - |
| PK | 59 | 0 (0%) | 0 | - |

---

## Example Queries

### Find all drugs in a specific class
```sql
SELECT rxcui, name, tty, rela
FROM pp_dw_bronze.rxclass_drug_members
WHERE class_id = 'N0000175413'  -- Platinum-based Drugs (EPC)
ORDER BY name;
-- Returns: cisplatin, oxaliplatin, carboplatin
```

### Find all classes for a specific drug
```sql
SELECT c.class_name, c.class_type, dm.rela
FROM pp_dw_bronze.rxclass_drug_members dm
JOIN pp_dw_bronze.rxclass c ON dm.class_id = c.class_id
WHERE dm.rxcui = '2555'  -- cisplatin
ORDER BY c.class_type, c.class_name;
```

### Drugs that treat a specific disease
```sql
SELECT DISTINCT dm.rxcui, dm.name, dm.tty
FROM pp_dw_bronze.rxclass c
JOIN pp_dw_bronze.rxclass_drug_members dm ON c.class_id = dm.class_id
WHERE c.class_type = 'DISEASE'
  AND c.class_name LIKE '%Hypertension%'
  AND dm.rela = 'may_treat'
ORDER BY dm.name;
```

### Contraindications for a drug
```sql
SELECT c.class_name as contraindicated_condition
FROM pp_dw_bronze.rxclass_drug_members dm
JOIN pp_dw_bronze.rxclass c ON dm.class_id = c.class_id
WHERE dm.rxcui = '2555'  -- cisplatin
  AND dm.rela = 'CI_with'
  AND c.class_type = 'DISEASE';
```

---

## Known Issues & Proposed Architecture Change

### 🚨 Critical Issue: Incomplete Data Due to API Approach

**Root Cause Analysis (2025-10-06)**:

The current **class-first approach** has fundamental limitations:

1. **Missing `rela` parameters** - Many class types require specific `rela` values:
   - DISEASE: needs `may_treat` (currently missing → only 18% success)
   - VA: needs `has_VAClass` (currently missing → only 1.6% success)
   - DISPOS, PK, etc.: need unknown rela values (0% success)

2. **Multiple relationships per class** - Some class types have MULTIPLE valid `rela` values:
   - DISEASE supports: `may_treat`, `CI_with`, `may_prevent`, `may_diagnose`
   - Current approach can only query ONE rela per class
   - Would need 4+ API calls per DISEASE class for complete data

3. **Incomplete coverage** - Even with fixes:
   - 22,430 classes × 1-4 API calls = **30K-90K API calls needed**
   - Still missing relationships we don't know exist
   - Current result: only **21,785 total relationships**

**Example**: Cisplatin (RXCUI 2555) has 35+ class relationships, but class-first approach only captures ~3-5.

---

## ✅ Recommended Solution: Drug-First Approach

### Why Switch to `getClassByRxNormDrugId` API?

**API Endpoint**: `https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}`

**Single API Call Returns ALL Relationships**:
```json
{
  "rxclassDrugInfoList": {
    "rxclassDrugInfo": [
      {
        "minConcept": {"rxcui": "2555", "name": "cisplatin", "tty": "IN"},
        "rxclassMinConceptItem": {"classId": "N0000175413", "className": "Platinum-based Drug", "classType": "EPC"},
        "rela": "has_epc",
        "relaSource": "DAILYMED"
      },
      {
        "minConcept": {"rxcui": "2555", "name": "cisplatin", "tty": "IN"},
        "rxclassMinConceptItem": {"classId": "D003920", "className": "Diabetes Mellitus", "classType": "DISEASE"},
        "rela": "may_treat",
        "relaSource": "MEDRT"
      },
      // ... 33+ more relationships (all rela types, all classes)
    ]
  }
}
```

### Comparison: Class-First vs Drug-First

| Metric | Class-First (Current) | Drug-First (Proposed) | Winner |
|--------|----------------------|----------------------|---------|
| **API Calls** | 22,430 classes × 1-4 calls = 30K-90K | ~20,000 drugs × 1 call = 20K | ✅ Drug-First |
| **Data Completeness** | ~21K relationships (incomplete) | ~500K-1M relationships (complete) | ✅ Drug-First |
| **Code Complexity** | Complex `rela` parameter mapping | No parameter mapping needed | ✅ Drug-First |
| **Runtime** | 25 min (incomplete data) | ~67 min (complete data) | ✅ Drug-First |
| **Missing Relationships** | VA, DISEASE (CI_with), unknown relas | None - API returns all | ✅ Drug-First |
| **Cost** | $0.44/run | ~$0.50/run | ≈ Same |

### Implementation Plan

**Source Data**: `pp_dw_silver.rxnorm_products` (~20K products: SCD, SBD, GPCK, BPCK)

**Processing**:
1. Read RXCUIs from `rxnorm_products` table
2. For each RXCUI, call `getClassByRxNormDrugId` API
3. Parse `rxclassDrugInfoList.rxclassDrugInfo[]` array
4. Extract: class_id, rxcui, name, tty, rela, rela_source
5. Write to bronze layer (same schema as current)

**Benefits**:
- ✅ **FEWER API calls** (20K vs 30K-90K)
- ✅ **10-50x more data** (complete relationships)
- ✅ **Simpler code** (no rela parameter logic)
- ✅ **Future-proof** (automatically captures new relationship types)
- ✅ **Same output schema** (no breaking changes)

**Estimated Results**:
- Total relationships: **500K-1M** (vs current 21K)
- Runtime: ~67 minutes (vs current 25 min incomplete)
- Cost: ~$0.50/run (vs current $0.44/run)

---

### 🐛 Issues to Fix (Class-First Approach - Current)

1. **DISEASE missing rela** - Needs `may_treat`, `CI_with`, `may_prevent`, `may_diagnose`
2. **VA missing rela** - Needs `has_VAClass` or `has_VAClass_extended`
3. **Multiple calls needed** - Each DISEASE class needs 4+ API calls for complete data
4. **Unknown class types** - TC, CVX, STRUCT, DISPOS, PK may need undocumented parameters

**Note**: These issues are SOLVED by switching to drug-first approach.

---

### 📋 Future Enhancements

1. **Switch to drug-first approach** ⭐ **HIGHEST PRIORITY**
   - Solves all data completeness issues
   - Simpler implementation
   - Better performance

2. **Historical tracking**
   - Keep previous run_id data to track when drugs added/removed from classes
   - SCD Type 2 dimension for class membership changes

3. **Data validation**
   - Compare against legacy Rails database counts
   - Automated quality checks for expected member counts

4. **Performance optimization**
   - Investigate optimal API rate (currently 5 calls/sec with 0.2s delay)
   - Consider parallel processing strategies

---

## Deployment & Testing

### Deploy
```bash
cd infra/etl
cdk deploy pp-dw-etl-rxclass-drug-members
```

### Run Job
```bash
aws glue start-job-run --job-name pp-dw-bronze-rxclass-drug-members
```

### Run Crawler
```bash
aws glue start-crawler --name pp-dw-bronze-rxclass-drug-members-crawler
```

### Validate Results
```sql
-- Row count
SELECT COUNT(*) FROM pp_dw_bronze.rxclass_drug_members;
-- Expected: ~20-25k rows

-- Check rela_source distribution
SELECT rela_source, rela, COUNT(*) as count
FROM pp_dw_bronze.rxclass_drug_members
GROUP BY rela_source, rela
ORDER BY count DESC;

-- Verify specific known class
SELECT * FROM pp_dw_bronze.rxclass_drug_members
WHERE class_id = 'N0000175413'  -- Should return 3 platinum drugs
ORDER BY name;
```

---

## References

- [RxNav RxClass API Documentation](https://lhncbc.nlm.nih.gov/RxNav/APIs/RxClassAPIs.html)
- [getClassMembers API](https://lhncbc.nlm.nih.gov/RxNav/APIs/api-RxClass.getClassMembers.html)
- [Architecture Strategy](/infra/README_ARCH_STRATEGY.md)
- [ETL Patterns](/infra/etl/CLAUDE.md)

**Last Updated:** 2025-10-06
**Status:** Production (with known data completeness issues - see "Recommended Solution: Drug-First Approach" above)
