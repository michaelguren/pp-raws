# Pocket Pharmacist Data Warehouse Architecture Strategy

## Vision
Build a lightning-fast drug lookup web application for students, healthcare professionals, and consumers by combining FDA, RxNORM, RxClass, and DailyMed data with custom content.

**Key Principle:** Cache everything locally for speed, analytics, and data enrichment.

---

## Two-Tier Data Architecture

### Tier 1: **Data Warehouse** (Analytical - S3 + Athena)
**Purpose:**
- Business intelligence and analytics
- QA pocket pharmacist custom content
- Historical trending and compliance tracking
- Data exploration and enrichment
- Future external API product
- Complex joins/aggregations across datasets

**Technology Stack:**
- **Storage:** S3 (bronze/silver/gold layers)
- **Query Engine:** Athena
- **ETL:** AWS Glue
- **Orchestration:** EventBridge Scheduler + Step Functions (future)

**Latency:** Seconds to minutes (acceptable for analytics)

**Update Frequency:** Monthly (after source data updates)

---

### Tier 2: **Operational Database** (Web UI - DynamoDB)
**Purpose:**
- Real-time drug lookups (<50ms response time)
- Search and autocomplete
- Serving web and mobile applications
- High-throughput, low-latency queries

**Technology Stack:**
- **Database:** DynamoDB with GSIs
- **API:** API Gateway + Lambda
- **CDN:** CloudFront (caching)

**Latency:** 5-50 milliseconds (required for web UI)

**Update Frequency:** Synced from gold layer monthly

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SOURCES (External)                       │
│  FDA NSDE/CDER │ RxNORM │ RxClass │ DailyMed │ Custom Content   │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data - S3)                  │
│                                                                   │
│  • Raw downloads preserved for lineage (run_id partitions)       │
│  • Minimal transformation (CSV → Parquet, type casting)          │
│  • One table per source file                                     │
│  • Glue Crawlers auto-discover schemas                           │
│                                                                   │
│  Tables: fda_nsde, fda_cder, rxnconso, rxnrel, rxnsat,          │
│          rxclass, rxclass_drug_members, rxnorm_spl_mappings      │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                 GOLD LAYER (Analytics-Ready - S3)                │
│                                                                   │
│  • Denormalized, business-ready tables                           │
│  • Complex joins pre-computed                                    │
│  • Used for analytics, reporting, QA                             │
│                                                                   │
│  Key Table: drug_lookup_master (denormalized "god table")        │
│    - NDC products + RxNORM + RxClass + Custom content            │
│    - Arrays for multi-valued attributes (diseases, MOAs, etc.)   │
│    - ~100k rows (one per product)                                │
└─────────────────────────────────────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
        ┌───────────────────┐       ┌───────────────────┐
        │  ATHENA QUERIES   │       │  DYNAMODB (Sync)  │
        │   (Analytics)     │       │   (Web UI Data)   │
        └───────────────────┘       └───────────────────┘
                                              │
                                              ▼
                                    ┌─────────────────────┐
                                    │   API Gateway       │
                                    │      Lambda         │
                                    │   CloudFront CDN    │
                                    └─────────────────────┘
                                              │
                                              ▼
                                    ┌─────────────────────┐
                                    │   WEB/MOBILE APP    │
                                    │  (Pocket Pharmacist)│
                                    └─────────────────────┘
```

---

## Key Datasets

### Bronze Layer
| Dataset | Source | Description | Update Frequency |
|---------|--------|-------------|------------------|
| `fda-nsde` | FDA ZIP | Comprehensive NDC SPL Data Elements | Monthly |
| `fda-cder` | FDA ZIP | National Drug Code Directory | Monthly |
| `rxnorm` | UMLS | Drug nomenclature (RRF files) | Monthly |
| `rxclass` | RxNav API | Drug classifications (ATC, MOA, EPC, etc.) | Monthly |
| `rxclass-drug-members` | RxNav API | Class → Drug relationships with `rela` | Monthly |
| `rxnorm-spl-mappings` | DailyMed API | RxCUI → SPL SET ID mappings | Monthly |

### Gold Layer (Planned)
| Table | Description | Use Case |
|-------|-------------|----------|
| `drug_lookup_master` | Denormalized "god table" with all drug info | Web UI queries, analytics |
| `drug_interactions` | Pre-computed drug-drug interactions | Interaction checking |
| `disease_drug_index` | Disease → Drugs reverse index | Therapeutic browsing |

---

## Bronze Layer: RxClass Relationships

### Current Implementation
```sql
-- rxclass: The class catalog
class_id, class_name, class_type, meta_run_id

-- rxclass_drug_members: Class → Drugs (with relationship metadata)
class_id, class_type, rxcui, name, tty,
rela_source,  -- WHERE relationship comes from (DAILYMED, MEDRT, ATC, VA)
rela,         -- WHAT the relationship is (has_EPC, may_treat, CI_with, etc.)
source_id, meta_run_id
```

### Relationship Types by Class Type
- **EPC/MOA/PE/CHEM**: `has_EPC`, `has_MoA`, `has_PE`, `has_chemical_structure`
- **DISEASE**: `may_treat`, `may_prevent`, `CI_with` (contraindicated)
- **ATC**: hierarchical membership (no explicit rela)
- **VA**: VA classification membership

---

## Gold Layer: drug_lookup_master Design

### Schema
```sql
CREATE TABLE gold.drug_lookup_master AS
SELECT
  -- Identifiers
  ndc.ndc_code,
  ndc.product_name,
  ndc.labeler_name,
  rxn.rxcui,
  rxn.generic_name,
  rxn.tty,

  -- Drug Classifications (aggregated arrays)
  ARRAY_AGG(DISTINCT rc.class_name) FILTER (WHERE rc.class_type = 'ATC1-4')
    as atc_classes,
  ARRAY_AGG(DISTINCT rc.class_name) FILTER (WHERE rc.class_type = 'MOA')
    as mechanisms_of_action,
  ARRAY_AGG(DISTINCT rc.class_name) FILTER (WHERE rc.class_type = 'EPC')
    as pharm_classes,
  ARRAY_AGG(DISTINCT rc.class_name) FILTER (WHERE rc.class_type = 'PE')
    as physiologic_effects,
  ARRAY_AGG(DISTINCT rc.class_name) FILTER (
    WHERE rc.class_type = 'DISEASE' AND rdm.rela = 'may_treat'
  ) as treats_diseases,
  ARRAY_AGG(DISTINCT rc.class_name) FILTER (
    WHERE rc.class_type = 'DISEASE' AND rdm.rela = 'CI_with'
  ) as contraindications,

  -- Custom Content (joins to your tables)
  custom.summary,
  custom.patient_counseling,
  custom.interactions_summary,

  -- Metadata
  ndc.updated_at,
  CURRENT_TIMESTAMP as gold_created_at

FROM fda_all_ndc ndc
JOIN rxnorm_products rxn ON ndc.rxcui = rxn.rxcui
LEFT JOIN rxclass_drug_members rdm ON rxn.rxcui = rdm.rxcui
LEFT JOIN rxclass rc ON rdm.class_id = rc.class_id
LEFT JOIN custom_drug_content custom ON rxn.rxcui = custom.rxcui
GROUP BY ndc.ndc_code, rxn.rxcui, custom.summary, ...
```

### Query Performance
- **Single drug lookup:** <10ms (from DynamoDB)
- **Analytics query:** Seconds (from Athena on S3)

---

## DynamoDB Design for Web UI

### Primary Table
```
Table: drug-lookup
Partition Key: PK (String)
Sort Key: SK (String)

Item Pattern:
{
  "PK": "RXCUI#7052",
  "SK": "METADATA",
  "ndc": "0378-5270-93",
  "product_name": "Lisinopril 10mg Tablet",
  "generic_name": "lisinopril",
  "atc_classes": ["C09AA03"],
  "mechanisms_of_action": ["Angiotensin Converting Enzyme Inhibitor"],
  "pharm_classes": ["ACE Inhibitor"],
  "treats_diseases": ["Hypertension", "Heart Failure"],
  "contraindications": ["Angioedema"],
  "summary": "<your custom content>",
  ...
}
```

### Global Secondary Indexes (GSIs)

**GSI 1: NDC Lookup**
```
PK: NDC#<ndc>
SK: METADATA
```

**GSI 2: Name Search/Autocomplete**
```
PK: NAME#<first_letter>
SK: <product_name>
```

**GSI 3: Generic Name Search**
```
PK: GENERIC#<first_letter>
SK: <generic_name>
```

### Sync Strategy
```python
# Glue job after gold layer completes
gold_df = spark.read.parquet("s3://.../gold/drug_lookup_master/")

# Write to S3 (for analytics)
gold_df.write.mode("overwrite").parquet("s3://.../gold/drug_lookup_master/")

# ALSO sync to DynamoDB (for web UI)
import boto3
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('drug-lookup')

for row in gold_df.toLocalIterator():  # Memory efficient
    table.put_item(Item={
        'PK': f'RXCUI#{row.rxcui}',
        'SK': 'METADATA',
        # ... all fields
    })
```

---

## Web API Architecture

```
User Request
    ↓
CloudFront (CDN caching)
    ↓
API Gateway
    ↓
Lambda Function
    ↓
DynamoDB Query
    ↓
JSON Response (5-50ms total)
```

### Example API Endpoints
```
GET /api/drugs/search?q=lisinopril
  → Lambda queries DynamoDB GSI 2 (name search)
  → Returns top 20 matches

GET /api/drugs/rxcui/{rxcui}
  → Lambda queries DynamoDB by PK
  → Returns complete drug object

GET /api/drugs/ndc/{ndc}
  → Lambda queries DynamoDB GSI 1 (NDC lookup)
  → Returns product details
```

---

## Implementation Roadmap

### ✅ Phase 1: Data Warehouse Foundation (Current)
- [x] ETL Core infrastructure (S3, Glue, IAM)
- [x] Bronze layer: FDA NSDE, CDER, RxNORM
- [x] Gold layer: fda-all-ndc (joined NDC products)
- [x] Bronze layer: RxClass, RxClass drug members
- [ ] **IN PROGRESS:** Add `rela` and `rela_source` to rxclass-drug-members
- [ ] Remove CHEM/DISEASE filter (include all class types)

### 🔄 Phase 2: Gold Layer - drug_lookup_master
- [ ] Create gold Glue job to build denormalized table
- [ ] Join: FDA NDC + RxNORM + RxClass + Custom content
- [ ] Aggregate arrays for multi-valued attributes
- [ ] Test with sample queries in Athena

### 📋 Phase 3: Operational Database Sync
- [ ] Create DynamoDB table with GSIs
- [ ] Build Glue job to sync gold → DynamoDB
- [ ] Test batch write performance
- [ ] Schedule monthly sync after gold layer

### 📋 Phase 4: Web API Layer
- [ ] API Gateway + Lambda infrastructure (CDK)
- [ ] Lambda functions for drug search/lookup
- [ ] CloudFront distribution for caching
- [ ] API documentation (OpenAPI/Swagger)

### 📋 Phase 5: Web Application
- [ ] React/Next.js frontend
- [ ] Drug search and detail pages
- [ ] Integrate custom pocket pharmacist content
- [ ] Mobile-responsive design

---

## Design Decisions

### Why Cache Data (vs. Real-time API calls)?
- ✅ **Analytics impossible** with external APIs (can't join FDA + RxClass)
- ✅ **Performance:** Local SQL = 10ms, API calls = 200-500ms
- ✅ **Integration:** Join 10+ datasets locally for enrichment
- ✅ **Historical tracking:** See how drug classifications change over time
- ✅ **Compliance:** Raw data lineage with run_id partitions
- ✅ **Custom enrichment:** Add your own content alongside source data

### Why Two-Tier Architecture (Warehouse + DynamoDB)?
- **S3/Athena:** Perfect for analytics, cheap storage, complex SQL
- **DynamoDB:** Perfect for web UI, <10ms queries, infinite scale
- **Separation of concerns:** Analytical workloads don't impact UI performance

### Why DynamoDB over RDS/Aurora?
- **Latency:** 5-10ms (DynamoDB) vs 50-100ms (Aurora)
- **Serverless:** No connection pooling or cold starts
- **Cost:** Pay-per-request at low volumes
- **Scale:** Infinite without capacity planning
- **Trade-off:** Less flexible queries (but gold layer pre-computes what we need)

### Why Monthly Updates?
- RxNORM: First Monday of each month
- FDA NDC: Monthly updates
- RxClass: Changes infrequently
- **Result:** Monthly ETL → DynamoDB sync is sufficient

---

## Cost Estimates (Rough)

### Data Warehouse (Monthly)
- S3 storage: ~100GB = $2-3/month
- Glue ETL jobs: ~6 jobs × 10 DPU-hours = $15-20/month
- Athena queries: ~100 queries × 1GB scanned = $0.50/month
- **Total:** ~$20-25/month

### Operational Database (Monthly)
- DynamoDB: 100k items, 1KB avg = $0.25 storage
- API calls: 1M reads at on-demand = $1.25/month
- Lambda: 1M invocations = $0.20/month
- API Gateway: 1M requests = $3.50/month
- CloudFront: 10GB transfer = $1/month
- **Total:** ~$6-7/month (at low volume)

### Total: ~$30/month for entire stack (scales with usage)

---

## Success Metrics

### Data Warehouse
- ✅ All source data refreshed monthly
- ✅ <5 minute query times for complex analytics
- ✅ 100% data lineage with run_id tracking
- ✅ Zero data quality issues flagged

### Web UI
- ✅ <50ms API response time (p95)
- ✅ <100ms page load time
- ✅ 99.9% uptime
- ✅ Support 1000+ concurrent users

---

## Future Enhancements

1. **Drug-Drug Interactions:** Build interaction matrix from multiple sources
2. **Real-time Alerts:** EventBridge → SNS for FDA recalls/updates
3. **AI Summaries:** Use Bedrock to generate patient-friendly drug summaries
4. **External API Product:** Sell access to enriched drug data API
5. **International Expansion:** Add EU, Canada drug databases
6. **Clinical Decision Support:** Dosing calculators, contraindication checking

---

## References
- [ETL Architecture](./etl/CLAUDE.md)
- [Infrastructure Overview](./CLAUDE.md)
- [RxNav API Docs](https://lhncbc.nlm.nih.gov/RxNav/APIs/RxClassAPIs.html)
- [FDA NDC Directory](https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory)
- [RxNORM Documentation](https://www.nlm.nih.gov/research/umls/rxnorm/docs/index.html)

---

**Last Updated:** 2025-01-05
**Status:** Phase 1 in progress (Bronze layer completion)
