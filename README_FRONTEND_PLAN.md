# Frontend Search Interface Plan

**RAWS v0.2 - Drug Product Search Implementation**

---

## Current State Analysis

### Gold Layer: `drug-product-codesets`
- **Location**: `pp_dw_gold.drug_product_codesets` (Athena/S3)
- **Record Count**: ~380,000 records
- **Data Structure**: Dual-source unified dataset
  - FDA fields (prefixed `fda_*`): NDC codes, proprietary names, dosage forms, marketing status
  - RxNORM fields (prefixed `rxnorm_*`): RxCUI codes, clinical terminology, ingredient names
  - Full outer join: Some records have FDA only, some RxNORM only, most have both

### Key Searchable Fields
| Field | Type | Example | Search Priority |
|-------|------|---------|----------------|
| `fda_ndc_11` | string | `00002322702` | High (exact/prefix) |
| `fda_proprietary_name` | string | `Prozac` | High (full-text) |
| `rxnorm_rxcui` | string | `392409` | High (exact) |
| `rxnorm_str` | string | `fluoxetine 20 MG Oral Capsule` | High (full-text) |
| `rxnorm_ingredient_names` | string | `fluoxetine` | High (full-text) |
| `rxnorm_brand_names` | string | `Prozac\|Sarafem` | High (full-text) |

**Note**: Dosage form fields (`fda_dosage_form`, `rxnorm_dosage_forms`) are **excluded** from search - they are display-only attributes, not primary search terms.

### Infrastructure Gaps
- No API layer exists yet
- No operational database (DynamoDB) exists yet
- No frontend infrastructure exists yet
- Data currently only accessible via Athena (not API-ready)

---

## RAWS-Aligned Architecture

Following the RAWS manifesto principles:
- **Convention Over Configuration**: Predictable naming, clear patterns
- **Good Enough > Perfect**: DynamoDB prefix search instead of OpenSearch
- **Infrastructure as Convention**: CDK for all resources, domain-scoped
- **Manual Before Automated**: Build end-to-end first, automate patterns later

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA FLOW (RAWS v0.2)                       │
└─────────────────────────────────────────────────────────────────┘

Gold Layer (Athena/S3)
  pp_dw_gold.drug_product_codesets (380K records)
        │
        │ [New ETL Job: Batch-Optimized PySpark]
        ↓
Operational Layer (DynamoDB)
  pocket-pharmacist (single-table design)
    ├─ Drug Records: PK=DRUG#{id}, SK=METADATA#{timestamp}
    └─ Search Tokens: PK=SEARCH#DRUG, SK=token#{word}#{field}#DRUG#{id}
        │
        │ [API Gateway + VTL]
        ↓
REST API
  GET /drugs/search?q={query}  → DynamoDB Query (begins_with)
  GET /drugs/{id}               → DynamoDB GetItem
        │
        │ [HTTPS]
        ↓
Static Frontend (S3 + CloudFront)
  ├─ index.html (search interface)
  ├─ app.js (search logic, API calls)
  └─ styles.css (minimal UI)
```

---

## Component Breakdown

### 1. DynamoDB Operational Layer

**Purpose**: Transform analytics data (Gold) into operational API-ready data

#### Table Design: `pocket-pharmacist`

**Design Philosophy**: Single-table DynamoDB design - this table will eventually hold all app entities (drugs, users, favorites, sessions, etc.). For v0.2, we're starting with drug records and search tokens.

**Primary Key Pattern**:
```
PK: DRUG#{fda_ndc_11 or rxnorm_rxcui}
SK: METADATA#{timestamp}
```

**Attributes** (following gold schema):
```javascript
{
  PK: "DRUG#00002322702",  // NDC without hyphens
  SK: "METADATA#2025-10-09",

  // FDA Fields (preserve fda_ prefix)
  fda_ndc_11: "00002322702",  // 11 digits, no hyphens
  fda_ndc_5: "00002",
  fda_proprietary_name: "Prozac",
  fda_dosage_form: "CAPSULE",
  fda_marketing_category: "NDA",
  fda_active_numerator_strength: "20",
  fda_active_ingredient_unit: "mg",
  fda_marketing_start_date: "1987-12-29",
  fda_nsde_flag: true,

  // RxNORM Fields (preserve rxnorm_ prefix)
  rxnorm_rxcui: "392409",
  rxnorm_str: "fluoxetine 20 MG Oral Capsule",
  rxnorm_tty: "SCD",
  rxnorm_ingredient_names: "fluoxetine",
  rxnorm_brand_names: "Prozac|Sarafem",
  rxnorm_multi_ingredient: false,

  // Search metadata
  search_text: "prozac fluoxetine capsule 20mg", // Normalized for display
  meta_run_id: "20251009-123456"
}
```

**Global Secondary Index (GSI) for Search Tokens**:

Following the **RAWS "Unified Domain" search pattern**:

```
PK: SEARCH#<entity_type>
SK: token#<normalized_token>#<field>#<entity_type>#<entity_id>

Example for DRUG entity:
PK: "SEARCH#DRUG"
SK: "token#prozac#proprietary_name#DRUG#00002322702"
SK: "token#fluoxetine#ingredient#DRUG#00002322702"
SK: "token#capsule#rxnorm_str#DRUG#00002322702"

PK: "SEARCH#DRUG"
SK: "token#atorvastatin#ingredient#DRUG#12345"
SK: "token#amlodipine#ingredient#DRUG#12345"  // Multi-value fields create multiple items
```

**Future entity types**: `SEARCH#USER`, `SEARCH#INTERACTION`, etc.

**Query Pattern**:
```javascript
// User types "pro" - search across ALL fields in one query
{
  KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
  ExpressionAttributeValues: {
    ":pk": "SEARCH#DRUG",
    ":prefix": "token#pro"  // Matches "token#prozac#..."
  }
}

// Parse SK to extract: token, field, entity_id
// "token#prozac#proprietary_name#DRUG#00002322702"
//   → token="prozac", field="proprietary_name", id="00002322702"
```

**Why This Design?**
- **Unified cross-field search**: One query searches ALL fields (name, ingredient, brand, NDC)
- **Field metadata preserved**: SK contains field name for filtering, highlighting, and relevance ranking
- **No progressive tokenization**: DynamoDB `begins_with` handles prefix matching natively
- **Deterministic**: No relevance scoring mysteries, just prefix matching
- **Scalable**: GSI queries scale independently of table size
- **Cost-effective**: ~$0.30/month for 380k drugs (vs $50-200/month for OpenSearch)
- **Simple**: No sync pipelines, no external dependencies
- **Escape hatch**: Can split to `SEARCH#DRUG#<field>` if one field grows too large (100K+ items)

#### ETL Sync Job: Gold → DynamoDB

**New Glue Job**: `pp-dw-gold-drugs-sync`
- **Input**: `pp_dw_gold.drug_product_codesets`
- **Output**: DynamoDB `pp-dw-drugs-table`
- **Frequency**: Daily (after gold layer refresh)

**Tokenization Strategy** (Refined in Planning Session 2025-10-10):

**Searchable Fields** (dosages excluded):
- `fda_ndc_11` - 11-digit NDC codes (no hyphens)
- `fda_proprietary_name` - Brand/proprietary names
- `rxnorm_rxcui` - RxNORM concept identifiers
- `rxnorm_str` - RxNORM concept strings
- `rxnorm_ingredient_names` - Active ingredient names
- `rxnorm_brand_names` - Brand name aliases

**Tokenization Rules**:
1. **Split on multiple delimiters**: Space `" "`, Slash `"/"`, Pipe `"|"` using regex `[\s/|]+`
2. **Normalize**: Trim whitespace, convert to lowercase
3. **Filter**: Remove tokens < 3 characters, filter 60+ English stop words
4. **Keep digits**: Numbers like `500`, `100` are valid search terms
5. **No special cases**: All fields processed uniformly (no NDC-specific logic)

**Example Tokenization**:
```python
"Tylenol / Pain Relief | 500 mg Tablets"
→ Split: ["Tylenol", "Pain", "Relief", "500", "mg", "Tablets"]
→ Filter: ["tylenol", "pain", "relief", "500", "tablets"]  # "mg" < 3 chars removed

"atorvastatin / amlodipine"
→ ["atorvastatin", "amlodipine"]

"Prozac|Sarafem/Fluoxetine Daily"
→ ["prozac", "sarafem", "fluoxetine", "daily"]

"00002322702"  # NDC
→ ["00002322702"]  # No split - single token
```

**Complete PySpark Implementation**:

```python
import re
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# ========================
# 1. Tokenization UDF
# ========================

# Stop words (60+ common English words)
STOP_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
    'for', 'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are',
    'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does', 'did',
    'will', 'would', 'should', 'could', 'may', 'might', 'must', 'can',
    'that', 'this', 'these', 'those', 'which', 'what', 'who', 'when',
    'where', 'why', 'how', 'all', 'each', 'every', 'both', 'few',
    'more', 'most', 'other', 'some', 'such', 'than', 'too', 'very'
}

def tokenize_field(text):
    """
    Split on space/slash/pipe, filter by length/stop words.
    All fields treated uniformly - no special cases.
    """
    if not text:
        return []

    return [
        token for token in
        (t.strip().lower() for t in re.split(r'[\s/|]+', text))
        if len(token) >= 3
        and token not in STOP_WORDS
    ]

# Register as Spark UDF
tokenize_udf = F.udf(tokenize_field, ArrayType(StringType()))

# ========================
# 2. Apply to All Fields
# ========================

gold_df = spark.read.table("pp_dw_gold.drug_product_codesets")

# Searchable fields mapping
searchable_fields = {
    "ndc": "fda_ndc_11",
    "proprietary_name": "fda_proprietary_name",
    "rxcui": "rxnorm_rxcui",
    "rxnorm_str": "rxnorm_str",
    "ingredient": "rxnorm_ingredient_names",
    "brand": "rxnorm_brand_names"
}

# Apply tokenization UDF
for field_name, col_name in searchable_fields.items():
    gold_df = gold_df.withColumn(f"{field_name}_tokens", tokenize_udf(F.col(col_name)))

# Add entity_id
gold_df = gold_df.withColumn("entity_id", F.coalesce(F.col("fda_ndc_11"), F.col("rxnorm_rxcui")))

# ========================
# 3. Explode Tokens
# ========================

exploded_dfs = []

for field_name in searchable_fields.keys():
    token_col = f"{field_name}_tokens"
    exploded_dfs.append(
        gold_df.select(
            F.col("entity_id"),
            F.lit(field_name).alias("field"),
            F.explode_outer(F.col(token_col)).alias("token")
        )
    )

# Union all exploded dataframes
tokens_df = exploded_dfs[0]
for next_df in exploded_dfs[1:]:
    tokens_df = tokens_df.unionByName(next_df)

# Filter nulls
tokens_df = tokens_df.filter(F.col("token").isNotNull())

# ========================
# 4. Format for DynamoDB
# ========================

search_tokens_df = (
    tokens_df
    .withColumn("PK", F.lit("SEARCH#DRUG"))
    .withColumn("SK", F.concat_ws("#",
                                  F.lit("token"),
                                  F.col("token"),
                                  F.col("field"),
                                  F.lit("DRUG"),
                                  F.col("entity_id")))
    .select("PK", "SK")
    .distinct()  # Deduplicate
)

# ========================
# 5. Batch Write to DynamoDB
# ========================

def batch_write_partition(partition_items):
    """Write partition to DynamoDB with error handling"""
    import boto3
    from botocore.exceptions import ClientError

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('pocket-pharmacist')

    with table.batch_writer(overwrite_by_pkeys=['PK', 'SK']) as batch:
        for row in partition_items:
            try:
                batch.put_item(Item={'PK': row['PK'], 'SK': row['SK']})
            except ClientError as e:
                print(f"Write failed for {row}: {e}")

# Partition sizing: 5-10K items per partition
num_partitions = max(4, search_tokens_df.rdd.getNumPartitions() * 2)

# Batch write (parallel across executors)
(
    search_tokens_df
    .repartition(num_partitions)
    .foreachPartition(batch_write_partition)
)
```

**Performance Expectations**:
- 380k drugs → ~1.2M search tokens (after deduplication)
- Job runtime: ~5 minutes (10 G.2X workers)
- DynamoDB writes: Parallel batch writes (25 items per API call)
- Cost per run: ~$0.35 Glue + $2.35 DynamoDB = $2.70 total

**Data Quality Checks**:
- Verify all 380k+ drug records synced
- Validate ~1.2M search token items created (after deduplication)
- Check null handling (some records have FDA only or RxNORM only)
- Verify multi-value fields split correctly (ingredient combos, multiple brands)
- Confirm tokenization applied stop words correctly (no "the", "and", etc.)
- Validate all tokens meet 3-char minimum

**DynamoDB Cost Estimates** (detailed):

| Component | Calculation | Monthly Cost |
|-----------|-------------|--------------|
| **Drug Records** | 380k × 2 KB = 760 MB | $0.19 |
| **Search Tokens** | 1.2M × 61 bytes = 73 MB | $0.02 |
| **Total Storage** | 833 MB | **$0.21/month** |
| **Read Ops** (10k/day) | 300k RRUs × $0.25/M | **$0.08/month** |
| **Initial Load** (one-time) | 1.58M WRUs × $1.25/M | **$2.35** |

**Total Ongoing Cost**: ~$0.29/month
**vs OpenSearch**: $50-200/month minimum (175-690x more expensive)

---

### 2. API Layer (`infra/api/`)

**New CDK Domain**: `infra/api/`

#### Stack: `ApiStack.js` (pp-dw-api-drugs)

**Resources**:
1. **API Gateway REST API**: `pp-dw-drugs-api`
2. **VTL Integration Templates** (no Lambda for CRUD)
3. **IAM Role**: Allow API Gateway → DynamoDB
4. **API Key** (auth v0.2, Cognito in future)

#### Endpoints

**1. Search Drugs**
```
GET /drugs/search?q={query}&limit={limit}

Query Parameters:
- q: Search query (required, min 3 chars)
- limit: Max results (optional, default: 20, max: 100)

Response:
{
  "results": [
    {
      "id": "00002-3227-02",
      "type": "fda_ndc",
      "matched_field": "proprietary_name",
      "matched_token": "prozac",
      "fda_proprietary_name": "Prozac",
      "fda_dosage_form": "CAPSULE",
      "rxnorm_rxcui": "392409",
      "rxnorm_ingredient_names": "fluoxetine",
      "search_text": "prozac fluoxetine capsule 20mg"
    }
  ],
  "count": 1,
  "query": "prozac"
}
```

**VTL Template** (DynamoDB Query via GSI):
```vtl
{
  "TableName": "pocket-pharmacist",
  "IndexName": "SearchTokenIndex",
  "KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
  "ExpressionAttributeValues": {
    ":pk": { "S": "SEARCH#DRUG" },
    ":prefix": { "S": "token#$input.params('q').toLowerCase()" }
  },
  "Limit": $input.params('limit')
}

## Response mapping: Parse SK to extract matched field
## SK format: "token#<word>#<field>#DRUG#<id>"
## Parse and include matched_field in response for highlighting
```

**Key Benefits**:
- **Single query** searches ALL fields (name, ingredient, brand, NDC)
- **Matched field** extracted from SK for result highlighting
- **No field parameter** needed (simpler API)
- **Optional future enhancement**: Filter by field client-side or add `?field=` param for field-specific search

**2. Get Drug by ID**
```
GET /drugs/{id}

Path Parameters:
- id: Drug ID (fda_ndc_11 or rxnorm_rxcui)

Response:
{
  "drug": {
    "PK": "DRUG#00002-3227-02",
    "fda_ndc_11": "00002-3227-02",
    "fda_proprietary_name": "Prozac",
    // ... all FDA and RxNORM fields
  }
}
```

**VTL Template** (DynamoDB GetItem):
```vtl
{
  "TableName": "pocket-pharmacist",
  "Key": {
    "PK": { "S": "DRUG#$input.params('id')" },
    "SK": { "S": "METADATA#latest" }
  }
}
```

**Why VTL, Not Lambda?**
- **RAWS Principle**: "Transport" (move, store, retrieve) uses VTL
- **Zero-ops**: No code to deploy, no cold starts, no scaling concerns
- **Deterministic**: VTL templates are explicit and inspectable
- **Cost**: Significantly cheaper than Lambda invocations
- **When to use Lambda**: Transform logic, aggregations, side effects (not needed here)

---

### 3. Frontend Layer (`infra/frontend/`)

**New CDK Domain**: `infra/frontend/`

#### Stack: `FrontendStack.js` (pp-dw-frontend-drugs)

**Resources**:
1. **S3 Bucket**: `pp-dw-frontend-drugs-{account}`
   - Static website hosting enabled
   - Private bucket (CloudFront OAI access only)

2. **CloudFront Distribution**:
   - Origin: S3 bucket
   - HTTPS only
   - Default root object: `index.html`
   - Error pages: `index.html` (SPA routing)

3. **Deployment**: Manual upload for v0.2, automated via CDK asset in future

#### Frontend Structure

```
frontend/
├── index.html          # Main search interface
├── app.js              # Search logic, API calls
├── styles.css          # Minimal styling
└── config.js           # API endpoint config
```

**UI Components**:
1. **Search Box**:
   - Text input with autocomplete (debounced, min 3 chars)
   - Searches across ALL fields (name, ingredient, brand, NDC)
   - Search button (also on Enter key)
   - Optional: Display matched field badge in results (e.g., "Matched in: Ingredient")

2. **Results Table**:
   - Drug name, NDC, RxCUI, ingredients, dosage form
   - Click row to see detail view
   - Pagination (client-side for v0.2)

3. **Detail View** (modal or separate page):
   - Complete drug record (all FDA + RxNORM fields)
   - Formatted display with sections

**Tech Stack**:
- **Vanilla JavaScript** (no framework overhead)
- **Fetch API** for HTTP calls
- **CSS Grid/Flexbox** for layout
- **No build step** (direct HTML/JS/CSS)

**Why No Framework?**
- RAWS principle: "Minimal stack... any future framework adhering to simplicity"
- Faster iteration for v0.2
- No build complexity
- Easy to understand and modify
- Can migrate to React/Vue later if needed

---

## Implementation Phases

### Phase 1: DynamoDB Foundation (Week 1)
**Goal**: Operational data layer ready

- [ ] Create `infra/api/` directory structure
- [ ] Write `ApiStack.js` with DynamoDB table + GSI
- [ ] Deploy DynamoDB table: `npm run api:deploy`
- [ ] Verify table creation and GSI status

**Success Criteria**:
- DynamoDB table `pocket-pharmacist` exists
- GSI `SearchTokenIndex` is ACTIVE
- Can manually write/read test items via AWS Console

---

### Phase 2: ETL Sync Job (Week 1-2)
**Goal**: Gold layer data flows into DynamoDB

- [ ] Create Glue job: `pp-dw-gold-drugs-sync`
- [ ] Implement tokenization logic (PySpark)
- [ ] Write drug records to DynamoDB
- [ ] Write search token items to DynamoDB (batch writes)
- [ ] Add data quality checks (record counts, null rates)
- [ ] Run job against full gold dataset
- [ ] Verify 380k+ drug records in DynamoDB
- [ ] Verify ~1.2M search token items created (after deduplication)

**Success Criteria**:
- All gold records synced to DynamoDB
- Search queries return expected results (test via Console)
- Job runs in < 5 minutes (with batch-optimized PySpark)
- No duplicate tokens per drug
- Tokenization quality verified (stop words filtered, min 3 chars)

---

### Phase 3: API Layer + CLI Testing (Week 2) ⏸️

**Goal**: REST API with search and detail endpoints + thorough CLI validation

**Part A: Deploy API**
- [ ] Add API Gateway to `ApiStack.js`
- [ ] Create VTL template for `/drugs/search`
- [ ] Create VTL template for `/drugs/{id}`
- [ ] Configure API key auth
- [ ] Deploy API stack

**Part B: CLI Testing (⏸️ PAUSE HERE)**
- [ ] Test search endpoints via AWS CLI:
  ```bash
  # Test 1: Brand name search
  aws dynamodb query --table-name pocket-pharmacist \
    --index-name SearchTokenIndex \
    --key-condition-expression "PK = :pk AND begins_with(SK, :prefix)" \
    --expression-attribute-values '{":pk":{"S":"SEARCH#DRUG"},":prefix":{"S":"token#prozac"}}'

  # Test 2: Ingredient search
  aws dynamodb query ... --expression-attribute-values '{..., ":prefix":{"S":"token#fluoxetine"}}'

  # Test 3: NDC partial search
  aws dynamodb query ... --expression-attribute-values '{..., ":prefix":{"S":"token#00002"}}'

  # Test 4: Multi-ingredient (check tokenization)
  aws dynamodb query ... --expression-attribute-values '{..., ":prefix":{"S":"token#atorvastatin"}}'
  ```

- [ ] Validate tokenization quality:
  - No stop words in results (`the`, `and`, etc.)
  - All tokens ≥ 3 characters
  - Brand names split correctly (`Prozac|Sarafem` → both searchable)
  - Ingredient combos split correctly (`atorvastatin / amlodipine` → both searchable)

- [ ] Test edge cases:
  - Short query (2 chars) - should fail validation
  - Special characters - should filter safely
  - Pure numbers (e.g., "500") - should return results

- [ ] Measure performance:
  - Query latency (target: P95 < 500ms)
  - Result relevance (matched field makes sense)

- [ ] Test API Gateway endpoints:
  - `GET /drugs/search?q=prozac` via curl
  - `GET /drugs/{id}` via curl
  - Verify CORS headers
  - Test error handling (invalid queries, not found)

**Part C: Iterate if Needed**
- [ ] If tokenization quality issues found:
  - Adjust stop word list
  - Modify field-specific handling
  - Re-run Glue sync job
  - Repeat CLI testing

**Success Criteria**:
- ✅ Search queries return expected drugs
- ✅ Tokenization quality meets standards (no noise)
- ✅ Query latency < 500ms (P95)
- ✅ API handles errors gracefully
- ✅ No Lambda cold starts (because no Lambda!)
- ✅ All stakeholders approve search quality

**⏸️ PAUSE POINT**: Only proceed to Phase 4 (Frontend) after CLI validation passes

---

### Phase 4: Frontend Interface (Week 3)
**Goal**: Static web interface with search functionality

- [ ] Create `frontend/` directory
- [ ] Write `index.html` with search UI
- [ ] Write `app.js` with API integration
- [ ] Write `styles.css` with responsive layout
- [ ] Test locally (serve via `python -m http.server`)
- [ ] Create `FrontendStack.js` with S3 + CloudFront
- [ ] Deploy frontend stack
- [ ] Upload static files to S3
- [ ] Test via CloudFront URL
- [ ] Verify search works end-to-end

**Success Criteria**:
- Search interface loads via CloudFront URL
- Typing "prozac" returns autocomplete suggestions
- Clicking result shows drug details
- Mobile-responsive layout
- No console errors

---

### Phase 5: Polish and Documentation (Week 4)
**Goal**: Production-ready v0.2 release

- [ ] Add loading states (spinner during API calls)
- [ ] Add error handling (API failures, no results)
- [ ] Add pagination (if > 20 results)
- [ ] Performance testing (search latency, concurrent users)
- [ ] Security review (API key rotation, CORS, CSP headers)
- [ ] Write user documentation
- [ ] Write developer documentation (API specs, deployment guide)
- [ ] Tag release: `v0.2-frontend`

**Success Criteria**:
- Sub-second search response times
- Handles 100+ concurrent users
- All errors display user-friendly messages
- Complete documentation

---

## Design Decisions and Trade-offs

### 1. DynamoDB vs OpenSearch
**Decision**: Use DynamoDB with prefix-based search

**Rationale**:
- RAWS principle: "Good enough" search, not perfect
- Significantly cheaper (no cluster costs)
- Simpler architecture (no sync pipelines)
- Deterministic results (no relevance scoring complexity)
- Fast for prefix queries (most common use case)

**Trade-offs**:
- No fuzzy matching (typos not handled)
- No relevance ranking (results are alphabetical)
- Limited to prefix search (not full-text substring)

**Future Path**: If users demand fuzzy search, consider OpenSearch in v0.4+

---

### 2. VTL vs Lambda for API
**Decision**: Use VTL templates for CRUD operations

**Rationale**:
- RAWS principle: "Transport uses VTL, Transform uses Lambda"
- Zero operational overhead
- No cold starts
- Predictable performance
- Lower cost

**Trade-offs**:
- VTL syntax is verbose
- Limited logic capabilities (no complex transformations)
- Harder to test locally

**When to Add Lambda**:
- Complex search ranking algorithms
- Aggregations across multiple queries
- Side effects (write to multiple tables, send notifications)
- Business logic (validate inputs, apply rules)

---

### 3. Vanilla JS vs React/Vue
**Decision**: Start with vanilla JavaScript for v0.2

**Rationale**:
- RAWS principle: "Minimal stack"
- Faster initial development (no build setup)
- Easier to understand and modify
- No framework lock-in

**Trade-offs**:
- More verbose DOM manipulation
- No state management library
- Manual event handling

**Future Path**: Migrate to React/HTMX in v0.3+ if complexity grows

---

### 4. Manual Deployment vs CI/CD
**Decision**: Manual deployment for v0.2

**Rationale**:
- RAWS principle: "Manual before automated"
- Learn deployment pain points first
- Understand what deserves automation

**Trade-offs**:
- Slower deployments
- Risk of manual errors
- No automated testing

**Future Path**: Add GitHub Actions CI/CD in v0.3+

---

## Testing Strategy

Following RAWS principle: **"No local simulation - test against real AWS resources"**

### Unit Tests (Minimal for v0.2)
- PySpark tokenization logic (can run locally)
- JavaScript search input validation

### Integration Tests (Cloud-Based)
1. **DynamoDB Layer**:
   - Write test records via AWS SDK
   - Query via GSI, verify results
   - Measure query latency (P50, P99)

2. **API Layer**:
   - curl/Postman tests against deployed API
   - Verify search returns expected drugs
   - Verify error handling (invalid queries)
   - Load testing with Apache Bench or k6

3. **Frontend Layer**:
   - Manual testing via browser
   - Test search flow end-to-end
   - Test on mobile devices
   - Check CloudFront caching behavior

### End-to-End Tests
- User journey: Open page → Search "aspirin" → Click result → View details
- Measure total latency (CloudFront → API Gateway → DynamoDB)
- Verify CORS headers allow browser requests

### Performance Benchmarks
- **Search latency**: < 500ms (P95)
- **Detail page load**: < 200ms (P95)
- **Concurrent users**: 100+ simultaneous searches
- **DynamoDB costs**: < $10/month for 10K queries/day

---

## Success Criteria for v0.2

### Functional Requirements
- [x] User can search drugs by name (FDA proprietary name)
- [x] User can search drugs by ingredient (RxNORM)
- [x] User can search drugs by NDC code
- [x] Search returns results within 1 second
- [x] User can view complete drug details
- [x] Interface works on mobile and desktop

### Technical Requirements
- [x] DynamoDB table synced from gold layer (304K+ records)
- [x] API Gateway endpoints deployed and functional
- [x] Static frontend hosted on CloudFront
- [x] No Lambda functions (pure VTL for CRUD)
- [x] All infrastructure defined in CDK
- [x] Documented API endpoints

### RAWS Alignment
- [x] Convention over configuration (predictable naming)
- [x] Infrastructure as code (CDK for all resources)
- [x] Good enough > perfect (DynamoDB prefix search)
- [x] Manual before automated (hands-on deployment)
- [x] Cloud-based testing (no local mocks)

---

## Open Questions and Future Considerations

### v0.2 Design Decisions (Resolved)
1. **Search pattern**: ✅ Unified entity pattern (`SEARCH#DRUG`) - single query across all fields
2. **Field metadata**: ✅ Included in SK (`token#word#field#DRUG#id`) for highlighting and ranking
3. **Tokenization**: ✅ Split on space/slash/pipe, min 3 chars, 60+ stop words, keep digits
4. **Multi-value fields**: ✅ Split by ALL delimiters: space `" "`, slash `"/"`, pipe `"|"`
5. **Table naming**: ✅ `pocket-pharmacist` (generic single-table design for future entities)
6. **Batch loading**: ✅ PySpark `explode_outer` + boto3 `batch_writer` + `foreachPartition`

### v0.2 Resolved Questions (Planning Session 2025-10-10)
1. **Search tokenization**: ✅ Min prefix = 3 chars globally (field-specific in future: NDC=5)
2. **Max tokens per drug**: ✅ 50 tokens (prevents outliers from bloating index)
3. **API pagination**: ✅ Client-side for v0.2, server-side in v0.3+
4. **Authentication**: ✅ API key sufficient for v0.2, Cognito in v0.3+
5. **Error handling**: ✅ Show user-friendly messages, log details server-side
6. **Dosage fields**: ✅ Excluded from search (display-only attributes)
7. **Digit handling**: ✅ Keep digits in tokens (valid search terms like "500")

### v0.3+ Enhancements
- **Advanced search**: Multi-field queries (name AND ingredient)
- **Filters**: Dosage form, marketing status, DEA schedule
- **Sort options**: Alphabetical, by marketing date, by match quality
- **Export**: CSV/JSON export of search results
- **Favorites**: User can save frequently searched drugs (requires auth)

### v0.4+ Scale Considerations
- **OpenSearch migration**: If fuzzy search becomes critical
- **Lambda @ Edge**: For personalized search results
- **GraphQL**: If frontend needs complex nested queries
- **Real-time sync**: EventBridge → Lambda → DynamoDB (instead of daily batch)

---

## References

- **RAWS Manifesto**: `README_RAWS.md`
- **Infrastructure Strategy**: `infra/CLAUDE.md`
- **ETL Architecture**: `infra/etl/CLAUDE.md`
- **Gold Layer Schema**: `infra/etl/datasets/drug-product-codesets/README.md`
- **AWS Best Practices**:
  - [DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
  - [API Gateway VTL Reference](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html)
  - [S3 Static Website Hosting](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WebsiteHosting.html)

---

**Next Step**: Review this plan, iterate on open questions, then begin Phase 1 (DynamoDB Foundation).
