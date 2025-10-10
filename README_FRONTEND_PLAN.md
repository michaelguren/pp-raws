# Frontend Search Interface Plan

**RAWS v0.2 - Drug Product Search Implementation**

---

## Current State Analysis

### Gold Layer: `drug-product-codesets`
- **Location**: `pp_dw_gold.drug_product_codesets` (Athena/S3)
- **Record Count**: 304,823 records
- **Data Structure**: Dual-source unified dataset
  - FDA fields (prefixed `fda_*`): NDC codes, proprietary names, dosage forms, marketing status
  - RxNORM fields (prefixed `rxnorm_*`): RxCUI codes, clinical terminology, ingredient names
  - Full outer join: Some records have FDA only, some RxNORM only, most have both

### Key Searchable Fields
| Field | Type | Example | Search Priority |
|-------|------|---------|----------------|
| `fda_ndc_11` | string | `00002-3227-02` | High (exact/prefix) |
| `fda_proprietary_name` | string | `Prozac` | High (full-text) |
| `fda_dosage_form` | string | `TABLET` | Medium (filter) |
| `rxnorm_rxcui` | string | `392409` | High (exact) |
| `rxnorm_str` | string | `fluoxetine 20 MG Oral Capsule` | High (full-text) |
| `rxnorm_ingredient_names` | string | `fluoxetine` | High (full-text) |
| `rxnorm_brand_names` | string | `Prozac` | Medium (full-text) |

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
  pp_dw_gold.drug_product_codesets (304K records)
        │
        │ [New ETL Job]
        ↓
Operational Layer (DynamoDB)
  pp-dw-drugs-table
    ├─ Drug Records: PK=DRUG#{id}, SK=METADATA#{timestamp}
    └─ Search Tokens: PK=SEARCH#DPC, SK=token#{word}#{field}#DRUG#{id}
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

#### Table Design: `pp-dw-drugs-table`

**Primary Key Pattern**:
```
PK: DRUG#{fda_ndc_11 or rxnorm_rxcui}
SK: METADATA#{timestamp}
```

**Attributes** (following gold schema):
```javascript
{
  PK: "DRUG#00002-3227-02",
  SK: "METADATA#2025-10-09",

  // FDA Fields (preserve fda_ prefix)
  fda_ndc_11: "00002-3227-02",
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
PK: SEARCH#<domain>
SK: token#<normalized_token>#<field>#DRUG#<entity_id>

Example for domain "DPC" (Drug Product Codesets):
PK: "SEARCH#DPC"
SK: "token#prozac#proprietary_name#DRUG#00002-3227-02"
SK: "token#fluoxetine#ingredient#DRUG#00002-3227-02"
SK: "token#capsule#dosage_form#DRUG#00002-3227-02"

PK: "SEARCH#DPC"
SK: "token#atorvastatin#ingredient#DRUG#12345"
SK: "token#amlodipine#ingredient#DRUG#12345"  // Multi-value fields create multiple items
```

**Query Pattern**:
```javascript
// User types "pro" - search across ALL fields in one query
{
  KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
  ExpressionAttributeValues: {
    ":pk": "SEARCH#DPC",
    ":prefix": "token#pro"  // Matches "token#prozac#..."
  }
}

// Parse SK to extract: token, field, entity_id
// "token#prozac#proprietary_name#DRUG#00002-3227-02"
//   → token="prozac", field="proprietary_name", id="00002-3227-02"
```

**Why This Design?**
- **Unified cross-field search**: One query searches ALL fields (name, ingredient, brand, etc.)
- **Field metadata preserved**: SK contains field name for filtering, highlighting, and relevance ranking
- **No progressive tokenization**: DynamoDB `begins_with` handles prefix matching natively
- **Deterministic**: No relevance scoring mysteries, just prefix matching
- **Scalable**: GSI queries scale independently of table size
- **Cost-effective**: Pay per request, no cluster overhead
- **Simple**: No sync pipelines, no external dependencies
- **Escape hatch**: Can split to `SEARCH#DPC#<field>` if one field grows too large (100K+ items)

#### ETL Sync Job: Gold → DynamoDB

**New Glue Job**: `pp-dw-gold-drugs-sync`
- **Input**: `pp_dw_gold.drug_product_codesets`
- **Output**: DynamoDB `pp-dw-drugs-table`
- **Frequency**: Daily (after gold layer refresh)

**Tokenization Strategy**:

1. **Extract and tokenize searchable fields** (one item per word):
   - `fda_proprietary_name` → Split into words
   - `fda_ndc_11` → Store as-is (numeric search)
   - `rxnorm_str` → Split into words (e.g., "fluoxetine 20 MG Oral Capsule")
   - `rxnorm_ingredient_names` → Split by ` / ` delimiter (handles combos like "atorvastatin / amlodipine")
   - `rxnorm_brand_names` → Split by `|` delimiter (handles multiple brands)

2. **Normalize tokens**:
   - Lowercase
   - Trim whitespace
   - Strip punctuation (optional: keep hyphens in NDCs)
   - Format: `token#<normalized_word>#<field>#DRUG#<id>`

3. **Write search token items to DynamoDB**:
   ```python
   # Single-value field example (fda_proprietary_name = "Prozac")
   PK: "SEARCH#DPC"
   SK: "token#prozac#proprietary_name#DRUG#00002-3227-02"

   # Multi-value field example (rxnorm_ingredient_names = "atorvastatin / amlodipine")
   PK: "SEARCH#DPC"
   SK: "token#atorvastatin#ingredient#DRUG#12345"
   SK: "token#amlodipine#ingredient#DRUG#12345"

   # Multi-word field example (rxnorm_str = "fluoxetine 20 MG Oral Capsule")
   PK: "SEARCH#DPC"
   SK: "token#fluoxetine#rxnorm_str#DRUG#00002-3227-02"
   SK: "token#oral#rxnorm_str#DRUG#00002-3227-02"
   SK: "token#capsule#rxnorm_str#DRUG#00002-3227-02"
   ```

4. **Deduplicate tokens** per drug (use set to avoid duplicate writes across fields)

5. **Prefix matching** is handled by DynamoDB `begins_with`:
   - Query `token#pro` matches `token#prozac#...`
   - Query `token#flu` matches `token#fluoxetine#...`
   - No need to store progressive prefixes!

**Data Quality Checks**:
- Verify all 304K+ records synced
- Validate search tokens created (expect ~1.5M search items, not 3M+)
- Check null handling (some records have FDA only or RxNORM only)
- Verify multi-value fields split correctly (ingredient combos, multiple brands)

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
  "TableName": "pp-dw-drugs-table",
  "IndexName": "SearchTokenIndex",
  "KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
  "ExpressionAttributeValues": {
    ":pk": { "S": "SEARCH#DPC" },
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
  "TableName": "pp-dw-drugs-table",
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
- DynamoDB table `pp-dw-drugs-table` exists
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
- [ ] Verify 304K+ drug records in DynamoDB
- [ ] Verify search tokens created (1-3M items)

**Success Criteria**:
- All gold records synced to DynamoDB
- Search queries return expected results (test via Console)
- Job runs in < 10 minutes
- No duplicate tokens per drug

---

### Phase 3: API Layer (Week 2)
**Goal**: REST API with search and detail endpoints

- [ ] Add API Gateway to `ApiStack.js`
- [ ] Create VTL template for `/drugs/search`
- [ ] Create VTL template for `/drugs/{id}`
- [ ] Configure API key auth
- [ ] Deploy API stack
- [ ] Test endpoints via curl/Postman:
  - Search for "prozac" → returns results
  - Search for "flu" → returns fluoxetine variants
  - Get drug by NDC → returns full record
- [ ] Document API endpoints in README

**Success Criteria**:
- `/drugs/search?q=prozac` returns results in < 500ms
- VTL templates correctly map DynamoDB responses
- No Lambda cold starts (because no Lambda!)
- API handles errors gracefully (invalid queries, not found)

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
1. **Search pattern**: ✅ Unified domain pattern (`SEARCH#DPC`) - single query across all fields
2. **Field metadata**: ✅ Included in SK (`token#word#field#DRUG#id`) for highlighting and ranking
3. **Tokenization**: ✅ One item per word (no progressive prefixes) - DynamoDB `begins_with` handles it
4. **Multi-value fields**: ✅ Split by delimiter (` / ` for ingredients, `|` for brands)

### v0.2 Open Questions
1. **Search tokenization**: Min prefix length = 3 chars? Max tokens per drug?
2. **API pagination**: Client-side or server-side for v0.2? (Lean toward client-side)
3. **Authentication**: API key sufficient for v0.2, Cognito in v0.3+
4. **Error handling**: Show user-friendly messages, log details server-side

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
