# ⚠️ BOOTSTRAP ORCHESTRATION ONLY

**This orchestration is for ONE-TIME INITIAL DATA WAREHOUSE SETUP ONLY.**

## What This Does

Runs all ETL jobs (bronze → silver → gold) sequentially to populate your data warehouse with complete datasets from all sources.

## When to Use

- ✅ First-time data warehouse initialization
- ✅ After major schema changes
- ✅ Testing in lower environments
- ✅ Full data refresh validation

## When NOT to Use

- ❌ Daily production data updates (use production orchestration instead)
- ❌ Incremental updates to existing data
- ❌ Regular scheduled runs

## Execution

```bash
# 1. Deploy infrastructure
npm run etl:deploy

# 2. Run one-time bootstrap
# Navigate to Step Functions → pp-dw-etl-bootstrap-orchestration
# Click "Start Execution" → Copy execution input JSON → Submit

# 3. Wait for completion (~60-90 minutes)

# 4. Verify data in Athena
SELECT COUNT(*) FROM pp_dw_bronze.fda_nsde;
SELECT COUNT(*) FROM pp_dw_silver.rxnorm_products;
SELECT COUNT(*) FROM pp_dw_gold.rxnorm_products WHERE status = 'current';
```

## Stack Name

```
pp-dw-etl-bootstrap-orchestration
```

## What Happens

1. All 6 bronze jobs run in parallel
2. Tables checked, crawlers run if needed
3. All 3 silver jobs run in parallel
4. Tables checked, crawlers run if needed
5. 4 gold jobs run sequentially (in order)
6. Tables checked, crawlers run if needed
7. Success → Data warehouse initialized

## After Bootstrap

For daily production updates, we will create a separate **Production Orchestration** that runs incrementally and is scheduled via EventBridge.

---

**Created**: October 2025
