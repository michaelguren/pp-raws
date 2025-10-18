# Monthly Orchestration (Placeholder)

## Purpose
Monthly full refresh orchestration for RxNorm datasets that update monthly (first Monday of each month).

## Datasets
- `rxnorm` - RxNORM RRF tables (RXNCONSO, RXNREL, RXNSAT)
- `rxclass` - RxClass drug classifications
- `rxclass-drug-members` - Drug-to-class mappings
- `rxnorm-spl-mappings` - RxNorm SPL mappings
- `rxnorm-products` - Silver layer prescribable products
- `rxnorm-ndc-mappings` - Silver layer NDC mappings
- `rxnorm-product-classifications` - Gold layer classifications
- Gold layer temporal versioned datasets

## Orchestration Flow (Future)
1. **Bronze Layer** - Download and process RxNorm data (requires UMLS API key)
2. **Check Tables** - Verify bronze tables exist, run crawlers if needed
3. **Silver Layer** - Transform and enrich RxNorm data
4. **Check Tables** - Verify silver tables exist, run crawlers if needed
5. **Gold Layer** - Apply temporal versioning (full refresh with change detection)
6. **DynamoDB Sync** - Sync changes to DynamoDB

## Trigger
- EventBridge scheduled rule: Monthly on 5th at 4 AM UTC (after RxNorm release)
- Manual execution via Step Functions console

## Status
Not yet implemented. See `bootstrap/` for reference implementation.
