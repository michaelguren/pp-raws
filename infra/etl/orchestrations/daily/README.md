# Daily Orchestration (Placeholder)

## Purpose
Daily incremental ETL orchestration for FDA datasets that update frequently.

## Datasets
- `fda-nsde` - FDA Comprehensive NDC SPL Data Elements
- `fda-cder` - FDA CDER National Drug Code Directory
- `fda-all-ndc` - Silver layer join of FDA datasets
- `fda-all-ndcs` - Gold layer with temporal versioning

## Orchestration Flow (Future)
1. **Bronze Layer** - Download and process FDA data
2. **Check Tables** - Verify bronze tables exist, run crawlers if needed
3. **Silver Layer** - Join FDA datasets
4. **Check Tables** - Verify silver tables exist, run crawlers if needed
5. **Gold Layer** - Apply temporal versioning (incremental changes only)
6. **DynamoDB Sync** - Sync incremental changes to DynamoDB

## Trigger
- EventBridge scheduled rule: Daily at 6 AM UTC
- Manual execution via Step Functions console

## Status
Not yet implemented. See `bootstrap/` for reference implementation.
