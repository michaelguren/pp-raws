# RxNORM ETL Pipeline

Complete ETL pipeline for processing RxNORM data with RRF tables, RxCUI change tracking, and NDC-to-RxCUI/TTY mapping.

## Overview

This ETL pipeline:
1. Downloads RxNORM data using UMLS authentication
2. Processes multiple RRF tables to Bronze layer
3. Tracks RxCUI changes (remapped/quantified)
4. Creates optimized NDC-to-RxCUI/TTY mapping for FDA enrichment

## Architecture

```
UMLS Auth → Download RxNORM → Extract RRF → Bronze Tables → GOLD Enrichment → RxCUI Changes
```

## Prerequisites

1. **UMLS API Key**:
   - Get your API key from [UTS Account](https://uts.nlm.nih.gov/uts/)
   - Store in AWS Secrets Manager:
   ```bash
   aws secretsmanager create-secret \
     --name "umls-api-key" \
     --description "UMLS API key for RxNORM downloads" \
     --secret-string "your-actual-umls-api-key-here"
   ```
2. **ETL Core Stack**: Deploy `pp-dw-etl-core` first
3. **Glue Databases**: `pp_dw_bronze` and `pp_dw_gold` (created by core stack)

## Deployment

```bash
# Deploy RxNORM stack
cdk deploy pp-dw-etl-rxnorm
```

## Manual Execution

### 1. Bronze Job (Download & Process RRF)

```bash
# With specific release date
aws glue start-job-run \
  --job-name pp-dw-bronze-rxnorm \
  --arguments '{"--release_date":"09022025"}'

# Auto-detect latest release date from NLM website
aws glue start-job-run \
  --job-name pp-dw-bronze-rxnorm
```

**Auto-detection**: When no `--release_date` is provided, the job will scrape https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html to find the latest available RxNorm release date.

### 2. Bronze Crawlers (Update Catalog)

Run after Bronze job completes:

```bash
aws glue start-crawler --name pp-dw-bronze-rxnorm-rxnconso-crawler
aws glue start-crawler --name pp-dw-bronze-rxnorm-rxnsat-crawler
aws glue start-crawler --name pp-dw-bronze-rxnorm-rxnrel-crawler
aws glue start-crawler --name pp-dw-bronze-rxnorm-rxnsty-crawler
aws glue start-crawler --name pp-dw-bronze-rxnorm-rxncui-crawler
aws glue start-crawler --name pp-dw-bronze-rxnorm-rxnatomarchive-crawler
```

### 3. GOLD Jobs (After Crawlers Complete)

Run in parallel:

```bash
# Track RxCUI changes
aws glue start-job-run --job-name pp-dw-gold-rxcui-changes

# Create NDC mapping
aws glue start-job-run --job-name pp-dw-gold-rxnorm-ndc-mapping
```

### 4. GOLD Crawlers

```bash
aws glue start-crawler --name pp-dw-gold-rxcui-changes-crawler
aws glue start-crawler --name pp-dw-gold-rxnorm-ndc-mapping-crawler
```

## Data Output

### Bronze Tables

- `bronze_rxnorm_rxnconso`: Main drug concepts
- `bronze_rxnorm_rxnsat`: Attributes (includes NDC mappings)
- `bronze_rxnorm_rxnrel`: Relationships between concepts
- `bronze_rxnorm_rxnsty`: Semantic types
- `bronze_rxnorm_rxncui`: CUI mappings
- `bronze_rxnorm_rxnatomarchive`: Historical atom archive

### GOLD Tables

- `gold_rxcui_changes`: RxCUI remapping/quantification tracking
- `gold_rxnorm_ndc_mapping`: Optimized NDC-to-RxCUI/TTY lookup

## Using the NDC Mapping

Query the mapping table:

```sql
SELECT
  ndc_11,
  rxcui,
  tty,
  mapping_status
FROM pp_dw_gold.gold_rxnorm_ndc_mapping
WHERE ndc_11 = '00071015523'
```

Integration with FDA data:

```sql
SELECT
  f.fda_ndc_11,
  f.fda_proprietary_name,
  r.rxcui,
  r.tty,
  r.mapping_status
FROM pp_dw_gold.gold_fda_all_ndc f
LEFT JOIN pp_dw_gold.gold_rxnorm_ndc_mapping r
  ON f.fda_ndc_11 = r.ndc_11
```

## RxNORM Release Schedule

- **Frequency**: Monthly (first Monday)
- **Format**: `RxNorm_full_MMDDYYYY.zip`
- **Example**: `RxNorm_full_09022025.zip` for September 2, 2025
- **Next Release**: Check [RxNorm Release Notes](https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html)

## Troubleshooting

### Authentication Issues
- Verify UMLS API key in Secrets Manager
- Check API key permissions on UTS website

### Download Failures
- Check if release date exists (first Monday format)
- Verify network connectivity and timeouts

### Bronze Job Failures
- Check Glue logs for RRF parsing errors
- Verify S3 bucket permissions
- Monitor memory usage (xlarge workers)

### Missing Tables
- Ensure all RRF files are in the zip
- Check table name mappings in bronze job
- Verify crawler configurations

## Cost Considerations

- **Bronze Job**: ~$5-10 per run (xlarge workers for 30-60 min)
- **GOLD Jobs**: ~$2-5 per run (medium workers for 10-20 min)
- **Storage**: ~$50-100/month for full RxNORM data
- **Data Transfer**: ~$1-5 per download (depending on size)

Total monthly cost: ~$60-120 for full pipeline

## Legacy Integration

This replaces the Rails-based RxNORM ETL with:
- ✅ **Serverless**: No database/server maintenance
- ✅ **Scalable**: Handles GB-sized files efficiently
- ✅ **Enhanced**: Includes TTY (not in legacy)
- ✅ **Traceable**: Complete audit trail and lineage
- ✅ **Cost-effective**: Pay only when running

## Next Steps

1. Test with recent RxNORM release
2. Integrate with FDA All NDC GOLD job
3. Set up monitoring and alerting
4. Consider automation via EventBridge
5. Optimize for specific use cases