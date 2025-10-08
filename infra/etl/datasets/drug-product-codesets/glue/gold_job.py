"""
Drug Product Codesets Gold Layer Job

Combines FDA All NDC (silver) + RxNORM Products (silver) via RXNSAT bronze table.
All fields are prefixed consistently: fda_* for FDA fields, rxnorm_* for RxNORM fields.

Input:
- Silver: fda_all_ndc (FDA NDC data with NSDE + CDER)
- Silver: rxnorm_products (RxNORM prescribable products)
- Bronze: rxnorm_rxnsat (RXNSAT table for NDC-to-RxCUI mappings)

Output:
- Gold: drug_product_codesets (one row per fda_ndc_11 with FDA + RxNORM fields)

Join Strategy:
  fda_all_ndc (fda_ndc_11)
    FULL OUTER JOIN rxnsat (atv WHERE sab='RXNORM' AND atn='NDC')
      ON fda_ndc_11 = rxnsat.atv
    FULL OUTER JOIN rxnorm_products (rxcui)
      ON rxnsat.rxcui = rxnorm_products.rxcui

Result: All FDA NDCs + All RxNORM Products (matched where possible, NULL otherwise)
"""

import sys
from awsglue.transforms import *  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]
from datetime import datetime

# Parse job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'dataset',
    'bucket_name',
    'bronze_database',
    'silver_database',
    'gold_database',
    'gold_base_path',
    'fda_all_ndc_table',
    'rxnorm_products_table',
    'rxnsat_table',
    'output_table',
    'compression_codec',
    'crawler_name'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting Drug Product Codesets gold job for dataset: {args['dataset']}")
print(f"Silver database: {args['silver_database']}")
print(f"Bronze database: {args['bronze_database']}")
print(f"Gold database: {args['gold_database']}")
print(f"Output path: {args['gold_base_path']}")

# Timestamps for metadata
run_timestamp = datetime.now()

# ============================================================================
# STEP 1: Load Source Tables
# ============================================================================

print("Loading source tables from Glue catalog...")

# Load FDA All NDC (Silver)
fda_df = glueContext.create_dynamic_frame.from_catalog(
    database=args['silver_database'],
    table_name=args['fda_all_ndc_table']
).toDF()

print(f"Loaded {fda_df.count()} FDA All NDC records")
print(f"FDA columns: {fda_df.columns}")

# Load RxNORM Products (Silver)
rxnorm_products_df = glueContext.create_dynamic_frame.from_catalog(
    database=args['silver_database'],
    table_name=args['rxnorm_products_table']
).toDF()

print(f"Loaded {rxnorm_products_df.count()} RxNORM Products records")
print(f"RxNORM Products columns: {rxnorm_products_df.columns}")

# Load RXNSAT (Bronze) - Filter to NDC attributes only
rxnsat_df = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name=args['rxnsat_table']
).toDF()

print(f"Loaded {rxnsat_df.count()} RXNSAT records (unfiltered)")

# Filter RXNSAT to NDC mappings only (SAB='RXNORM', ATN='NDC')
rxnsat_ndc = rxnsat_df.filter(
    (F.col('sab') == 'RXNORM') & (F.col('atn') == 'NDC')
).select(
    F.col('rxcui').alias('ndc_rxcui'),
    F.col('atv').alias('ndc_value')
).distinct()

print(f"Filtered to {rxnsat_ndc.count()} NDC mappings in RXNSAT")

# ============================================================================
# STEP 2: Verify Field Prefixes (Already Correct in Silver Layers)
# ============================================================================

print("Verifying FDA and RxNORM field prefixes from silver layers...")
print(f"FDA columns: {fda_df.columns}")
print(f"RxNORM columns: {rxnorm_products_df.columns}")

# Silver layers now have correct prefixes - no renaming needed!

# ============================================================================
# STEP 3: Join FDA + RXNSAT + RxNORM
# ============================================================================

print("Performing FULL OUTER JOIN: FDA <-> RXNSAT <-> RxNORM...")

# Step 3a: FULL OUTER JOIN FDA with RXNSAT (via NDC)
fda_with_rxcui = fda_df.join(
    rxnsat_ndc,
    fda_df['fda_ndc_11'] == rxnsat_ndc['ndc_value'],
    'full_outer'
).select(
    fda_df['*'],
    rxnsat_ndc['ndc_rxcui']
)

print(f"After FDA + RXNSAT join: {fda_with_rxcui.count()} records")
print(f"Records with RxCUI match: {fda_with_rxcui.filter(F.col('ndc_rxcui').isNotNull()).count()}")

# Step 3b: FULL OUTER JOIN with RxNORM Products (via RxCUI)
gold_df = fda_with_rxcui.join(
    rxnorm_products_df,
    fda_with_rxcui['ndc_rxcui'] == rxnorm_products_df['rxnorm_rxcui'],
    'full_outer'
).select(
    # FDA fields (all with fda_ prefix from silver layer)
    fda_with_rxcui['fda_ndc_11'],
    fda_with_rxcui['fda_ndc_5'],
    fda_with_rxcui['fda_marketing_category'],
    fda_with_rxcui['fda_product_type'],
    fda_with_rxcui['fda_proprietary_name'],
    fda_with_rxcui['fda_dosage_form'],
    fda_with_rxcui['fda_application_number'],
    fda_with_rxcui['fda_dea_schedule'],
    fda_with_rxcui['fda_package_description'],
    fda_with_rxcui['fda_active_numerator_strength'],
    fda_with_rxcui['fda_active_ingredient_unit'],
    fda_with_rxcui['fda_spl_id'],
    fda_with_rxcui['fda_marketing_start_date'],
    fda_with_rxcui['fda_marketing_end_date'],
    fda_with_rxcui['fda_billing_unit'],
    fda_with_rxcui['fda_nsde_flag'],

    # RxNORM fields (all with rxnorm_ prefix from silver layer)
    rxnorm_products_df['rxnorm_rxcui'],
    rxnorm_products_df['rxnorm_tty'],
    rxnorm_products_df['rxnorm_str'],
    rxnorm_products_df['rxnorm_strength'],
    rxnorm_products_df['rxnorm_ingredient_names'],
    rxnorm_products_df['rxnorm_brand_names'],
    rxnorm_products_df['rxnorm_dosage_forms'],
    rxnorm_products_df['rxnorm_psn'],
    rxnorm_products_df['rxnorm_sbdf_rxcui'],
    rxnorm_products_df['rxnorm_sbdf_name'],
    rxnorm_products_df['rxnorm_scdf_rxcui'],
    rxnorm_products_df['rxnorm_scdf_name'],
    rxnorm_products_df['rxnorm_sbd_rxcui'],
    rxnorm_products_df['rxnorm_bpck_rxcui'],
    rxnorm_products_df['rxnorm_multi_ingredient'],

    # Metadata
    fda_with_rxcui['meta_run_id']
)

print(f"Final GOLD records: {gold_df.count()}")
print(f"Records with FDA data: {gold_df.filter(F.col('fda_ndc_11').isNotNull()).count()}")
print(f"Records with RxNORM data: {gold_df.filter(F.col('rxnorm_rxcui').isNotNull()).count()}")
print(f"Records with BOTH FDA + RxNORM: {gold_df.filter(F.col('fda_ndc_11').isNotNull() & F.col('rxnorm_rxcui').isNotNull()).count()}")
print(f"Final GOLD columns: {gold_df.columns}")

# ============================================================================
# STEP 4: Write to GOLD Layer (Kill-and-Fill)
# ============================================================================

print(f"Writing GOLD data to: {args['gold_base_path']}")

gold_df.write \
    .mode('overwrite') \
    .format('parquet') \
    .option('compression', args['compression_codec'].lower()) \
    .save(args['gold_base_path'])

print("GOLD table written successfully")

# ============================================================================
# STEP 5: Crawler Notification (Manual Trigger)
# ============================================================================

print(f"Note: Run crawler manually when needed: {args['crawler_name']}")
print("Command: aws glue start-crawler --name " + args['crawler_name'])

# ============================================================================
# Complete Job
# ============================================================================

job.commit()
print("Drug Product Codesets gold job completed successfully")
