"""
FDA GOLD Layer ETL Job - Temporal Versioning with Delta Lake

Applies temporal versioning (SCD Type 2 pattern) to FDA silver data using Delta Lake.
Tracks changes over time with active_from/active_to dates and status partitioning.

╔══════════════════════════════════════════════════════════════════════════════╗
║                     CRITICAL: NOVEMBER 19, 2025 INCIDENT                     ║
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  ROOT CAUSE: content_hash included volatile metadata columns (run_date,     ║
║  run_id, source_run_id), causing ALL ~80K rows to appear CHANGED on         ║
║  every run. All rows were expired + re-inserted, resetting active_from      ║
║  dates to 2025-11-19 and destroying temporal history.                        ║
║                                                                              ║
║  PREVENTION: business_columns now contains ONLY stable business             ║
║  attributes. Volatile metadata is automatically filtered by library.        ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Benefits:
- 90%+ reduction in S3 writes (MERGE only changes vs. full overwrite)
- ACID transactions
- Time travel capabilities
- Automatic schema evolution

Input: pp_dw_silver.fda_all_ndcs
Output: pp_dw_gold.fda_all_ndcs (Delta table, partitioned by status: current/historical)
"""

import sys
import boto3  # type: ignore[import-not-found]
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark import SparkConf  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset',
    'silver_database', 'gold_database', 'gold_base_path',
    'compression_codec',
    'silver_table', 'temporal_lib_path'
])

# ============================================================================
# Delta Lake Configuration (CRITICAL - Must be BEFORE SparkContext)
# ============================================================================
# Configure Spark for Delta Lake BEFORE creating SparkContext
# Setting configs here (before SparkContext creation) is the correct approach
# Setting them after would cause "Cannot modify static config" error
#
# Note: S3SingleDriverLogStore is configured via --conf in CDK
# (see GoldFdaAllNdcsStack.js), not here
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Initialize Glue with Delta-configured SparkConf
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Add temporal versioning library to Python path
sys.path.append(args['temporal_lib_path'])
from temporal_versioning_delta import apply_temporal_versioning_delta  # type: ignore[import-not-found]

# Generate run metadata
run_date = datetime.now().strftime("%Y-%m-%d")
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Get configuration from job arguments
dataset = args['dataset']
silver_database = args['silver_database']
gold_database = args['gold_database']
gold_path = args['gold_base_path']
silver_table = args['silver_table']
gold_table = dataset.replace('-', '_')  # gold-fda-all-ndc → gold_fda_all_ndc

print(f"=== Starting GOLD ETL for {dataset} ===")
print(f"Silver database: {silver_database}")
print(f"Gold database: {gold_database}")
print(f"Gold path: {gold_path}")
print(f"Silver table: {silver_table}")
print(f"Gold table: {gold_table}")
print(f"Run date: {run_date}")
print(f"Run ID: {run_id}")
print(f"Mode: Delta Lake with Temporal Versioning (SCD Type 2)")
print(f"Format: Delta (ACID, Time Travel, Auto-Optimize)")

try:
    # ===========================
    # Step 1: Read Silver Data
    # ===========================
    print("\n[1/3] Reading silver data...")

    silver_df = glueContext.create_dynamic_frame.from_catalog(
        database=silver_database,
        table_name=silver_table
    ).toDF()

    silver_count = silver_df.count()
    print(f"Silver records: {silver_count}")
    print(f"Silver columns: {silver_df.columns}")

    # ===========================
    # Step 2: Transform Schema
    # ===========================
    print("\n[2/3] Transforming schema...")

    # Rename meta_run_id to source_run_id for lineage tracking
    if "meta_run_id" in silver_df.columns:
        silver_df = silver_df.withColumnRenamed("meta_run_id", "source_run_id")

    # Add source_file metadata
    silver_df = silver_df.withColumn("source_file", F.lit(f"silver.{silver_table}"))

    print(f"Transformed columns: {silver_df.columns}")

    # ===========================
    # Step 3: Apply Temporal Versioning with Delta Lake
    # ===========================
    print("\n[3/3] Applying Delta Lake temporal versioning...")

    # ╔══════════════════════════════════════════════════════════════════════╗
    # ║  CRITICAL: business_columns MUST contain ONLY stable business       ║
    # ║  attributes from source data. NEVER include volatile metadata       ║
    # ║  like run_date, run_id, source_run_id, source_file.                 ║
    # ║                                                                      ║
    # ║  November 19 Incident Prevention: The library automatically         ║
    # ║  filters out volatile columns using VOLATILE_METADATA_COLUMNS       ║
    # ║  denylist. Providing volatile columns here will log warnings        ║
    # ║  but won't break the hash.                                          ║
    # ║                                                                      ║
    # ║  See: FDA_ALL_NDCS_BUSINESS_COLUMNS.md for complete reference       ║
    # ╚══════════════════════════════════════════════════════════════════════╝

    # Define business columns for change detection
    # These are ONLY stable business attributes from Silver fda_all_ndcs
    # Note: ndc_11 is the business key and is NOT included in content_hash
    business_columns = [
        # Product Information
        "proprietary_name",           # Brand/trade name
        "dosage_form",                # e.g., "TABLET", "CAPSULE"
        "marketing_category",         # e.g., "NDA", "ANDA", "OTC"
        "product_type",               # e.g., "HUMAN PRESCRIPTION DRUG"
        "application_number",         # FDA application number
        "dea_schedule",               # DEA controlled substance schedule
        "package_description",        # Package size and type

        # Strength Information
        "active_numerator_strength",  # e.g., "20"
        "active_ingredient_unit",     # e.g., "mg"

        # Regulatory Information
        "spl_id",                     # Structured Product Labeling ID
        "marketing_start_date",       # When product entered market
        "marketing_end_date",         # When product left market (if applicable)
        "billing_unit",               # Billing/reimbursement unit

        # Source Flags
        "nsde_flag"                   # Boolean: Present in FDA NSDE dataset
    ]

    print(f"\n[BUSINESS COLUMNS] Provided {len(business_columns)} business columns")
    print(f"[BUSINESS COLUMNS] Library will validate and filter volatile metadata automatically")

    # Apply temporal versioning - Delta MERGE handles the write internally
    # The library will:
    # 1. Validate business_columns exist in DataFrame
    # 2. Filter out any volatile metadata columns
    # 3. Log which columns are used for content_hash
    # 4. Fail fast if no business columns remain
    stats = apply_temporal_versioning_delta(
        spark=spark,
        incoming_df=silver_df,
        gold_path=gold_path,
        business_key="ndc_11",
        business_columns=business_columns,
        run_date=run_date,
        run_id=run_id
    )

    # ===========================
    # Step 4: Summary Statistics
    # ===========================
    print("\n╔══════════════════════════════════════════════════════════════════╗")
    print("║                      DELTA MERGE SUMMARY                          ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(f"NEW records:       {stats['new']:>6,} (inserted with active_to=9999-12-31)")
    print(f"CHANGED records:   {stats['changed']:>6,} (old expired, new inserted)")
    print(f"EXPIRED records:   {stats['expired']:>6,} (no longer in source)")
    print(f"UNCHANGED records: {stats['unchanged']:>6,} (skipped - no write)")
    print(f"────────────────────────────────────────────────────────────────")
    print(f"Total S3 writes:   {stats['total_written']:>6,} (90%+ reduction vs full overwrite)")
    print()

    # Calculate efficiency metric
    total_incoming = silver_count
    write_reduction = 100 * (1 - (stats['total_written'] / (total_incoming * 2))) if total_incoming > 0 else 0
    print(f"Write efficiency: {write_reduction:.1f}% fewer writes than full overwrite")

    print("\n=== GOLD ETL completed successfully ===")
    print("Note: Delta table registered via Glue Crawler (see Step Functions orchestration)")

except Exception as e:
    print(f"\n=== GOLD ETL FAILED ===")
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise e

finally:
    job.commit()
