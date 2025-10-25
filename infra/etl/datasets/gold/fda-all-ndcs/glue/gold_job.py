"""
FDA GOLD Layer ETL Job - Temporal Versioning with Delta Lake

Applies temporal versioning (SCD Type 2 pattern) to FDA silver data using Delta Lake.
Tracks changes over time with active_from/active_to dates and status partitioning.

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

# Configure Spark for Delta Lake BEFORE creating SparkContext
# Setting configs here (before SparkContext creation) is the correct approach
# Setting them after would cause "Cannot modify static config" error
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

    # Define business columns for change detection (excludes keys and metadata)
    business_columns = [
        "marketing_category",
        "product_type",
        "proprietary_name",
        "dosage_form",
        "application_number",
        "dea_schedule",
        "package_description",
        "active_numerator_strength",
        "active_ingredient_unit",
        "spl_id",
        "marketing_start_date",
        "marketing_end_date",
        "billing_unit",
        "nsde_flag"
    ]

    # Apply temporal versioning - Delta MERGE handles the write internally
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
    print("\n=== Summary Statistics ===")
    print(f"NEW records: {stats['new']}")
    print(f"CHANGED records: {stats['changed']}")
    print(f"EXPIRED records: {stats['expired']}")
    print(f"UNCHANGED records: {stats['unchanged']} (skipped)")
    print(f"Total writes: {stats['total_written']}")

    print("\n=== GOLD ETL completed successfully ===")
    print("Note: Delta table registered via CDK (no crawler needed)")

except Exception as e:
    print(f"\n=== GOLD ETL FAILED ===")
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise e

finally:
    job.commit()
