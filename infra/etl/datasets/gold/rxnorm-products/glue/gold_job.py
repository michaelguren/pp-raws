"""
RxNORM Products GOLD Layer ETL Job - Temporal Versioning

Applies temporal versioning (SCD Type 2 pattern) to RxNORM silver data.
Tracks changes over time with active_from/active_to dates and status partitioning.

Input: pp_dw_silver.rxnorm_products
Output: pp_dw_gold.rxnorm_products (partitioned by status: current/historical)
"""

import sys
import boto3  # type: ignore[import-not-found]
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset',
    'silver_database', 'gold_database', 'gold_base_path',
    'compression_codec', 'crawler_name',
    'silver_table', 'temporal_lib_path'
])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])
spark.conf.set("spark.sql.parquet.summary.metadata.level", "ALL")

# Add temporal versioning library to Python path
sys.path.append(args['temporal_lib_path'])
from temporal_versioning import apply_temporal_versioning  # type: ignore[import-not-found]

# Generate run metadata
run_date = datetime.now().strftime("%Y-%m-%d")
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Get configuration from job arguments
dataset = args['dataset']
silver_database = args['silver_database']
gold_database = args['gold_database']
gold_path = args['gold_base_path']
silver_table = args['silver_table']
gold_table = dataset.replace('-', '_')  # gold-rxnorm-products → gold_rxnorm_products

print(f"=== Starting GOLD ETL for {dataset} ===")
print(f"Silver database: {silver_database}")
print(f"Gold database: {gold_database}")
print(f"Gold path: {gold_path}")
print(f"Silver table: {silver_table}")
print(f"Gold table: {gold_table}")
print(f"Run date: {run_date}")
print(f"Run ID: {run_id}")
print(f"Mode: Temporal versioning (SCD Type 2)")

try:
    # ===========================
    # Step 1: Read Silver Data
    # ===========================
    print("\n[1/4] Reading silver data...")

    silver_df = glueContext.create_dynamic_frame.from_catalog(
        database=silver_database,
        table_name=silver_table
    ).toDF()

    silver_count = silver_df.count()
    print(f"Silver records: {silver_count}")
    print(f"Silver columns: {silver_df.columns}")

    # ===========================
    # Step 2: Transform Schema (Drop rxnorm_ prefix)
    # ===========================
    print("\n[2/4] Transforming schema (dropping rxnorm_ prefix)...")

    # Drop rxnorm_ prefix from all column names (table name indicates source)
    for col_name in silver_df.columns:
        if col_name.startswith("rxnorm_"):
            new_name = col_name[7:]  # Remove "rxnorm_" prefix
            silver_df = silver_df.withColumnRenamed(col_name, new_name)

    # Add source_file metadata
    silver_df = silver_df.withColumn("source_file", F.lit(f"silver.{silver_table}"))

    print(f"Transformed columns: {silver_df.columns}")

    # ===========================
    # Step 3: Apply Temporal Versioning
    # ===========================
    print("\n[3/4] Applying temporal versioning...")

    # Define business columns for change detection (excludes keys and metadata)
    business_columns = [
        "tty",
        "str",
        "strength",
        "ingredient_names",
        "brand_names",
        "dosage_forms",
        "psn",
        "sbdf_rxcui",
        "sbdf_name",
        "scdf_rxcui",
        "scdf_name",
        "sbd_rxcui",
        "bpck_rxcui",
        "multi_ingredient"
    ]

    # Apply temporal versioning
    gold_df = apply_temporal_versioning(
        spark=spark,
        incoming_df=silver_df,
        existing_table=f"{gold_database}.{gold_table}",
        business_key="rxcui",
        business_columns=business_columns,
        run_date=run_date,
        run_id=run_id
    )

    gold_count = gold_df.count()
    print(f"\nGold records to write: {gold_count}")

    if gold_count == 0:
        print("No changes detected - skipping write")
        print("GOLD ETL completed successfully (no changes)")
        job.commit()
        sys.exit(0)

    # ===========================
    # Step 4: Write to GOLD Layer
    # ===========================
    print("\n[4/4] Writing gold data...")
    print(f"Target path: {gold_path}")
    print("Partitioning: status (current/historical)")
    print("Format: Parquet with ZSTD compression")

    # Write partitioned by status
    gold_df.write \
        .mode("overwrite") \
        .partitionBy("status") \
        .parquet(gold_path)

    print("Gold data written successfully")

    # ===========================
    # Step 5: Summary Statistics
    # ===========================
    print("\n=== Summary Statistics ===")

    status_counts = gold_df.groupBy("status").count().collect()
    for row in status_counts:
        print(f"Status '{row['status']}': {row['count']} records")

    print(f"\nCrawler: {args['crawler_name']}")
    print("Command: aws glue start-crawler --name " + args['crawler_name'])

    print("\n=== GOLD ETL completed successfully ===")

except Exception as e:
    print(f"\n=== GOLD ETL FAILED ===")
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise e

finally:
    job.commit()
