"""
RxNORM Product Classifications GOLD Layer ETL Job - Temporal Versioning

Applies temporal versioning (SCD Type 2 pattern) to RxClass drug classification data.
Tracks changes over time with active_from/active_to dates and status partitioning.
Supports bidirectional queries: drug→classes and class→drugs.

Input: pp_dw_bronze.rxclass_drug_members
Output: pp_dw_gold.rxnorm_product_classifications (partitioned by status: current/historical)
"""

import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset',
    'bronze_database', 'gold_database', 'gold_base_path',
    'compression_codec', 'crawler_name',
    'bronze_table', 'temporal_lib_path'
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
bronze_database = args['bronze_database']
gold_database = args['gold_database']
gold_path = args['gold_base_path']
bronze_table = args['bronze_table']
gold_table = dataset.replace('-', '_')  # rxnorm-product-classifications → rxnorm_product_classifications

print(f"=== Starting GOLD ETL for {dataset} ===")
print(f"Bronze database: {bronze_database}")
print(f"Gold database: {gold_database}")
print(f"Gold path: {gold_path}")
print(f"Bronze table: {bronze_table}")
print(f"Gold table: {gold_table}")
print(f"Run date: {run_date}")
print(f"Run ID: {run_id}")
print(f"Mode: Temporal versioning (SCD Type 2) with composite key [rxcui, class_id, class_type]")

try:
    # ===========================
    # Step 1: Read Bronze Data
    # ===========================
    print("\n[1/4] Reading bronze data...")

    bronze_df = glueContext.create_dynamic_frame.from_catalog(
        database=bronze_database,
        table_name=bronze_table
    ).toDF()

    bronze_count = bronze_df.count()
    print(f"Bronze records: {bronze_count:,}")
    print(f"Bronze columns: {bronze_df.columns}")

    # ===========================
    # Step 2: Transform Schema
    # ===========================
    print("\n[2/4] Transforming schema (cleaning column names)...")

    # Rename columns for clarity (drop verbose prefixes)
    bronze_df = bronze_df \
        .withColumnRenamed("product_rxcui", "rxcui") \
        .withColumnRenamed("product_tty", "tty") \
        .withColumnRenamed("rxclassminconceptitem_classid", "class_id") \
        .withColumnRenamed("rxclassminconceptitem_classname", "class_name") \
        .withColumnRenamed("rxclassminconceptitem_classtype", "class_type")

    # Rename meta columns for gold layer consistency
    if "meta_run_id" in bronze_df.columns:
        bronze_df = bronze_df.withColumnRenamed("meta_run_id", "source_run_id")

    # Add source_file metadata
    bronze_df = bronze_df.withColumn("source_file", F.lit(f"bronze.{bronze_table}"))

    print(f"Transformed columns: {bronze_df.columns}")

    # Sample data check
    print("\nSample records:")
    bronze_df.select("rxcui", "class_id", "class_name", "class_type", "tty").show(5, truncate=False)

    # ===========================
    # Step 3: Apply Temporal Versioning
    # ===========================
    print("\n[3/4] Applying temporal versioning with composite key...")

    # Composite business key: [rxcui, class_id, class_type]
    # This tracks when a specific drug-class-type relationship changes
    business_key = ["rxcui", "class_id", "class_type"]

    # Business columns for change detection (from config.json)
    business_columns = [
        "tty",
        "minconcept_rxcui",
        "minconcept_name",
        "minconcept_tty",
        "class_name",
        "rela",
        "relasource"
    ]

    # Apply temporal versioning
    gold_df = apply_temporal_versioning(
        spark=spark,
        incoming_df=bronze_df,
        existing_table=f"{gold_database}.{gold_table}",
        business_key=business_key,  # Composite key
        business_columns=business_columns,
        run_date=run_date,
        run_id=run_id
    )

    gold_count = gold_df.count()
    print(f"\nGold records to write: {gold_count:,}")

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
        print(f"Status '{row['status']}': {row['count']:,} records")

    # Sample current records by class type
    print("\nSample classifications by type:")
    class_type_counts = gold_df.filter(F.col("status") == "current") \
        .groupBy("class_type") \
        .count() \
        .orderBy(F.desc("count"))

    class_type_counts.show(20, truncate=False)

    print("\nSample current records:")
    gold_df.filter(F.col("status") == "current").select(
        "rxcui", "class_id", "class_name", "class_type", "active_from", "active_to"
    ).show(10, truncate=False)

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
