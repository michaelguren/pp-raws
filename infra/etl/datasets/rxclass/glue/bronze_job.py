"""
Complete RxClass ETL Job - API Collection, Transform, and Store
Collects RxClass data from NLM RxNav API, saves raw JSON for lineage, transforms to Bronze layer
"""
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import col  # type: ignore[import-not-found]
from pyspark.sql.types import StructType, StructField, StringType  # type: ignore[import-not-found]
from etl_runtime_utils import build_raw_path_with_run_id, fetch_json_with_retry, save_json_to_s3  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset', 'all_classes_url', 'bronze_database',
    'raw_path', 'bronze_path', 'compression_codec'
])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])

# Generate human-readable runtime run_id
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: 20240311_143022

# Get configuration from job arguments
dataset = args['dataset']
all_classes_url = args['all_classes_url']
bronze_database = args['bronze_database']

# Build S3 paths (raw includes run_id partition for lineage tracking)
raw_s3_path = build_raw_path_with_run_id(args['raw_path'], run_id)
bronze_s3_path = args['bronze_path']

print(f"Starting RxClass ETL job")
print(f"Dataset: {dataset}")
print(f"Run ID: {run_id}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze path: {bronze_s3_path}")

try:
    # Step 1: Fetch all classes in single API call
    print("Step 1: Fetching all classes...")
    all_classes_data = fetch_json_with_retry(all_classes_url)

    if not all_classes_data:
        raise Exception(f"Failed to fetch data from {all_classes_url}")

    # Save raw response to S3 for lineage
    save_json_to_s3(all_classes_data, f"{raw_s3_path}all_classes_response.json")

    # Extract classes
    concept_list = all_classes_data.get('rxclassMinConceptList', {})
    all_rxclasses = concept_list.get('rxclassMinConcept', [])

    print(f"Total classes collected: {len(all_rxclasses)}")

    # Step 2: Transform to Bronze layer
    print("Step 2: Transforming to Bronze layer...")

    if not all_rxclasses:
        raise Exception("No RxClass data collected - cannot proceed to Bronze layer")

    # Define schema for consistent typing
    rxclass_schema = StructType([
        StructField("class_id", StringType(), True),
        StructField("class_name", StringType(), True),
        StructField("class_type", StringType(), True),
        StructField("meta_run_id", StringType(), True)
    ])

    # Convert to Spark DataFrame with explicit schema
    rxclass_records = []
    for rxclass in all_rxclasses:
        record = (
            rxclass.get('classId', ''),
            rxclass.get('className', ''),
            rxclass.get('classType', ''),
            run_id
        )
        rxclass_records.append(record)

    # Create DataFrame
    df = spark.createDataFrame(rxclass_records, schema=rxclass_schema)

    print(f"Created DataFrame with {df.count()} records")
    df.show(10, truncate=False)

    # Data quality checks
    null_class_ids = df.filter(col("class_id").isNull() | (col("class_id") == "")).count()
    null_class_names = df.filter(col("class_name").isNull() | (col("class_name") == "")).count()

    print(f"Data quality check - Null class_ids: {null_class_ids}, Null class_names: {null_class_names}")

    if null_class_ids > len(all_rxclasses) * 0.01:  # More than 1% null IDs
        raise Exception(f"Too many null class_ids: {null_class_ids}")

    # Write to Bronze layer (kill-and-fill approach)
    print(f"Writing to Bronze layer: {bronze_s3_path}")

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", args['compression_codec']) \
        .parquet(bronze_s3_path)

    print("Successfully wrote Bronze layer data")

    print("RxClass ETL job completed successfully!")
    print(f"Total records processed: {len(all_rxclasses)}")
    print(f"Bronze data location: {bronze_s3_path}")
    print("Note: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"ERROR: RxClass ETL job failed: {str(e)}")
    raise e

finally:
    job.commit()