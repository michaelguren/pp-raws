"""
Shared Bronze Layer ETL Job - HTTP/ZIP Downloads
Downloads ZIP files from HTTP/HTTPS URLs, extracts contents, saves raw files for lineage, transforms to Bronze layer
Supports both single-table and multi-table datasets
For API-based data sources, use bronze_api_job.py instead
"""
import sys
import json
import posixpath
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit  # type: ignore[import-not-found]
from etl_runtime_utils import download_and_extract, apply_schema  # type: ignore[import-not-found]

# ============================================================================
# INITIALIZATION
# ============================================================================

# Parse job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset', 'bronze_database',
    'raw_path', 'bronze_path',
    'file_table_mapping', 'column_schema', 'source_url',
    'compression_codec', 'delimiter'
])

# Initialize Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])
spark.conf.set("spark.sql.parquet.summary.metadata.level", "ALL")

# ============================================================================
# CONFIGURATION
# ============================================================================

# Dataset metadata
dataset = args['dataset']  # e.g., "fda-nsde"
bronze_database = args['bronze_database']  # e.g., "pp_dw_bronze"
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g., "20250104_153045"

# Source configuration
source_url = args['source_url']  # e.g., "https://www.fda.gov/files/nsde.zip"
file_table_mapping = json.loads(args['file_table_mapping'])  # e.g., {"package.txt": "fda-packages", "product.txt": "fda-products"}
column_schema = json.loads(args['column_schema']) if 'column_schema' in args else {}  # Optional schema transformations
delimiter = args.get('delimiter', ',')  # e.g., "\t" or ","

# S3 paths (dataset-specific, raw includes run_id partition for lineage tracking)
raw_path_with_run = posixpath.join(args['raw_path'], f'run_id={run_id}') + '/'  # e.g., "s3://pp-dw-123456789/raw/fda-nsde/run_id=20250104_153045/"
bronze_dataset_path = args['bronze_path']  # e.g., "s3://pp-dw-123456789/bronze/fda-nsde/"

# ============================================================================
# JOB EXECUTION
# ============================================================================

print(f"Starting Bronze ETL for {dataset}")
print(f"  Run ID: {run_id}")
print(f"  Database: {bronze_database}")
print(f"  Tables: {list(file_table_mapping.values())}")
print(f"  Source: {source_url}")
print(f"  Raw path: {raw_path_with_run}")
print(f"  Bronze path: {bronze_dataset_path}")

try:
    # Step 1: Download and extract ZIP from HTTP source
    print(f"\n[1/2] Downloading from {source_url}...")
    result = download_and_extract(source_url, raw_path_with_run, file_table_mapping)
    print(f"  ✓ Downloaded {result.get('content_length', 'unknown')} bytes")
    print(f"  ✓ Extracted {result['files_extracted']} files to raw layer")

    # Step 2: Transform and load to bronze
    print(f"\n[2/2] Transforming to bronze layer...")
    total_records = 0
    tables_processed = []
    is_multi_table = len(file_table_mapping) > 1

    for source_file, table_name in file_table_mapping.items():
        print(f"\n  Processing: {table_name}")

        # Read CSV from raw layer
        source_path = posixpath.join(raw_path_with_run, source_file)
        df = spark.read.option("header", "true") \
                      .option("inferSchema", "false") \
                      .option("delimiter", delimiter) \
                      .csv(source_path)

        if not df.head(1):
            raise ValueError(f"No data found in {source_file}")

        row_count = df.count()
        print(f"    Loaded: {row_count:,} records")

        # Apply schema transformations
        df = apply_schema(df, column_schema.get(table_name))
        print(f"    Schema: {'transformed' if table_name in column_schema else 'inferred'}")

        # Add metadata and write to bronze
        df = df.withColumn("meta_run_id", lit(run_id))
        output_path = posixpath.join(bronze_dataset_path, table_name) + '/' if is_multi_table else bronze_dataset_path
        df.write.mode("overwrite").option("compression", args['compression_codec']).parquet(output_path)

        print(f"    Written: {output_path}")
        total_records += row_count
        tables_processed.append(table_name)

    # Summary
    print(f"\n✓ Bronze ETL completed successfully")
    print(f"  Total records: {total_records:,}")
    print(f"  Tables: {', '.join(tables_processed)}")
    print(f"\nNote: Run crawlers to update Glue catalog")

except Exception as e:
    print(f"\n✗ Bronze ETL failed: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()
    print("Job committed")