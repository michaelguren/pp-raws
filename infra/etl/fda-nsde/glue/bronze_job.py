"""
Complete NSDE ETL Job - Download, Transform, and Store
Downloads FDA data, saves raw files for lineage, transforms to Bronze layer
"""
import sys
import boto3  # type: ignore[import-not-found]
import posixpath
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit, col, when, to_date  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset',
    'source_url', 'bronze_database', 'raw_path', 'bronze_path',
    'date_format', 'compression_codec', 'file_table_mapping'
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

# Generate human-readable runtime run_id
from datetime import datetime
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: 20240311_143022

# Get configuration from job arguments
import json
dataset = args['dataset']
source_url = args['source_url']
bronze_database = args['bronze_database']
date_format = args['date_format']
file_table_mapping = json.loads(args['file_table_mapping'])

# S3 paths are already complete URLs from stack
import posixpath
raw_base_path = args['raw_path']
bronze_s3_path = args['bronze_path']

# Append run_id to raw path
scheme, path = raw_base_path.rstrip('/').split('://', 1)
raw_s3_path = f"{scheme}://{posixpath.join(path, f'run_id={run_id}')}/"

# Extract bucket and key components from raw S3 path
raw_path_parts = raw_s3_path.replace('s3://', '').split('/', 1)
raw_bucket = raw_path_parts[0]
raw_prefix = raw_path_parts[1] if len(raw_path_parts) > 1 else ''

# Load shared ETL utilities
sys.path.append('/tmp')
s3_client = boto3.client('s3')
s3_client.download_file(raw_bucket, 'etl/util-runtime/etl_utils.py', '/tmp/etl_utils.py')
from etl_utils import download_and_extract

print(f"Starting Complete ETL for {dataset} (download + transform)")
print(f"Source URL: {source_url}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze path: {bronze_s3_path}")
print(f"Run ID: {run_id}")
print(f"Date format: {date_format}")
print(f"Bronze database: {bronze_database}")
print(f"Mode: overwrite (kill-and-fill)")

def apply_column_mapping(df, column_mapping):
    """Apply column name mapping to DataFrame"""
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    return df

def safe_key(prefix, name):
    """Prevent zip-slip: strip leading slashes, collapse .., keep posix separators"""
    name = name.lstrip("/").replace("\\", "/")
    parts = [p for p in name.split("/") if p not in ("", ".", "..")]
    return posixpath.join(prefix, *parts)

# Column mappings from Ruby code
NSDE_COLUMN_MAPPING = {
    "Item Code": "item_code",
    "NDC11": "ndc_11",
    "Proprietary Name": "proprietary_name",
    "Dosage Form": "dosage_form",
    "Marketing Category": "marketing_category",
    "Application Number or Citation": "application_number_or_citation",
    "Product Type": "product_type",
    "Marketing Start Date": "marketing_start_date",
    "Marketing End Date": "marketing_end_date",
    "Billing Unit": "billing_unit",
    "Inactivation Date": "inactivation_date",
    "Reactivation Date": "reactivation_date"
}

try:
    # Step 1: Download and extract using shared utilities
    print(f"Downloading from: {source_url}")

    result = download_and_extract(source_url, raw_s3_path, file_table_mapping)
    csv_count = result["files_extracted"]
    content_length = result["content_length"]
    
    # Step 2: Read specific CSV file from raw path for transformation
    csv_filename = list(file_table_mapping.keys())[0]  # "Comprehensive_NDC_SPL_Data_Elements_File.csv"
    csv_file_path = posixpath.join(raw_s3_path, csv_filename)
    print(f"Reading CSV file: {csv_file_path}")

    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(csv_file_path)

    # Quick emptiness check before full count
    if not df.head(1):
        raise ValueError(f"No data found in file: {csv_file_path}")

    row_count = df.count()
    print(f"Loaded {row_count} records")
    
    # Apply column mapping and transformations
    df = apply_column_mapping(df, NSDE_COLUMN_MAPPING)
    
    print(f"Mapped columns: {df.columns}")
    
    # Fix date columns - convert date strings to proper dates using configurable format
    date_columns = [c for c in df.columns if 'date' in c]  # Avoid shadowing col function
    print(f"Using date format: {date_format}")
    for date_col in date_columns:
        df = df.withColumn(
            date_col,
            when(col(date_col).isNull() | (col(date_col) == ""), lit(None))
            .otherwise(to_date(col(date_col).cast("string"), date_format))
        )
    
    print(f"Fixed date columns: {date_columns}")
    
    # Add basic metadata (run_id serves as both identifier and timestamp)
    df_bronze = df.withColumn("meta_run_id", lit(run_id))
    
    # Write to bronze layer (overwrite for kill-and-fill approach)
    print(f"Writing to: {bronze_s3_path}")
    
    df_bronze.write \
             .mode("overwrite") \
             .option("compression", args['compression_codec']) \
             .parquet(bronze_s3_path)
    
    print(f"Successfully processed {row_count} records to bronze layer")
    
    print(f"Complete ETL finished successfully:")
    print(f"  - Downloaded: {content_length or 'unknown'} bytes")
    print(f"  - Raw files: {csv_count} files saved")
    print(f"  - Bronze records: {row_count} processed")
    print("Note: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Bronze job completed")