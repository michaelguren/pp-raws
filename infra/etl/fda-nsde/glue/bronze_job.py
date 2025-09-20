"""
Complete NSDE ETL Job - Download, Transform, and Store
Downloads FDA data, saves raw files for lineage, transforms to Bronze layer
"""
import sys
import boto3  # type: ignore[import-not-found]
import urllib.request
import zipfile
from io import BytesIO
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit, col, when, to_date  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'bucket_name', 'dataset',
    'source_url', 'bronze_database', 'raw_path', 'bronze_path',
    'date_format', 'compression_codec'
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
bucket_name = args['bucket_name']
dataset = args['dataset']
source_url = args['source_url']
bronze_database = args['bronze_database']
date_format = args['date_format']

# Build full S3 paths from path fragments
raw_path_fragment = args['raw_path']
bronze_path_fragment = args['bronze_path']

raw_s3_path = f"s3://{bucket_name}/{raw_path_fragment}run_id={run_id}/"
bronze_s3_path = f"s3://{bucket_name}/{bronze_path_fragment}"

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
    # Step 1: Memory-efficient download and extraction using temporary file
    print(f"Downloading from: {source_url}")

    s3_client = boto3.client('s3')
    zip_key = f"{raw_path_fragment}run_id={run_id}/source.zip"

    import tempfile
    import shutil
    from urllib.error import URLError, HTTPError
    import time

    max_retries = 3
    content_length = None

    for attempt in range(max_retries):
        try:
            print(f"Download attempt {attempt + 1}/{max_retries}")

            with tempfile.NamedTemporaryFile(suffix='.zip', delete=True) as tmp_file:
                # Stream download to temporary file
                with urllib.request.urlopen(source_url, timeout=300) as response:
                    # Get content length if available
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        print(f"File size: {int(content_length):,} bytes")

                    # Stream to temp file in chunks (memory efficient)
                    shutil.copyfileobj(response, tmp_file)
                    tmp_file.flush()

                print(f"Downloaded to temporary file")

                # Upload original zip to S3 for lineage
                tmp_file.seek(0)
                s3_client.upload_fileobj(tmp_file, bucket_name, zip_key)
                print(f"Uploaded original zip to: s3://{bucket_name}/{zip_key}")

                # Extract CSV files and upload to S3
                tmp_file.seek(0)
                csv_count = 0

                with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
                    for file_info in zip_ref.infolist():
                        if not file_info.is_dir() and file_info.filename.lower().endswith('.csv'):
                            # Stream each CSV file from zip to S3
                            with zip_ref.open(file_info) as csv_file:
                                csv_key = f"{raw_path_fragment}run_id={run_id}/{file_info.filename}"

                                s3_client.upload_fileobj(
                                    csv_file,
                                    bucket_name,
                                    csv_key
                                )
                                print(f"Extracted CSV: {file_info.filename}")
                                csv_count += 1

                print(f"Successfully extracted and uploaded {csv_count} CSV files")
                break  # Success, exit retry loop

        except (URLError, HTTPError) as e:
            print(f"Network error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise
    
    # Step 2: Read CSV files from raw path for transformation
    print(f"Reading CSV from: {raw_s3_path}*.csv")

    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(f"{raw_s3_path}*.csv")
    
    row_count = df.count()
    print(f"Loaded {row_count} records")
    
    if row_count == 0:
        raise ValueError("No data found in CSV files")
    
    # Apply column mapping and transformations
    df = apply_column_mapping(df, NSDE_COLUMN_MAPPING)
    
    print(f"Mapped columns: {df.columns}")
    
    # Fix date columns - convert date strings to proper dates using configurable format
    date_columns = [col for col in df.columns if 'date' in col]
    print(f"Using date format: {date_format}")
    for date_col in date_columns:
        df = df.withColumn(
            date_col, 
            when(col(date_col).isNull() | (col(date_col) == ""), None)
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
    print(f"  - Raw files: {csv_count} CSVs saved")
    print(f"  - Bronze records: {row_count} processed")
    print("Note: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Bronze job completed")