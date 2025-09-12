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
    'source_url', 'bronze_database', 'raw_base_path', 'bronze_base_path', 
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

# Generate human-readable runtime run_id
from datetime import datetime
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: 20240311_143022

# Get configuration from job arguments
bucket_name = args['bucket_name']
dataset = args['dataset']
source_url = args['source_url']
bronze_database = args['bronze_database']
date_format = args['date_format']

# Build paths from pre-computed base paths
raw_base_path = args['raw_base_path']
bronze_base_path = args['bronze_base_path']

raw_path = f"{raw_base_path}run_id={run_id}/"
bronze_path = bronze_base_path.rstrip('/')

print(f"Starting Complete ETL for {dataset} (download + transform)")
print(f"Source URL: {source_url}")
print(f"Raw path: {raw_path}")
print(f"Bronze path: {bronze_path}")
print(f"Run ID: {run_id}")
print(f"Date format: {date_format}")
print(f"Bronze database: {bronze_database}")
print(f"Mode: overwrite (kill-and-fill)")

try:
    # Step 1: Download and save raw data
    print(f"Downloading from: {source_url}")
    
    # Download with timeout
    with urllib.request.urlopen(source_url, timeout=300) as response:
        zip_data = response.read()
    
    print(f"Downloaded {len(zip_data)} bytes")
    
    # Save original zip file to raw layer for lineage
    s3_client = boto3.client('s3')
    zip_key = f"raw/{dataset}/run_id={run_id}/source.zip"
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=zip_key,
        Body=zip_data,
        ContentType='application/zip'
    )
    print(f"Saved original zip to: s3://{bucket_name}/{zip_key}")
    
    # Extract and save CSV files to raw layer
    csv_count = 0
    with zipfile.ZipFile(BytesIO(zip_data)) as z:
        for file_name in z.namelist():
            if file_name.lower().endswith('.csv'):
                with z.open(file_name) as csv_file:
                    csv_data = csv_file.read()
                    csv_key = f"raw/{dataset}/run_id={run_id}/{file_name}"
                    
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=csv_key,
                        Body=csv_data,
                        ContentType='text/csv'
                    )
                    csv_count += 1
    
    print(f"Extracted and saved {csv_count} CSV files to raw layer")
    
    # Step 2: Read CSV files from raw path for transformation
    print(f"Reading CSV from: {raw_path}*.csv")
    
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(f"{raw_path}*.csv")
    
    row_count = df.count()
    print(f"Loaded {row_count} records")
    
    if row_count == 0:
        raise ValueError("No data found in CSV files")
    
    print("Cleaning column names and data types...")
    
    # Clean column names: lowercase, replace spaces/special chars with underscores
    def clean_column_name(name):
        import re
        return re.sub(r'[^a-z0-9]+', '_', name.lower().strip()).strip('_')
    
    # Rename columns
    for old_col in df.columns:
        clean_name = clean_column_name(old_col)
        if old_col != clean_name:
            df = df.withColumnRenamed(old_col, clean_name)
    
    print(f"Cleaned columns: {df.columns}")
    
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
    print(f"Writing to: {bronze_path}")
    
    df_bronze.write \
             .mode("overwrite") \
             .option("compression", args['compression_codec']) \
             .parquet(bronze_path)
    
    print(f"Successfully processed {row_count} records to bronze layer")
    
    print(f"Complete ETL finished successfully:")
    print(f"  - Downloaded: {len(zip_data)} bytes")
    print(f"  - Raw files: {csv_count} CSVs saved")
    print(f"  - Bronze records: {row_count} processed")
    print("Note: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Bronze job completed")