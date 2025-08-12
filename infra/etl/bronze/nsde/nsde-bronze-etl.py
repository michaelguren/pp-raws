"""
NSDE Bronze ETL - Minimal Ingestion Only
According to Medallion architecture:
- Bronze: Minimal ingestion only—download/extract from FDA, convert to Parquet with ZSTD compression,
  infer schema, add basic metadata (e.g., ingest timestamp, source SHA256), partition by version_date
- No cleansing, normalization, filtering, or DQ tripwires (those belong in Silver)
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, input_file_name, current_timestamp, to_date
from datetime import datetime
import urllib.request
import zipfile
from io import BytesIO
import boto3
import time
import hashlib
import json

# Get job parameters (make --force optional with default false)
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'database_name', 'force'])
    force_run = args['force'].lower() == 'true'
except:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'database_name'])
    force_run = False

# Initialize Spark/Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark for ZSTD compression
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Initialize S3 client
s3 = boto3.client('s3')

# Configuration
bucket = args['bucket_name']
database = args['database_name']
date_str = datetime.utcnow().strftime('%Y-%m-%d')
raw_prefix = f'raw/nsde/{date_str}/'
bronze_prefix = f'bronze/nsde/'
manifest_prefix = f'bronze/nsde/metadata/summary_version={date_str}/'

# FDA NSDE download URL - Comprehensive NDC SPL Data Elements File
nsde_url = 'https://download.open.fda.gov/Comprehensive_NDC_SPL_Data_Elements_File.zip'

print(f"Starting NSDE Bronze ETL for date: {date_str}")
print(f"Data lake bucket: {bucket}")
print(f"Database: {database}")
print(f"Force run: {force_run}")

try:
    # Step 0: Check for idempotency manifest
    manifest_key = f'{manifest_prefix}_SUCCESS.json'
    prev_sha256 = None
    source_data = None
    
    # Read manifest if exists (to get previous SHA256)
    if not force_run:
        try:
            response = s3.get_object(Bucket=bucket, Key=manifest_key)
            manifest = json.loads(response['Body'].read())
            prev_sha256 = manifest.get('sha256')
            print(f"Found existing manifest for {date_str} with SHA256: {prev_sha256}")
        except s3.exceptions.NoSuchKey:
            print(f"No manifest found for {date_str}. First run for this date.")
        except Exception as e:
            print(f"Error reading manifest: {str(e)}. Treating as first run.")
    
    # Step 1: Download source and compute current SHA256
    print(f"Downloading NSDE data from: {nsde_url}")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(nsde_url, timeout=60) as response:
                source_data = response.read()
                source_sha256 = hashlib.sha256(source_data).hexdigest()
            print(f"Successfully downloaded ZIP file (attempt {attempt + 1})")
            print(f"Source SHA256: {source_sha256}")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                raise Exception(f"Failed to download after {max_retries} attempts: {str(e)}")
            print(f"Download attempt {attempt + 1} failed, retrying...")
            time.sleep(2 ** attempt)
    
    # Idempotency short-circuit 
    if prev_sha256 and prev_sha256 == source_sha256 and not force_run:
        print("NSDE source unchanged; skipping transform.")
        job.commit()
        sys.exit(0)
    elif prev_sha256 and prev_sha256 != source_sha256:
        print(f"Source data changed. Previous: {prev_sha256}, Current: {source_sha256}")
    
    print("Proceeding with Bronze ETL...")
    
    # Step 2: Extract NSDE ZIP file to raw S3
    zip_data = BytesIO(source_data)
    
    # Extract CSV files from ZIP
    csv_count = 0
    with zipfile.ZipFile(zip_data) as z:
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        
        if not csv_files:
            print(f"ERROR: No CSV files found in downloaded ZIP")
            print(f"ZIP contents: {z.namelist()}")
            print(f"ZIP file size: {len(source_data)} bytes")
            raise ValueError("No CSV files found in downloaded ZIP")
        
        for csv_name in csv_files:
            with z.open(csv_name) as csv_file:
                key = f'{raw_prefix}{csv_name}'
                s3.put_object(
                    Bucket=bucket, 
                    Key=key, 
                    Body=csv_file.read(),
                    ContentType='text/csv'
                )
                print(f"Uploaded raw CSV to: s3://{bucket}/{key}")
                csv_count += 1
    
    print(f"Successfully extracted {csv_count} CSV file(s) to raw layer")
    
    # Step 3: Read raw CSV and convert to Parquet with minimal processing
    print(f"Reading CSV from: s3://{bucket}/{raw_prefix}*.csv")
    
    # Read all CSVs from raw folder - INFER SCHEMA for Bronze
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(f"s3://{bucket}/{raw_prefix}*.csv")
    
    row_count_raw = df.count()
    print(f"Loaded {row_count_raw} records from raw CSV")
    
    # Validate we have data
    if row_count_raw == 0:
        print(f"ERROR: CSV file(s) contain no data")
        print(f"CSV files processed: {csv_files}")
        raise ValueError("CSV files contain no data")
    
    # Add basic metadata columns for Bronze layer (minimal metadata only)
    df = df.withColumn("meta_ingest_timestamp", current_timestamp()) \
           .withColumn("meta_source_sha256", lit(source_sha256)) \
           .withColumn("meta_source_url", lit(nsde_url)) \
           .withColumn("meta_source_file", input_file_name()) \
           .withColumn("version_date", to_date(lit(date_str), "yyyy-MM-dd"))
    
    # Write to bronze layer as Parquet with ZSTD compression
    bronze_data_path = f"s3://{bucket}/bronze/nsde/data/"
    print(f"Writing {row_count_raw} records to bronze layer: {bronze_data_path}")
    
    # Write with version_date partitioning, ZSTD compression
    # Use append mode to preserve history if re-run on same partition
    (df.write
      .mode("append")
      .option("compression","zstd")
      .partitionBy("version_date")
      .parquet(bronze_data_path))
    
    print(f"Successfully wrote data to bronze layer")
    
    # Step 4: Create metadata table for ETL observability (Bronze layer only tracks ingestion stats)
    # Only write metadata and manifest after successful data write to avoid partial states
    processed_at_utc = datetime.utcnow().isoformat()
    
    # Create metadata as a proper DataFrame (unpartitioned table)
    metadata_data = [{
        "dataset": "nsde",
        "layer": "bronze",
        "version": date_str,
        "version_date": date_str,  # Store as string in metadata
        "row_count_raw": row_count_raw,
        "processed_at_utc": processed_at_utc,
        "source_url": nsde_url,
        "sha256": source_sha256,
        "job_name": args['JOB_NAME']
    }]
    
    # Repartition metadata to single file (small dataset)
    metadata_df = spark.createDataFrame(metadata_data).repartition(1)
    
    # Write metadata to unpartitioned bronze metadata table
    metadata_path = f"s3://{bucket}/bronze/nsde/metadata/data/"
    metadata_df.write.mode("append") \
                     .option("compression", "zstd") \
                     .parquet(metadata_path)
    
    print(f"Metadata written to: {metadata_path}")
    
    # Step 5: Write SUCCESS manifest for idempotency (only after all writes succeed)
    manifest = {
        "version": date_str,
        "layer": "bronze",
        "sha256": source_sha256,
        "row_count_raw": row_count_raw,
        "source_url": nsde_url,
        "processed_at_utc": processed_at_utc
    }
    
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType='application/json'
    )
    print(f"Manifest written to: s3://{bucket}/{manifest_key}")
    
    print("\nBronze ETL Summary:")
    for key, value in metadata_data[0].items():
        print(f"  {key}: {value}")
    
    print(f"\nMetadata written to: {metadata_path}")
    print(f"Data written to: {bronze_data_path}")
    print(f"Manifest written to: s3://{bucket}/{manifest_key}")
    
except Exception as e:
    print(f"Bronze ETL Error: {str(e)}")
    raise
finally:
    # Commit the job
    job.commit()
    print("NSDE Bronze ETL job completed")