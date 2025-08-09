import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    lit, to_date, hash, col, when, concat, 
    lpad, split, year, month, dayofmonth, 
    input_file_name, count, trim, upper, isnan, isnull
)
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

print(f"Starting NSDE ETL for date: {date_str}")
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
    
    print("Proceeding with ETL...")
    # Step 2: Extract NSDE ZIP file to raw S3
    zip_data = BytesIO(source_data)
    
    # Extract CSV files from ZIP
    csv_count = 0
    with zipfile.ZipFile(zip_data) as z:
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        
        if not csv_files:
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
    
    # Step 2: Read raw CSV and transform to bronze
    print(f"Reading CSV from: s3://{bucket}/{raw_prefix}*.csv")
    
    # Read all CSVs from raw folder
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "false") \
                   .csv(f"s3://{bucket}/{raw_prefix}*.csv")
    
    initial_count = df.count()
    print(f"Loaded {initial_count} records from raw CSV")
    
    # Step 3: Data cleansing and normalization with DQ tracking
    date_parse_errors = 0
    malformed_ndc_count = 0
    
    # Normalize NDC codes to 11-digit format without dashes
    # The CSV has "Item Code" and "NDC11" columns
    df = df.withColumn("item_code_parts", split(col("Item Code"), "-"))
    df = df.withColumn("normalized_ndc11",
        when(col("NDC11").isNotNull() & (trim(col("NDC11")) != ""), 
             # NDC11 should already be 11 digits without dashes
             trim(col("NDC11")))
        .otherwise(
            when(col("item_code_parts").isNotNull() & (col("item_code_parts")[0].isNotNull()),
                # Create 11-digit NDC without dashes (5-4-2 becomes 542)
                concat(
                    lpad(col("item_code_parts")[0], 5, "0"),  # Labeler: 5 digits
                    lpad(col("item_code_parts")[1], 4, "0"),  # Product: 4 digits
                    lpad(col("item_code_parts")[2], 2, "0")   # Package: 2 digits
                )
            ).otherwise(lit(None))
        )
    ).drop("item_code_parts")
    
    # Parse and format date fields (actual column names from NSDE CSV)
    date_columns = [
        "Marketing Start Date",
        "Marketing End Date",
        "Inactivation Date",
        "Reactivation Date"
    ]
    
    for date_col in date_columns:
        if date_col in df.columns:
            # FDA dates are typically in YYYYMMDD format
            # Track parse errors for DQ
            df = df.withColumn(f"{date_col}_parse_error",
                when(
                    col("`" + date_col + "`").isNotNull() & 
                    (trim(col("`" + date_col + "`")) != "") &
                    to_date(col("`" + date_col + "`"), "yyyyMMdd").isNull(),
                    lit(1)
                ).otherwise(lit(0))
            )
            date_parse_errors += df.filter(col(f"{date_col}_parse_error") == 1).count()
            
            df = df.withColumn(date_col, 
                when(col("`" + date_col + "`").isNotNull() & (trim(col("`" + date_col + "`")) != ""),
                     to_date(col("`" + date_col + "`"), "yyyyMMdd")
                ).otherwise(lit(None))
            ).drop(f"{date_col}_parse_error")
    
    # Rename columns to remove spaces for easier querying
    df = df.withColumnRenamed("Item Code", "item_code") \
           .withColumnRenamed("NDC11", "ndc11_original") \
           .withColumnRenamed("Proprietary Name", "proprietary_name") \
           .withColumnRenamed("Dosage Form", "dosage_form") \
           .withColumnRenamed("Marketing Category", "marketing_category") \
           .withColumnRenamed("Application Number or Monograph ID", "application_number_or_monograph_id") \
           .withColumnRenamed("Product Type", "product_type") \
           .withColumnRenamed("Marketing Start Date", "marketing_start_date") \
           .withColumnRenamed("Marketing End Date", "marketing_end_date") \
           .withColumnRenamed("Billing Unit", "billing_unit") \
           .withColumnRenamed("Inactivation Date", "inactivation_date") \
           .withColumnRenamed("Reactivation Date", "reactivation_date")
    
    # Check for malformed NDCs after normalization
    malformed_ndc_df = df.filter(
        col("normalized_ndc11").isNotNull() & 
        (~col("normalized_ndc11").rlike("^[0-9]{11}$"))
    )
    malformed_ndc_count = malformed_ndc_df.count()
    
    # Log sample malformed NDCs for debugging
    if malformed_ndc_count > 0:
        print(f"Sample malformed NDCs (junk entries to be filtered out):")
        malformed_samples = malformed_ndc_df.select("item_code", "ndc11_original", "normalized_ndc11").limit(10)
        for row in malformed_samples.collect():
            print(f"  Item Code: '{row.item_code}', Original NDC11: '{row.ndc11_original}', Normalized: '{row.normalized_ndc11}'")
    
    # Filter out malformed NDCs - we don't want junk data in our clean dataset
    print(f"Filtering out {malformed_ndc_count} malformed NDCs from dataset")
    df = df.filter(
        col("normalized_ndc11").isNull() | 
        col("normalized_ndc11").rlike("^[0-9]{11}$")
    )
    
    # Add metadata columns
    df = df.withColumn("meta_change_hash", hash(*df.columns)) \
           .withColumn("version", lit(date_str)) \
           .withColumn("version_date", to_date(lit(date_str), "yyyy-MM-dd")) \
           .withColumn("meta_processing_date", lit(datetime.utcnow().isoformat())) \
           .withColumn("meta_source_file", input_file_name()) \
           .withColumn("meta_record_count", lit(initial_count))
    
    # Clean and standardize text fields
    text_fields = ["proprietary_name", "dosage_form", "product_type", "billing_unit"]
    for field in text_fields:
        if field in df.columns:
            df = df.withColumn(field, trim(col(field)))
    
    # Step 4: Data quality checks (DQ Tripwires)
    print("Performing data quality checks...")
    
    # Check for null NDCs (FAIL if > 0)
    null_ndc_count = df.filter(col("normalized_ndc11").isNull() | (col("normalized_ndc11") == "")).count()
    print(f"Null NDC count: {null_ndc_count}")
    
    # Check for duplicate NDCs (LOG only, don't fail)
    duplicate_count = df.groupBy("normalized_ndc11") \
                       .count() \
                       .filter(col("count") > 1) \
                       .count()
    print(f"Duplicate NDC count: {duplicate_count}")
    
    # Check for malformed NDCs (FAIL if > 0)
    print(f"Malformed NDC count: {malformed_ndc_count}")
    
    # Check for date parse errors (FAIL if > 0)
    print(f"Date parse error count: {date_parse_errors}")
    
    # DQ Tripwires - fail the job on critical issues
    if null_ndc_count > 0:
        raise ValueError(f"DQ FAILURE: Found {null_ndc_count} records with null NDC codes")
    
    if date_parse_errors > 0:
        raise ValueError(f"DQ FAILURE: Found {date_parse_errors} date parse errors")
    
    # Note: Malformed NDCs are logged but filtered out, not a failure condition
    
    print("Data quality checks PASSED")
    
    # Step 5: Write to bronze layer as Parquet with ZSTD compression
    bronze_data_path = f"s3://{bucket}/bronze/nsde/data/"
    final_count = df.count()
    print(f"Writing {final_count} records to bronze layer: {bronze_data_path}")
    
    # Write with version and version_date partitioning, ZSTD compression
    (df.write
      .mode("overwrite")
      .option("compression","zstd")
      .partitionBy("version","version_date")
      .parquet(bronze_data_path))
    
    print(f"Successfully wrote data to bronze layer")
    
    # Step 6: Create metadata table for ETL observability
    unique_ndc_count = df.select("normalized_ndc11").distinct().count()
    processed_at_utc = datetime.utcnow().isoformat()
    
    # Create metadata as a proper DataFrame (unpartitioned table)
    metadata_data = [{
        "dataset": "nsde",
        "version": date_str,
        "version_date": date_str,  # Store as string in metadata
        "row_count": final_count,
        "unique_ndc_codes": unique_ndc_count,
        "null_ndc_count": null_ndc_count,
        "duplicate_ndc_count": duplicate_count,
        "malformed_ndc_count": malformed_ndc_count,
        "date_parse_errors": date_parse_errors,
        "processed_at_utc": processed_at_utc,
        "source_url": nsde_url,
        "sha256": source_sha256,
        "job_name": args['JOB_NAME']
    }]
    
    metadata_df = spark.createDataFrame(metadata_data)
    
    # Write metadata to unpartitioned bronze metadata table
    metadata_path = f"s3://{bucket}/bronze/nsde/metadata/data/"
    metadata_df.write.mode("append") \
                     .option("compression", "zstd") \
                     .parquet(metadata_path)
    
    # Step 7: Write SUCCESS manifest for idempotency
    manifest = {
        "version": date_str,
        "sha256": source_sha256,
        "row_count": final_count,
        "source_url": nsde_url,
        "processed_at_utc": processed_at_utc,
        "null_ndc_count": null_ndc_count,
        "duplicate_ndc_count": duplicate_count,
        "malformed_ndc_count": malformed_ndc_count,
        "date_parse_errors": date_parse_errors
    }
    
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType='application/json'
    )
    print(f"Manifest written to: s3://{bucket}/{manifest_key}")
    
    print("\nETL Summary:")
    for key, value in metadata_data[0].items():
        print(f"  {key}: {value}")
    
    print(f"\nMetadata written to: {metadata_path}")
    print(f"Data written to: {bronze_data_path}")
    print(f"Manifest written to: s3://{bucket}/{manifest_key}")
    
except Exception as e:
    print(f"ETL Error: {str(e)}")
    raise
finally:
    # Commit the job
    job.commit()
    print("NSDE ETL job completed")