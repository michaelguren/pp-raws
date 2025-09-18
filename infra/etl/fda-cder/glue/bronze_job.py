"""
Complete FDA CDER ETL Job - Download, Transform, and Store Products & Packages
Downloads FDA CDER NDC data, saves raw files for lineage, transforms to Bronze layer with dual tables
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
from pyspark.sql.functions import lit, col, when, to_date, expr, split, lpad, substring  # type: ignore[import-not-found]

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
bronze_products_path = f"{bronze_base_path}_products".rstrip('/')
bronze_packages_path = f"{bronze_base_path}_packages".rstrip('/')

print(f"Starting Complete CDER ETL for {dataset} (download + transform dual tables)")
print(f"Source URL: {source_url}")
print(f"Raw path: {raw_path}")
print(f"Bronze products path: {bronze_products_path}")
print(f"Bronze packages path: {bronze_packages_path}")
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

def add_formatted_ndc_columns_products(df):
    """Add formatted NDC columns for products table"""
    return df.withColumn("spl_id", split(col("product_id"), "_").getItem(1)) \
             .withColumn("formatted_ndc_9", 
                        expr("concat(lpad(split(product_ndc, '-')[0], 5, '0'), lpad(split(product_ndc, '-')[1], 4, '0'))")) \
             .withColumn("formatted_labeler_code", substring(col("formatted_ndc_9"), 1, 5)) \
             .withColumn("formatted_product_code", substring(col("formatted_ndc_9"), 6, 4))

def add_formatted_ndc_columns_packages(df):
    """Add formatted NDC columns for packages table"""
    return df.withColumn("spl_id", split(col("product_id"), "_").getItem(1)) \
             .withColumn("formatted_ndc", 
                        expr("concat(lpad(split(ndc_package_code, '-')[0], 5, '0'), lpad(split(ndc_package_code, '-')[1], 4, '0'), lpad(split(ndc_package_code, '-')[2], 2, '0'))")) \
             .withColumn("formatted_ndc_9", substring(col("formatted_ndc"), 1, 9)) \
             .withColumn("formatted_labeler_code", substring(col("formatted_ndc"), 1, 5)) \
             .withColumn("formatted_product_code", substring(col("formatted_ndc"), 6, 4))

# Column mappings from Ruby code
PRODUCTS_COLUMN_MAPPING = {
    "PRODUCTID": "product_id",
    "PRODUCTNDC": "product_ndc",
    "PRODUCTTYPENAME": "product_type_name",
    "PROPRIETARYNAME": "proprietary_name",
    "PROPRIETARYNAMESUFFIX": "proprietary_name_suffix", 
    "NONPROPRIETARYNAME": "non_proprietary_name",
    "DOSAGEFORMNAME": "dosage_form_name", 
    "ROUTENAME": "route_name",
    "STARTMARKETINGDATE": "start_marketing_date", 
    "ENDMARKETINGDATE": "end_marketing_date",
    "MARKETINGCATEGORYNAME": "marketing_category_name", 
    "APPLICATIONNUMBER": "application_number", 
    "LABELERNAME": "labeler_name", 
    "SUBSTANCENAME": "substance_name",
    "ACTIVE_NUMERATOR_STRENGTH": "active_numerator_strength", 
    "ACTIVE_INGRED_UNIT": "active_ingredient_unit", 
    "PHARM_CLASSES": "pharm_class", 
    "DEASCHEDULE": "dea_schedule",
    "NDC_EXCLUDE_FLAG": "ndc_exclude_flag",
    "LISTING_RECORD_CERTIFIED_THROUGH": "listing_record_certified_through"
}

PACKAGES_COLUMN_MAPPING = {
    "PRODUCTID": "product_id",
    "PRODUCTNDC": "product_ndc",
    "NDCPACKAGECODE": "ndc_package_code",
    "PACKAGEDESCRIPTION": "package_description",
    "STARTMARKETINGDATE": "start_marketing_date",
    "ENDMARKETINGDATE": "end_marketing_date",
    "NDC_EXCLUDE_FLAG": "ndc_exclude_flag",
    "LISTING_RECORD_CERTIFIED_THROUGH": "listing_record_certified_through"
}

try:
    # Step 1: Download and save raw data
    print(f"Downloading from: {source_url}")
    
    # Memory-efficient download and extraction using temporary file
    s3_client = boto3.client('s3')
    zip_key = f"raw/{dataset}/run_id={run_id}/source.zip"

    import tempfile
    import shutil
    from urllib.error import URLError, HTTPError
    import time

    max_retries = 3
    content_length = None
    txt_files = {}

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

                print(f"Downloaded to temporary file")

                # Upload original zip to S3 for lineage
                tmp_file.seek(0)
                s3_client.upload_fileobj(tmp_file, bucket_name, zip_key)
                print(f"Uploaded original zip to: s3://{bucket_name}/{zip_key}")

                # Extract TXT files and upload to S3 (with encoding conversion)
                tmp_file.seek(0)

                with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
                    for file_info in zip_ref.infolist():
                        if not file_info.is_dir() and file_info.filename.lower().endswith('.txt'):
                            print(f"Processing file: {file_info.filename}")

                            with zip_ref.open(file_info) as txt_file:
                                # Read as bytes and decode from ISO-8859-1 to UTF-8
                                raw_data = txt_file.read()
                                decoded_data = raw_data.decode('iso-8859-1').encode('utf-8')

                                txt_key = f"raw/{dataset}/run_id={run_id}/{file_info.filename}"

                                # Upload decoded text file to S3
                                s3_client.put_object(
                                    Bucket=bucket_name,
                                    Key=txt_key,
                                    Body=decoded_data,
                                    ContentType='text/plain; charset=utf-8'
                                )
                                txt_files[file_info.filename] = txt_key

                print(f"Successfully extracted and uploaded {len(txt_files)} TXT files: {list(txt_files.keys())}")
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
    
    if 'product.txt' not in txt_files or 'package.txt' not in txt_files:
        raise ValueError(f"Required files not found. Available files: {list(txt_files.keys())}")
    
    # Step 2: Process Products Table
    print("Processing products table...")
    
    products_s3_path = f"s3://{bucket_name}/{txt_files['product.txt']}"
    print(f"Reading products from: {products_s3_path}")
    
    df_products = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .option("sep", "\t") \
                           .csv(products_s3_path)
    
    products_row_count = df_products.count()
    print(f"Loaded {products_row_count} product records")
    
    if products_row_count == 0:
        raise ValueError("No product data found")
    
    # Apply column mapping and transformations
    df_products = apply_column_mapping(df_products, PRODUCTS_COLUMN_MAPPING)
    
    # Fix date columns
    date_columns = [col for col in df_products.columns if 'date' in col]
    print(f"Products date columns: {date_columns}")
    for date_col in date_columns:
        df_products = df_products.withColumn(
            date_col, 
            when(col(date_col).isNull() | (col(date_col) == ""), None)
            .otherwise(to_date(col(date_col).cast("string"), date_format))
        )
    
    # Add formatted NDC columns
    df_products = add_formatted_ndc_columns_products(df_products)
    
    # Add basic metadata (run_id serves as both identifier and timestamp)
    df_products_bronze = df_products.withColumn("meta_run_id", lit(run_id))
    
    print(f"Products final columns: {df_products_bronze.columns}")
    
    # Write products to bronze layer
    print(f"Writing products to: {bronze_products_path}")
    
    df_products_bronze.write \
                     .mode("overwrite") \
                     .option("compression", args['compression_codec']) \
                     .parquet(bronze_products_path)
    
    print(f"Successfully processed {products_row_count} product records to bronze layer")
    
    # Step 3: Process Packages Table
    print("Processing packages table...")
    
    packages_s3_path = f"s3://{bucket_name}/{txt_files['package.txt']}"
    print(f"Reading packages from: {packages_s3_path}")
    
    df_packages = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .option("sep", "\t") \
                           .csv(packages_s3_path)
    
    packages_row_count = df_packages.count()
    print(f"Loaded {packages_row_count} package records")
    
    if packages_row_count == 0:
        raise ValueError("No package data found")
    
    # Apply column mapping and transformations
    df_packages = apply_column_mapping(df_packages, PACKAGES_COLUMN_MAPPING)
    
    # Fix date columns
    date_columns = [col for col in df_packages.columns if 'date' in col]
    print(f"Packages date columns: {date_columns}")
    for date_col in date_columns:
        df_packages = df_packages.withColumn(
            date_col, 
            when(col(date_col).isNull() | (col(date_col) == ""), None)
            .otherwise(to_date(col(date_col).cast("string"), date_format))
        )
    
    # Add formatted NDC columns
    df_packages = add_formatted_ndc_columns_packages(df_packages)
    
    # Add basic metadata (run_id serves as both identifier and timestamp)
    df_packages_bronze = df_packages.withColumn("meta_run_id", lit(run_id))
    
    print(f"Packages final columns: {df_packages_bronze.columns}")
    
    # Write packages to bronze layer
    print(f"Writing packages to: {bronze_packages_path}")
    
    df_packages_bronze.write \
                     .mode("overwrite") \
                     .option("compression", args['compression_codec']) \
                     .parquet(bronze_packages_path)
    
    print(f"Successfully processed {packages_row_count} package records to bronze layer")
    
    print(f"Complete CDER ETL finished successfully:")
    print(f"  - Downloaded: {content_length or 'unknown'} bytes")
    print(f"  - Raw files: {len(txt_files)} TXT files saved")
    print(f"  - Products bronze records: {products_row_count} processed")
    print(f"  - Packages bronze records: {packages_row_count} processed")
    print("Note: Run crawlers manually via console if schema changes are needed")

except Exception as e:
    print(f"CDER Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("CDER Bronze job completed")