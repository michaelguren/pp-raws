"""
Complete FDA CDER ETL Job - Download, Transform, and Store Products & Packages
Downloads FDA CDER NDC data, saves raw files for lineage, transforms to Bronze layer with dual tables
"""
import sys
import boto3  # type: ignore[import-not-found]
import posixpath
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit, col, when, to_date, expr, split, lpad, substring  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset',
    'source_url', 'bronze_database', 'raw_path', 'bronze_path', 'bronze_products_path', 'bronze_packages_path',
    'compression_codec', 'file_table_mapping', 'column_schema'
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
file_table_mapping = json.loads(args['file_table_mapping'])
column_schema = json.loads(args['column_schema']) if 'column_schema' in args else None

# S3 paths are already complete URLs from stack
raw_base_path = args['raw_path']
bronze_products_s3_path = args['bronze_products_path']
bronze_packages_s3_path = args['bronze_packages_path']

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
from etl_utils import download_and_extract  # type: ignore[import-not-found]

print(f"Starting Complete CDER ETL for {dataset} (download + transform dual tables)")
print(f"Source URL: {source_url}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze products path: {bronze_products_s3_path}")
print(f"Bronze packages path: {bronze_packages_s3_path}")
print(f"Run ID: {run_id}")
print(f"Bronze database: {bronze_database}")
print(f"Schema-driven: {'Yes' if column_schema else 'No'}")
print(f"Mode: overwrite (kill-and-fill)")

def apply_schema(df, column_schema):
    """Apply column schema including renaming and type casting"""
    if not column_schema:
        return df

    for source_col, config in column_schema.items():
        if source_col not in df.columns:
            continue

        target_col = config['target_name']
        col_type = config.get('type', 'string')

        # Rename column
        df = df.withColumnRenamed(source_col, target_col)

        # Apply type conversions
        if col_type == 'date':
            date_format = config.get('format', 'MM/dd/yyyy')  # CDER default
            df = df.withColumn(
                target_col,
                when(col(target_col).isNull() | (col(target_col) == ""), lit(None))
                .otherwise(to_date(col(target_col).cast("string"), date_format))
            )
        elif col_type == 'integer':
            df = df.withColumn(target_col, col(target_col).cast('integer'))
        elif col_type == 'decimal':
            df = df.withColumn(target_col, col(target_col).cast('decimal(10,2)'))
        elif col_type == 'boolean':
            df = df.withColumn(target_col, col(target_col).cast('boolean'))
        # string is default, no casting needed

    return df

def apply_column_mapping(df, column_mapping):
    """Apply column name mapping to DataFrame (legacy function)"""
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
    # Step 1: Download and extract using shared utilities
    print(f"Downloading from: {source_url}")

    result = download_and_extract(source_url, raw_s3_path, file_table_mapping)
    files_extracted = result["files_extracted"]
    content_length = result["content_length"]

    # Step 2: Process Products Table
    print("Processing products table...")

    products_filename = "product.txt"
    products_s3_path = posixpath.join(raw_s3_path, products_filename)
    print(f"Reading products from: {products_s3_path}")

    df_products = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .option("sep", "\t") \
                           .csv(products_s3_path)

    # Quick emptiness check before full count
    if not df_products.head(1):
        raise ValueError(f"No data found in products file: {products_s3_path}")

    products_row_count = df_products.count()
    print(f"Loaded {products_row_count} product records")

    # Apply schema if provided, otherwise use legacy column mapping
    if column_schema and 'products' in column_schema:
        df_products = apply_schema(df_products, column_schema['products'])
        print(f"Applied products schema, columns: {df_products.columns}")
    else:
        df_products = apply_column_mapping(df_products, PRODUCTS_COLUMN_MAPPING)
        # Legacy date handling
        date_columns = [col_name for col_name in df_products.columns if 'date' in col_name]
        print(f"Products date columns: {date_columns}")
        for date_col in date_columns:
            df_products = df_products.withColumn(
                date_col,
                when(col(date_col).isNull() | (col(date_col) == ""), lit(None))
                .otherwise(to_date(col(date_col).cast("string"), "MM/dd/yyyy"))
            )
        print(f"Applied legacy products mapping, columns: {df_products.columns}")

    # Add formatted NDC columns
    df_products = add_formatted_ndc_columns_products(df_products)

    # Add basic metadata (run_id serves as both identifier and timestamp)
    df_products_bronze = df_products.withColumn("meta_run_id", lit(run_id))

    print(f"Products final columns: {df_products_bronze.columns}")

    # Write products to bronze layer
    print(f"Writing products to: {bronze_products_s3_path}")

    df_products_bronze.write \
                     .mode("overwrite") \
                     .option("compression", args['compression_codec']) \
                     .parquet(bronze_products_s3_path)

    print(f"Successfully processed {products_row_count} product records to bronze layer")
    
    # Step 3: Process Packages Table
    print("Processing packages table...")

    packages_filename = "package.txt"
    packages_s3_path = posixpath.join(raw_s3_path, packages_filename)
    print(f"Reading packages from: {packages_s3_path}")

    df_packages = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .option("sep", "\t") \
                           .csv(packages_s3_path)

    # Quick emptiness check before full count
    if not df_packages.head(1):
        raise ValueError(f"No data found in packages file: {packages_s3_path}")

    packages_row_count = df_packages.count()
    print(f"Loaded {packages_row_count} package records")

    # Apply schema if provided, otherwise use legacy column mapping
    if column_schema and 'packages' in column_schema:
        df_packages = apply_schema(df_packages, column_schema['packages'])
        print(f"Applied packages schema, columns: {df_packages.columns}")
    else:
        df_packages = apply_column_mapping(df_packages, PACKAGES_COLUMN_MAPPING)
        # Legacy date handling
        date_columns = [col_name for col_name in df_packages.columns if 'date' in col_name]
        print(f"Packages date columns: {date_columns}")
        for date_col in date_columns:
            df_packages = df_packages.withColumn(
                date_col,
                when(col(date_col).isNull() | (col(date_col) == ""), lit(None))
                .otherwise(to_date(col(date_col).cast("string"), "MM/dd/yyyy"))
            )
        print(f"Applied legacy packages mapping, columns: {df_packages.columns}")

    # Add formatted NDC columns
    df_packages = add_formatted_ndc_columns_packages(df_packages)

    # Add basic metadata (run_id serves as both identifier and timestamp)
    df_packages_bronze = df_packages.withColumn("meta_run_id", lit(run_id))

    print(f"Packages final columns: {df_packages_bronze.columns}")

    # Write packages to bronze layer
    print(f"Writing packages to: {bronze_packages_s3_path}")

    df_packages_bronze.write \
                     .mode("overwrite") \
                     .option("compression", args['compression_codec']) \
                     .parquet(bronze_packages_s3_path)

    print(f"Successfully processed {packages_row_count} package records to bronze layer")
    
    print(f"Complete CDER ETL finished successfully:")
    print(f"  - Downloaded: {content_length or 'unknown'} bytes")
    print(f"  - Raw files: {files_extracted} files saved")
    print(f"  - Products bronze records: {products_row_count} processed")
    print(f"  - Packages bronze records: {packages_row_count} processed")
    print("Note: Run crawlers manually via console if schema changes are needed")

except Exception as e:
    print(f"CDER Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("CDER Bronze job completed")