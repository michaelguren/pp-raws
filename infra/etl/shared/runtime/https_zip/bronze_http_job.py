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
import boto3 # type: ignore[import-not-found]
# AWS Glue imports - only available in Glue runtime environment
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit, col, when, to_date  # type: ignore[import-not-found]

# Required job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset', 'bronze_database',
    'raw_path', 'bronze_path',
    'file_table_mapping', 'column_schema', 'source_url',
    'compression_codec', 'delimiter'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])
spark.conf.set("spark.sql.parquet.summary.metadata.level", "ALL")

# Generate run_id for this execution
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Parse configuration from job arguments
dataset = args['dataset']
source_url = args['source_url']
bronze_database = args['bronze_database']
file_table_mapping = json.loads(args['file_table_mapping'])
column_schema = json.loads(args['column_schema']) if 'column_schema' in args else {}

# S3 paths from stack
raw_base_path = args['raw_path']
bronze_base_path = args['bronze_path']

# Append run_id to raw path for lineage tracking
scheme, path = raw_base_path.rstrip('/').split('://', 1)
raw_s3_path = f"{scheme}://{posixpath.join(path, f'run_id={run_id}')}/"

# Extract bucket name for downloading etl_utils
raw_path_parts = raw_s3_path.replace('s3://', '').split('/', 1)
raw_bucket = raw_path_parts[0]

# Load shared ETL utilities
sys.path.append('/tmp')
s3_client = boto3.client('s3')
s3_client.download_file(raw_bucket, 'etl/shared/runtime/https_zip/etl_utils.py', '/tmp/etl_utils.py')
from etl_utils import download_and_extract  # type: ignore[import-not-found]

print(f"Starting Bronze ETL for {dataset}")
print(f"Source URL: {source_url}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze base path: {bronze_base_path}")
print(f"Run ID: {run_id}")
print(f"Bronze database: {bronze_database}")
print(f"Tables to process: {list(file_table_mapping.values())}")
print(f"Schema-driven: {'Yes' if column_schema else 'No'}")

def apply_schema(df, table_schema):
    """Apply column schema including renaming and type casting"""
    if not table_schema:
        return df

    for source_col, config in table_schema.items():
        if source_col not in df.columns:
            print(f"Warning: Column '{source_col}' not found in dataframe, skipping")
            continue

        target_col = config['target_name']
        col_type = config.get('type', 'string')

        # Rename column
        df = df.withColumnRenamed(source_col, target_col)

        # Apply type conversions
        if col_type == 'date':
            date_format = config.get('format', 'yyyyMMdd')
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

try:
    # Step 1: Download and extract files to raw layer
    print(f"Downloading from: {source_url}")
    result = download_and_extract(source_url, raw_s3_path, file_table_mapping)
    files_extracted = result["files_extracted"]
    content_length = result.get("content_length", "unknown")

    print(f"Downloaded {content_length} bytes, extracted {files_extracted} files")

    # Step 2: Process each file/table
    total_records_processed = 0
    tables_processed = []

    for source_file, table_name in file_table_mapping.items():
        print(f"\nProcessing table: {table_name} from file: {source_file}")

        # Read source file from raw layer
        source_s3_path = posixpath.join(raw_s3_path, source_file)
        print(f"Reading from: {source_s3_path}")

        # Get delimiter from config (default to comma if not specified)
        delimiter = args.get('delimiter', ",")
        print(f"Reading {source_file} with delimiter: {repr(delimiter)}")

        # Read all columns as strings, schema transformations applied later
        df = spark.read.option("header", "true") \
                      .option("inferSchema", "false") \
                      .option("delimiter", delimiter) \
                      .csv(source_s3_path)

        # Validate data exists
        if not df.head(1):
            raise ValueError(f"No data found in {source_file}")

        row_count = df.count()
        print(f"Loaded {row_count} records from {source_file}")

        # Apply schema transformations if provided
        table_schema = column_schema.get(table_name, None)
        if table_schema:
            print(f"Applying schema transformations for {table_name}")
            df = apply_schema(df, table_schema)
            print(f"Schema applied. Columns: {df.columns[:5]}..." if len(df.columns) > 5 else f"Columns: {df.columns}")
        else:
            print(f"No schema provided for {table_name}, using inferred schema")

        # Add metadata
        df_bronze = df.withColumn("meta_run_id", lit(run_id))

        # Determine output path based on number of tables
        if len(file_table_mapping) == 1:
            # Single table: write directly to bronze path
            bronze_output_path = bronze_base_path
            print(f"Writing to bronze layer (single-table): {bronze_output_path}")
        else:
            # Multi-table: write to table-specific subdirectory
            bronze_output_path = posixpath.join(bronze_base_path.rstrip('/'), table_name) + '/'
            bronze_output_path = bronze_output_path.replace('s3://', 's3://')  # Ensure proper protocol
            print(f"Writing to bronze layer (multi-table): {bronze_output_path}")

        # Write to bronze layer
        df_bronze.write \
                 .mode("overwrite") \
                 .option("compression", args['compression_codec']) \
                 .parquet(bronze_output_path)

        print(f"Successfully wrote {row_count} records for {table_name}")
        total_records_processed += row_count
        tables_processed.append(table_name)

    # Summary
    print(f"\nBronze ETL completed successfully:")
    print(f"  - Downloaded: {content_length} bytes")
    print(f"  - Raw files saved: {files_extracted}")
    print(f"  - Total records processed: {total_records_processed}")
    print(f"  - Tables processed: {', '.join(tables_processed)}")
    print("\nNote: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"Bronze job error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()
    print("Bronze job committed")