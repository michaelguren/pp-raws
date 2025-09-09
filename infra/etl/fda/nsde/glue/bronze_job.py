"""
Simple NSDE Bronze Job - minimal dependencies
Reads raw CSV from S3 and converts to Parquet
"""
import sys
import boto3
import copy
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp, col, regexp_replace, when, to_date
from pyspark.sql.types import IntegerType, DateType
from datetime import datetime

# Get job parameters - only runtime essentials
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'run_id', 'bucket_name'])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Load configuration
s3_client = boto3.client('s3')
bucket_name = args['bucket_name']
run_id = args['run_id']

# Load warehouse config
warehouse_config_key = 'scripts/config/warehouse.json'
warehouse_config = json.loads(
    s3_client.get_object(Bucket=bucket_name, Key=warehouse_config_key)['Body'].read()
)

# Load dataset config  
dataset_config_key = 'scripts/fda/nsde/config/dataset.json'
dataset_config = json.loads(
    s3_client.get_object(Bucket=bucket_name, Key=dataset_config_key)['Body'].read()
)

# Extract configuration values
dataset = dataset_config['dataset']
date_format = dataset_config['date_format']
bronze_database = warehouse_config['bronze_database']

# Build paths from config
raw_path = f"s3://{bucket_name}/raw/{dataset}/run_id={run_id}/"
bronze_path = f"s3://{bucket_name}/bronze/bronze_{dataset}/partition_datetime={run_id}/"

print(f"Starting Bronze ETL for {dataset}")
print(f"Raw path: {raw_path}")
print(f"Bronze path: {bronze_path}")
print(f"Run ID: {run_id}")
print(f"Date format: {date_format}")
print(f"Bronze database: {bronze_database}")

try:
    # Read CSV files from raw path
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
    
    # Add basic metadata  
    df_bronze = df.withColumn("meta_ingest_timestamp", current_timestamp()) \
                  .withColumn("meta_run_id", lit(run_id))
    
    # Write to bronze layer
    print(f"Writing to: {bronze_path}")
    
    df_bronze.write \
             .mode("append") \
             .option("compression", "zstd") \
             .parquet(bronze_path)
    
    print(f"Successfully processed {row_count} records")

    # Register partition with Glue catalog
    print("Registering partition with Glue catalog...")
    glue_client = boto3.client('glue')
    
    # Robust partition value extraction
    partition_key = 'partition_datetime'
    path_parts = [part for part in bronze_path.split('/') if part]
    partition_value = None
    
    for part in path_parts:
        if part.startswith(f"{partition_key}="):
            partition_value = part.split('=', 1)[1]
            break
    
    if not partition_value:
        print(f"Warning: Partition key '{partition_key}' not found in bronze_path: {bronze_path}")
        print("This could indicate a path format change or different dataset structure")
        raise ValueError(f"Partition key '{partition_key}' not found in bronze_path: {bronze_path}")
    
    print(f"Extracted partition value: {partition_value}")
    
    try:
        # Try to get table metadata - if table doesn't exist, trigger crawler first
        try:
            table = glue_client.get_table(
                DatabaseName=bronze_database,
                Name=dataset
            )['Table']
            print("Bronze table found, proceeding with partition registration")
            
        except glue_client.exceptions.EntityNotFoundException:
            print("Bronze table not found, triggering crawler to create it...")
            
            # Start the crawler to create the initial table
            crawler_name = f"{warehouse_config['warehouse_prefix']}-bronze-{dataset}-crawler"
            
            try:
                # Check if crawler is already running
                crawler_response = glue_client.get_crawler(Name=crawler_name)
                if crawler_response['Crawler']['State'] == 'RUNNING':
                    print("Crawler already running, waiting for completion...")
                else:
                    print("Starting crawler...")
                    glue_client.start_crawler(Name=crawler_name)
                
                # Wait for crawler to complete
                print("Waiting for crawler to complete table creation...")
                import time
                max_wait_time = 600  # 10 minutes max
                wait_interval = 30   # Check every 30 seconds
                total_waited = 0
                
                while total_waited < max_wait_time:
                    time.sleep(wait_interval)
                    total_waited += wait_interval
                    
                    crawler_status = glue_client.get_crawler(Name=crawler_name)
                    state = crawler_status['Crawler']['State']
                    print(f"Crawler state: {state} (waited {total_waited}s)")
                    
                    if state == 'READY':
                        print("Crawler completed successfully")
                        break
                    elif state == 'STOPPING':
                        print("Crawler is stopping...")
                        continue
                    elif state in ['RUNNING', 'STOPPING']:
                        continue
                    else:
                        raise ValueError(f"Crawler failed with state: {state}")
                
                if total_waited >= max_wait_time:
                    print(f"Error: Crawler timed out after {max_wait_time//60} minutes")
                    print("Cannot proceed without confirmed table creation")
                    raise ValueError(f"Crawler timed out after {max_wait_time//60} minutes - table creation uncertain")
                
                # Now get the newly created table
                table = glue_client.get_table(
                    DatabaseName=bronze_database,
                    Name=dataset
                )['Table']
                print("Successfully retrieved table metadata after crawler completion")
                
            except Exception as crawler_error:
                print(f"Error with crawler: {str(crawler_error)}")
                # Try to continue anyway - maybe table was created by another process
                try:
                    table = glue_client.get_table(
                        DatabaseName=bronze_database,
                        Name=dataset
                    )['Table']
                    print("Table found despite crawler error, continuing...")
                except:
                    raise ValueError(f"Could not create or find bronze table: {str(crawler_error)}")
        
        # Deep copy storage descriptor to avoid modifying nested structures
        storage_desc = copy.deepcopy(table['StorageDescriptor'])
        storage_desc['Location'] = bronze_path
        
        # Check if partition exists
        try:
            glue_client.get_partition(
                DatabaseName=bronze_database,
                TableName=dataset,
                PartitionValues=[partition_value]
            )
            print(f"Partition {partition_value} already exists, skipping creation")
        except glue_client.exceptions.EntityNotFoundException:
            # Create new partition
            glue_client.create_partition(
                DatabaseName=bronze_database,
                TableName=dataset,
                PartitionInput={
                    'Values': [partition_value],
                    'StorageDescriptor': storage_desc
                }
            )
            print(f"Successfully registered partition: {partition_value}")
            
    except Exception as e:
        print(f"Warning: Could not register partition: {str(e)}")
        # Don't fail the job - partition registration is nice-to-have

except Exception as e:
    print(f"Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Bronze job completed")