"""
NSDE Silver Job with SCD Type 2 Implementation
Reads bronze data and applies SCD Type 2 versioning for change tracking
"""
import sys
import boto3  # type: ignore[import-not-found]
import copy
import json
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import (  # type: ignore[import-not-found]
    lit, col, when, concat,
    current_timestamp, md5, coalesce,
    row_number, desc
)
from pyspark.sql.window import Window  # type: ignore[import-not-found]
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
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

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
bronze_database = warehouse_config['bronze_database']
silver_database = warehouse_config['silver_database']
process_date = datetime.now().strftime('%Y-%m-%d')

# Build paths from config - read from latest bronze partition
bronze_path = f"s3://{bucket_name}/bronze/bronze_{dataset}/partition_datetime={run_id}/"
silver_path = f"s3://{bucket_name}/silver/silver_{dataset}/"

print(f"Starting Silver ETL with SCD Type 2 for {dataset}")
print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")
print(f"Run ID: {run_id}")
print(f"Process date: {process_date}")
print(f"Bronze database: {bronze_database}")
print(f"Silver database: {silver_database}")

try:
    # Read from bronze
    print(f"Reading from: {bronze_path}")
    df_bronze_raw = spark.read.parquet(bronze_path)
    
    row_count_bronze = df_bronze_raw.count()
    print(f"Loaded {row_count_bronze} records from bronze")
    
    if row_count_bronze == 0:
        raise ValueError("No data in bronze layer")
    
    print(f"Bronze columns: {df_bronze_raw.columns}")
    
    # Business key preparation (Bronze already cleaned the data)
    print("Preparing business key for SCD Type 2...")
    df_bronze = df_bronze_raw
    
    # Use business key from config
    business_key = dataset_config['business_key']
    if business_key not in df_bronze.columns:
        raise ValueError(f"Required business key column '{business_key}' not found in bronze data")
    
    # Remove records with null business keys
    df_bronze_valid = df_bronze.filter(col(business_key).isNotNull())
    valid_count = df_bronze_valid.count()
    print(f"Records with valid business keys: {valid_count}")
    
    if valid_count != row_count_bronze:
        print(f"WARNING: Filtered out {row_count_bronze - valid_count} records with null business keys")
    
    # Deduplicate within this batch (keep latest by meta_ingest_timestamp)
    window_dedup = Window.partitionBy(business_key).orderBy(desc("meta_ingest_timestamp"))
    df_bronze_dedup = df_bronze_valid.withColumn("row_num", row_number().over(window_dedup)) \
                                     .filter(col("row_num") == 1) \
                                     .drop("row_num")
    
    dedup_count = df_bronze_dedup.count()
    print(f"After deduplication: {dedup_count} unique records")
    
    # Create hash of all non-key, non-metadata columns for change detection
    # Exclude metadata and SCD columns from hash
    metadata_cols = ['meta_ingest_timestamp', 'meta_run_id']
    scd_cols = ['record_effective_date', 'record_end_date', 'is_current', 'record_hash', 'change_type']
    exclude_cols = metadata_cols + scd_cols + [business_key]
    
    # Get columns to include in hash (all business data columns)
    hash_columns = [c for c in df_bronze_dedup.columns if c not in exclude_cols]
    print(f"Hash columns: {hash_columns}")
    
    # Create concatenated string for hashing (handle nulls)
    hash_expr = concat(*[
        coalesce(col(c).cast("string"), lit("NULL")) for c in hash_columns
    ])
    
    df_bronze_with_hash = df_bronze_dedup.withColumn("record_hash", md5(hash_expr))
    
    # Try to read existing silver data
    try:
        print(f"Attempting to read existing silver data from: {silver_path}")
        df_silver_existing = spark.read.parquet(silver_path)
        
        # Get only current records for comparison
        df_silver_current = df_silver_existing.filter(col("is_current") == True)
        existing_count = df_silver_current.count()
        print(f"Found {existing_count} current records in silver layer")
        
        # Perform SCD Type 2 merge
        print("Performing SCD Type 2 merge...")
        
        # Join new data with existing current records on business key
        df_comparison = df_bronze_with_hash.alias("new").join(
            df_silver_current.alias("existing"),
            col("new." + business_key) == col("existing." + business_key),
            "full_outer"
        )
        
        # Classify records into change types
        df_classified = df_comparison.withColumn(
            "change_type",
            when(col("existing." + business_key).isNull(), lit("INSERT"))  # New record
            .when(col("new." + business_key).isNull(), lit("DELETE"))      # Deleted record
            .when(col("new.record_hash") != col("existing.record_hash"), lit("UPDATE"))  # Changed record
            .otherwise(lit("NO_CHANGE"))  # Unchanged record
        )
        
        # Count changes
        change_counts = df_classified.groupBy("change_type").count().collect()
        for row in change_counts:
            print(f"{row['change_type']}: {row['count']} records")
        
        # Process INSERTs (completely new business keys)
        df_inserts = df_classified.filter(col("change_type") == "INSERT") \
                                  .select("new.*") \
                                  .withColumn("record_effective_date", lit(process_date).cast("date")) \
                                  .withColumn("record_end_date", lit("9999-12-31").cast("date")) \
                                  .withColumn("is_current", lit(True)) \
                                  .withColumn("change_type", lit("INSERT"))
        
        # Process UPDATEs (changed records)
        # First, close existing records
        df_updates_close = df_classified.filter(col("change_type") == "UPDATE") \
                                        .select("existing.*") \
                                        .withColumn("record_end_date", lit(process_date).cast("date")) \
                                        .withColumn("is_current", lit(False))
        
        # Then, insert new versions
        df_updates_new = df_classified.filter(col("change_type") == "UPDATE") \
                                      .select("new.*") \
                                      .withColumn("record_effective_date", lit(process_date).cast("date")) \
                                      .withColumn("record_end_date", lit("9999-12-31").cast("date")) \
                                      .withColumn("is_current", lit(True)) \
                                      .withColumn("change_type", lit("UPDATE"))
        
        # Process DELETEs (records no longer in source)
        df_deletes = df_classified.filter(col("change_type") == "DELETE") \
                                  .select("existing.*") \
                                  .withColumn("record_end_date", lit(process_date).cast("date")) \
                                  .withColumn("is_current", lit(False)) \
                                  .withColumn("change_type", lit("DELETE"))
        
        # Keep unchanged records as-is (already current in silver)
        df_unchanged = df_classified.filter(col("change_type") == "NO_CHANGE") \
                                    .select("existing.*")
        
        # Combine all results
        df_silver_final = df_inserts.unionByName(df_updates_close) \
                                   .unionByName(df_updates_new) \
                                   .unionByName(df_deletes) \
                                   .unionByName(df_unchanged)
        
    except Exception as e:
        print(f"No existing silver data found or error reading: {str(e)}")
        print("Treating all records as new INSERTs")
        
        # First load - all records are inserts
        df_silver_final = df_bronze_with_hash.withColumn("record_effective_date", lit(process_date).cast("date")) \
                                             .withColumn("record_end_date", lit("9999-12-31").cast("date")) \
                                             .withColumn("is_current", lit(True)) \
                                             .withColumn("change_type", lit("INSERT"))
    
    # Add silver processing metadata
    df_silver_final = df_silver_final.withColumn("meta_silver_timestamp", current_timestamp()) \
                                     .withColumn("meta_silver_run_id", lit(run_id))
    
    # Write to silver layer - overwrite mode to replace all data with complete history
    print(f"Writing silver data to: {silver_path}")
    final_count = df_silver_final.count()
    print(f"Final silver record count: {final_count}")
    
    # Partition by effective date year-month for query performance
    df_silver_final = df_silver_final.withColumn("effective_year_month", 
                                                 concat(
                                                     col("record_effective_date").substr(1, 4),
                                                     lit("-"),
                                                     col("record_effective_date").substr(6, 2)
                                                 ))
    
    df_silver_final.write \
                   .mode("overwrite") \
                   .option("compression", "zstd") \
                   .partitionBy("effective_year_month") \
                   .parquet(silver_path)
    
    print(f"Successfully processed {final_count} records with SCD Type 2")
    
    # Log summary statistics
    current_records = df_silver_final.filter(col("is_current") == True).count()
    historical_records = final_count - current_records
    print(f"Current records: {current_records}")
    print(f"Historical records: {historical_records}")

    # Register partitions with Glue catalog (following Bronze pattern)
    print("Registering partitions with Glue catalog...")
    glue_client = boto3.client('glue')
    
    # Collect all unique partition values from the data
    partition_values = df_silver_final.select("effective_year_month").distinct().rdd.map(lambda x: x[0]).collect()
    print(f"Found {len(partition_values)} partitions to register: {partition_values}")
    
    # Check if table exists before registering any partitions
    table_name = dataset  # Just 'nsde', consistent with Bronze naming
    table_exists = False
    
    try:
        table = glue_client.get_table(
            DatabaseName=silver_database,
            Name=table_name
        )['Table']
        table_exists = True
        print("Silver table found, proceeding with partition registration")
        
    except glue_client.exceptions.EntityNotFoundException:
        print("Silver table not found, triggering crawler to create it...")
        
        # Start the crawler to create the initial table
        crawler_name = f"{warehouse_config['warehouse_prefix']}-silver-{dataset}-crawler"
        
        try:
            # Check if crawler is already running
            crawler_response = glue_client.get_crawler(Name=crawler_name)
            if crawler_response['Crawler']['State'] == 'RUNNING':
                print("Silver crawler already running, waiting for completion...")
            else:
                print("Starting silver crawler...")
                glue_client.start_crawler(Name=crawler_name)
            
            # Wait for crawler to complete
            print("Waiting for silver crawler to complete table creation...")
            import time
            max_wait_time = 600  # 10 minutes max
            wait_interval = 30   # Check every 30 seconds
            total_waited = 0
            
            while total_waited < max_wait_time:
                time.sleep(wait_interval)
                total_waited += wait_interval
                
                crawler_status = glue_client.get_crawler(Name=crawler_name)
                state = crawler_status['Crawler']['State']
                print(f"Silver crawler state: {state} (waited {total_waited}s)")
                
                if state == 'READY':
                    print("Silver crawler completed successfully")
                    break
                elif state == 'STOPPING':
                    print("Silver crawler is stopping...")
                    continue
                elif state in ['RUNNING', 'STOPPING']:
                    continue
                else:
                    raise ValueError(f"Silver crawler failed with state: {state}")
            
            if total_waited >= max_wait_time:
                raise ValueError("Silver crawler timed out after 10 minutes")
            
            # Now get the newly created table
            table = glue_client.get_table(
                DatabaseName=silver_database,
                Name=table_name
            )['Table']
            table_exists = True
            print("Successfully retrieved silver table metadata after crawler completion")
            
        except Exception as crawler_error:
            print(f"Error with silver crawler: {str(crawler_error)}")
            # Try to continue anyway - maybe table was created by another process
            try:
                table = glue_client.get_table(
                    DatabaseName=silver_database,
                    Name=table_name
                )['Table']
                table_exists = True
                print("Silver table found despite crawler error, continuing...")
            except:
                print(f"Could not create or find silver table, skipping partition registration: {str(crawler_error)}")
                table_exists = False
    
    # Only register partitions if table exists
    if table_exists:
        for partition_value in partition_values:
            try:
                # Deep copy storage descriptor to avoid modifying nested structures  
                storage_desc = copy.deepcopy(table['StorageDescriptor'])
                partition_path = f"{silver_path}effective_year_month={partition_value}/"
                storage_desc['Location'] = partition_path
                
                # Check if partition already exists
                try:
                    glue_client.get_partition(
                        DatabaseName=silver_database,
                        TableName=table_name,
                        PartitionValues=[partition_value]
                    )
                    print(f"Partition {partition_value} already exists, skipping creation")
                except glue_client.exceptions.EntityNotFoundException:
                    # Create new partition
                    glue_client.create_partition(
                        DatabaseName=silver_database,
                        TableName=table_name,
                        PartitionInput={
                            'Values': [partition_value],
                            'StorageDescriptor': storage_desc
                        }
                    )
                    print(f"Successfully registered partition: {partition_value}")
                    
            except Exception as e:
                print(f"Warning: Could not register partition {partition_value}: {str(e)}")
                # Don't fail the job - partition registration is nice-to-have
    else:
        print("Table does not exist, skipping partition registration")

except Exception as e:
    print(f"Silver job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Silver job completed")