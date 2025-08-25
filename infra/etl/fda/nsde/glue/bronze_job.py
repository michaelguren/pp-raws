"""
Simple NSDE Bronze Job - minimal dependencies
Reads raw CSV from S3 and converts to Parquet
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp, col, regexp_replace, when, to_date
from pyspark.sql.types import IntegerType, DateType
from datetime import datetime

# Get job parameters - bronze_path now points to bronze bucket
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_path', 'run_id', 'dataset', 'bronze_path'])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Extract parameters
raw_path = args['raw_path']
run_id = args['run_id']
dataset = args['dataset']
bronze_path = args['bronze_path']

print(f"Starting Bronze ETL for {dataset}")
print(f"Raw path: {raw_path}")
print(f"Run ID: {run_id}")

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
    
    # Fix date columns - convert YYYYMMDD strings to proper dates
    date_columns = [col for col in df.columns if 'date' in col]
    for date_col in date_columns:
        df = df.withColumn(
            date_col, 
            when(col(date_col).isNull() | (col(date_col) == ""), None)
            .otherwise(to_date(col(date_col).cast("string"), "yyyyMMdd"))
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

except Exception as e:
    print(f"Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Bronze job completed")