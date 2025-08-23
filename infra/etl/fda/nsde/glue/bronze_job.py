"""
Simple NSDE Bronze Job - minimal dependencies
Reads raw CSV from S3 and converts to Parquet
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp
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
    
    # Add basic metadata
    df_bronze = df.withColumn("meta_ingest_timestamp", current_timestamp()) \
                  .withColumn("meta_run_id", lit(run_id)) \
                  .withColumn("run", lit(run_id))  # For partitioning
    
    # Write to bronze layer
    print(f"Writing to: {bronze_path}")
    
    df_bronze.write \
             .mode("append") \
             .option("compression", "zstd") \
             .partitionBy("run") \
             .parquet(bronze_path)
    
    print(f"Successfully processed {row_count} records")

except Exception as e:
    print(f"Bronze job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Bronze job completed")