"""
Simple NSDE Silver Job - minimal dependencies
Reads bronze data and applies basic cleansing
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    lit, to_date, col, when, concat, lpad, split, trim, upper,
    current_timestamp, count
)
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bronze_path', 'run_id', 'bucket_name', 'dataset'])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Extract parameters
bronze_path = args['bronze_path']
run_id = args['run_id']
bucket_name = args['bucket_name']
dataset = args['dataset']
version = datetime.utcnow().strftime('%Y-%m-%d')

print(f"Starting Silver ETL for {dataset}")
print(f"Bronze path: {bronze_path}")
print(f"Run ID: {run_id}")

try:
    # Read from bronze
    print(f"Reading from: {bronze_path}")
    df_bronze = spark.read.parquet(bronze_path)
    
    row_count_bronze = df_bronze.count()
    print(f"Loaded {row_count_bronze} records from bronze")
    
    if row_count_bronze == 0:
        raise ValueError("No data in bronze layer")
    
    # Basic data cleansing for NSDE
    df_cleansed = df_bronze
    
    # Normalize NDC11 if column exists
    if "ndc11" in df_bronze.columns:
        df_cleansed = df_cleansed.withColumn(
            "ndc11_normalized",
            when(col("ndc11").isNotNull() & (col("ndc11") != ""),
                 lpad(concat(*[s for s in split(col("ndc11"), "-")]), 11, "0")
            ).otherwise(None)
        )
    
    # Clean text fields if they exist
    text_columns = ["packagedescription", "proprietaryname", "nonproprietaryname"]
    for text_col in text_columns:
        if text_col in df_bronze.columns:
            df_cleansed = df_cleansed.withColumn(
                text_col + "_clean",
                when(col(text_col).isNotNull(),
                     trim(upper(col(text_col)))
                ).otherwise(None)
            )
    
    # Simple deduplication (keep first occurrence)
    df_deduped = df_cleansed.dropDuplicates()
    row_count_silver = df_deduped.count()
    
    print(f"After deduplication: {row_count_silver} records")
    
    # Add silver metadata
    df_silver = df_deduped.withColumn("meta_silver_timestamp", current_timestamp()) \
                          .withColumn("version", lit(version)) \
                          .withColumn("version_date", to_date(lit(version), "yyyy-MM-dd"))
    
    # Write to silver layer
    silver_path = f"s3://{bucket_name}/silver/{dataset}/data/"
    print(f"Writing to: {silver_path}")
    
    df_silver.write \
             .mode("append") \
             .option("compression", "zstd") \
             .partitionBy("version", "version_date") \
             .parquet(silver_path)
    
    print(f"Successfully processed {row_count_silver} records")

except Exception as e:
    print(f"Silver job error: {str(e)}")
    raise

finally:
    job.commit()
    print("Silver job completed")