"""
NSDE Silver ETL - Full Cleansing, Normalization, and Data Quality
According to Medallion architecture:
- Silver: Full cleansing, normalization (e.g., NDC11 to 11-digit, date parsing, text trimming),
  DQ checks/filtering/tripwires (fail on critical issues like null NDCs), deduping
- Read from Bronze, output as Parquet partitioned by version and version_date
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    lit, to_date, hash, col, when, concat, 
    lpad, split, trim, upper, isnan, isnull,
    current_timestamp, count, countDistinct
)
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import json

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'database_name', 'silver_prefix'])

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
silver_prefix = args.get('silver_prefix', 'silver/nsde/')
date_str = datetime.utcnow().strftime('%Y-%m-%d')

print(f"Starting NSDE Silver ETL for date: {date_str}")
print(f"Data lake bucket: {bucket}")
print(f"Database: {database}")

try:
    # Step 0: Early-exit check - verify Bronze manifest exists for today
    # This provides extra safety if Silver is manually triggered without Bronze completion
    bronze_manifest_key = f'bronze/nsde/metadata/summary_version={date_str}/_SUCCESS.json'
    
    try:
        response = s3.get_object(Bucket=bucket, Key=bronze_manifest_key)
        bronze_manifest = json.loads(response['Body'].read())
        bronze_sha256 = bronze_manifest.get('sha256')
        print(f"Bronze manifest found for {date_str} with SHA256: {bronze_sha256}")
        print(f"Bronze processed {bronze_manifest.get('row_count_raw', 'unknown')} records")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"EARLY EXIT: Bronze manifest not found for {date_str}")
            print(f"Expected: s3://{bucket}/{bronze_manifest_key}")
            print("Silver requires Bronze to complete first. Exiting gracefully.")
            job.commit()
            sys.exit(0)
        else:
            print(f"EARLY EXIT: AWS error reading Bronze manifest: {str(e)}")
            print("Silver requires valid Bronze completion. Exiting gracefully.")
            job.commit()
            sys.exit(0)
    except Exception as e:
        print(f"EARLY EXIT: Error reading Bronze manifest: {str(e)}")
        print("Silver requires valid Bronze completion. Exiting gracefully.")
        job.commit()
        sys.exit(0)
    
    # Step 1: Read from Bronze layer
    bronze_data_path = f"s3://{bucket}/bronze/nsde/data/"
    print(f"Reading Bronze data from: {bronze_data_path}")
    
    # Read the most recent Bronze data (latest version_date partition)
    df = spark.read.parquet(bronze_data_path)
    
    # Cache the dataset for multiple transformations (small dataset, speeds transforms)
    df.cache()
    
    # Filter to most recent version_date if multiple exist
    latest_version = df.select("version_date").distinct().orderBy(col("version_date").desc()).first()[0]
    df = df.filter(col("version_date") == latest_version)
    
    initial_count = df.count()
    print(f"Loaded {initial_count} records from Bronze layer (version_date: {latest_version})")
    
    # Step 2: Data cleansing and normalization with DQ tracking
    date_parse_errors = 0
    malformed_ndc_count = 0
    
    # Normalize NDC codes to 11-digit format without dashes
    # The Bronze data has "Item Code" and "NDC11" columns
    df = df.withColumn("item_code_parts", split(col("Item Code"), "-"))
    df = df.withColumn("normalized_ndc11",
        when(col("NDC11").isNotNull() & (trim(col("NDC11")) != ""), 
             # NDC11 should already be 11 digits without dashes
             trim(col("NDC11")))
        .otherwise(
            when(col("item_code_parts").isNotNull() & (col("item_code_parts")[0].isNotNull()),
                # Create 11-digit NDC without dashes (5-4-2 becomes 54200000000)
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
            
            # Count date parse errors before dropping the error column
            error_count = df.filter(col(f"{date_col}_parse_error") == 1).count()
            date_parse_errors += error_count
            
            # Try multiple date formats for flexibility
            df = df.withColumn(date_col, 
                when(col("`" + date_col + "`").isNotNull() & (trim(col("`" + date_col + "`")) != ""),
                     # Try YYYYMMDD first (most common FDA format)
                     when(to_date(col("`" + date_col + "`"), "yyyyMMdd").isNotNull(),
                          to_date(col("`" + date_col + "`"), "yyyyMMdd")
                     ).otherwise(
                         # Try yyyy-MM-dd as fallback
                         when(to_date(col("`" + date_col + "`"), "yyyy-MM-dd").isNotNull(),
                              to_date(col("`" + date_col + "`"), "yyyy-MM-dd")
                         ).otherwise(lit(None))
                     )
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
        print(f"Sample malformed NDCs (will be filtered out):")
        malformed_samples = malformed_ndc_df.select("item_code", "ndc11_original", "normalized_ndc11").limit(10)
        for row in malformed_samples.collect():
            print(f"  Item Code: '{row.item_code}', Original NDC11: '{row.ndc11_original}', Normalized: '{row.normalized_ndc11}'")
    
    # Filter out malformed NDCs - we don't want junk data in Silver
    print(f"Filtering out {malformed_ndc_count} malformed NDCs from dataset")
    df = df.filter(
        col("normalized_ndc11").isNull() | 
        col("normalized_ndc11").rlike("^[0-9]{11}$")
    )
    
    # Clean and standardize text fields
    text_fields = ["proprietary_name", "dosage_form", "product_type", "billing_unit", 
                   "marketing_category", "application_number_or_monograph_id"]
    for field in text_fields:
        if field in df.columns:
            df = df.withColumn(field, trim(col(field)))
    
    # Add Silver layer metadata columns
    df = df.withColumn("meta_change_hash", hash(*[c for c in df.columns if not c.startswith("meta_")])) \
           .withColumn("version", lit(date_str)) \
           .withColumn("meta_silver_processing_date", current_timestamp()) \
           .withColumn("meta_record_count_bronze", lit(initial_count))
    
    # Keep version_date from Bronze but ensure it's a date type
    df = df.withColumn("version_date", to_date(col("version_date"), "yyyy-MM-dd"))
    
    # Step 3: Data quality checks (DQ Tripwires)
    print("Performing data quality checks...")
    
    # Check for null NDCs (FAIL if > 0)
    null_ndc_count = df.filter(col("normalized_ndc11").isNull() | (col("normalized_ndc11") == "")).count()
    print(f"Null NDC count: {null_ndc_count}")
    
    # Check for null values in business critical fields
    null_proprietary_name_count = df.filter(col("proprietary_name").isNull() | (trim(col("proprietary_name")) == "")).count()
    null_dosage_form_count = df.filter(col("dosage_form").isNull() | (trim(col("dosage_form")) == "")).count()
    print(f"Null proprietary name count: {null_proprietary_name_count}")
    print(f"Null dosage form count: {null_dosage_form_count}")
    
    # Check for duplicate NDCs (LOG only, don't fail)
    duplicate_ndc_df = df.groupBy("normalized_ndc11") \
                        .count() \
                        .filter(col("count") > 1)
    duplicate_count = duplicate_ndc_df.count()
    print(f"Duplicate NDC count: {duplicate_count}")
    
    # Log sample duplicate NDCs for debugging
    if duplicate_count > 0:
        print(f"Sample duplicate NDCs (keeping all instances):")
        duplicate_samples = duplicate_ndc_df.orderBy(col("count").desc()).limit(5)
        for row in duplicate_samples.collect():
            print(f"  NDC: '{row.normalized_ndc11}', Count: {row['count']}")
    
    # Count unique NDCs
    unique_ndc_count = df.select("normalized_ndc11").distinct().count()
    print(f"Unique NDC count: {unique_ndc_count}")
    
    # Check for malformed NDCs (already filtered)
    print(f"Malformed NDC count (filtered): {malformed_ndc_count}")
    
    # Check for date parse errors (FAIL if > 0)
    print(f"Date parse error count: {date_parse_errors}")
    
    # DQ Tripwires - fail the job on critical issues
    if null_ndc_count > 0:
        raise ValueError(f"DQ FAILURE: Found {null_ndc_count} records with null NDC codes")
    
    if date_parse_errors > 0:
        raise ValueError(f"DQ FAILURE: Found {date_parse_errors} date parse errors")
    
    print("Data quality checks PASSED")
    
    # Step 4: Write to Silver layer as Parquet with ZSTD compression
    silver_data_path = f"s3://{bucket}/silver/nsde/data/"
    final_count = df.count()
    print(f"Writing {final_count} records to silver layer: {silver_data_path}")
    
    # Write with version and version_date partitioning, ZSTD compression
    # Use append mode to preserve historical partitions if re-run
    (df.write
      .mode("append")
      .option("compression","zstd")
      .partitionBy("version","version_date")
      .parquet(silver_data_path))
    
    print(f"Successfully wrote data to silver layer")
    
    # Step 5: Create metadata table for ETL observability
    processed_at_utc = datetime.utcnow().isoformat()
    
    # Create metadata as a proper DataFrame (unpartitioned table)
    metadata_data = [{
        "dataset": "nsde",
        "layer": "silver",
        "version": date_str,
        "version_date": latest_version,
        "row_count_bronze": initial_count,
        "row_count_silver": final_count,
        "unique_ndc_count": unique_ndc_count,
        "null_ndc_count": null_ndc_count,
        "duplicate_ndc_count": duplicate_count,
        "malformed_ndc_count": malformed_ndc_count,
        "date_parse_errors": date_parse_errors,
        "bronze_sha256": bronze_sha256,
        "processed_at_utc": processed_at_utc,
        "job_name": args['JOB_NAME']
    }]
    
    metadata_df = spark.createDataFrame(metadata_data)
    
    # Write metadata to unpartitioned silver metadata table
    metadata_path = f"s3://{bucket}/silver/nsde/metadata/data/"
    metadata_df.write.mode("append") \
                     .option("compression", "zstd") \
                     .parquet(metadata_path)
    
    print("\nSilver ETL Summary:")
    for key, value in metadata_data[0].items():
        print(f"  {key}: {value}")
    
    print(f"\nMetadata written to: {metadata_path}")
    print(f"Data written to: {silver_data_path}")
    
except Exception as e:
    print(f"Silver ETL Error: {str(e)}")
    raise
finally:
    # Commit the job
    job.commit()
    print("NSDE Silver ETL job completed")