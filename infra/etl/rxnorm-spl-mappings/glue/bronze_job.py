import sys
import tempfile
import zipfile
import urllib.request
import shutil
import boto3
import time
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType

# Get job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'dataset',
    'bronze_database',
    'raw_base_path',
    'bronze_base_path',
    'compression_codec',
    'bucket_name',
    'source_url'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])

# Configuration
dataset = args['dataset']
bronze_database = args['bronze_database']
raw_base_path = args['raw_base_path']
bronze_base_path = args['bronze_base_path']
compression_codec = args['compression_codec']
bucket_name = args['bucket_name']
source_url = args['source_url']

# Generate run_id (human-readable timestamp)
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# File configuration
txt_filename = "rxnorm_mappings.txt"

# Column mappings from Rails model
column_mappings = {
    "SETID": "set_id",
    "SPL_VERSION": "spl_version",
    "RXCUI": "rxcui",
    "RXSTRING": "rx_string",
    "RXTTY": "rx_tty"
}

def download_and_process_file():
    """Memory-efficient HTTPS download of pipe-delimited text file"""
    print(f"Downloading from: {source_url}")

    s3_client = boto3.client('s3')
    txt_key = f"raw/{dataset}/run_id={run_id}/{txt_filename}"

    from urllib.error import URLError, HTTPError

    max_retries = 3
    content_length = None

    for attempt in range(max_retries):
        try:
            print(f"Download attempt {attempt + 1}/{max_retries}")

            # Stream download directly to S3
            with urllib.request.urlopen(source_url, timeout=300) as response:
                content_length = response.headers.get('Content-Length')
                if content_length:
                    print(f"File size: {int(content_length):,} bytes")

                print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")

                # Stream pipe-delimited text file directly to S3
                s3_client.upload_fileobj(
                    response,
                    bucket_name,
                    txt_key
                )
                print(f"Streamed text file to: s3://{bucket_name}/{txt_key}")

            print(f"Successfully downloaded and uploaded pipe-delimited file")
            return  # Success, exit retry loop

        except (URLError, HTTPError) as e:
            print(f"Network error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                raise

def process_csv_to_bronze():
    """Read pipe-delimited text file from S3 and transform to Bronze parquet"""
    print(f"Processing pipe-delimited file from S3 raw layer")

    try:
        # Read text file from S3 raw path
        raw_s3_path = f"{raw_base_path}run_id={run_id}/"
        txt_s3_path = f"{raw_s3_path}{txt_filename}"

        print(f"Reading pipe-delimited file from: {txt_s3_path}")

        # Define schema based on Rails model
        schema = StructType([
            StructField("SETID", StringType(), True),
            StructField("SPL_VERSION", StringType(), True),
            StructField("RXCUI", StringType(), True),
            StructField("RXSTRING", StringType(), True),
            StructField("RXTTY", StringType(), True)
        ])

        # Read pipe-delimited file from S3
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", "|") \
            .option("encoding", "UTF-8") \
            .schema(schema) \
            .csv(txt_s3_path)

        row_count = df.count()
        print(f"Read {row_count} records from pipe-delimited file")

        if row_count == 0:
            raise ValueError("No data found in pipe-delimited file")

        # Apply column mappings and add metadata
        for old_col, new_col in column_mappings.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)

        # Add metadata column
        df = df.withColumn("meta_run_id", lit(run_id))

        print("Applied column mappings and added metadata")

        # Write to Bronze layer (kill-and-fill approach)
        bronze_table_path = f"{bronze_base_path}"

        print(f"Writing to Bronze layer: {bronze_table_path}")

        df.write \
            .mode("overwrite") \
            .option("compression", compression_codec) \
            .parquet(bronze_table_path)

        print(f"Successfully wrote {row_count} records to Bronze layer")

        # Note: Crawler will update the Glue catalog table after job completion
        bronze_table_name = f"{dataset.replace('-', '_')}"
        print(f"Bronze table will be available as: {bronze_database}.{bronze_table_name}")
        return row_count

    except Exception as e:
        print(f"Error processing pipe-delimited file to Bronze: {str(e)}")
        raise

# Main execution
def main():
    print(f"Starting RxNORM SPL Mappings Bronze ETL job")
    print(f"Dataset: {dataset}")
    print(f"Run ID: {run_id}")

    try:
        # Download, extract, and upload with streaming
        download_and_process_file()

        # Process to Bronze (reads from S3)
        record_count = process_csv_to_bronze()

        print(f"✅ ETL job completed successfully")
        print(f"   Records processed: {record_count}")
        print(f"   Raw data: {raw_base_path}run_id={run_id}/{txt_filename}")
        print(f"   Bronze data: {bronze_base_path}")

    except Exception as e:
        print(f"❌ ETL job failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    job.commit()