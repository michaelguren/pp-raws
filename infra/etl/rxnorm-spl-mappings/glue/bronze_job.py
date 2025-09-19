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
    """Memory-efficient HTTPS download and extraction of ZIP file containing pipe-delimited text"""
    print(f"Downloading ZIP from: {source_url}")

    import tempfile
    import shutil
    import zipfile
    s3_client = boto3.client('s3')
    zip_key = f"raw/{dataset}/run_id={run_id}/rxnorm_mappings.zip"
    txt_key = f"raw/{dataset}/run_id={run_id}/{txt_filename}"

    from urllib.error import URLError, HTTPError

    max_retries = 3
    content_length = None

    for attempt in range(max_retries):
        try:
            print(f"Download attempt {attempt + 1}/{max_retries}")

            # Stream download to temporary file
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=True) as tmp_file:
                with urllib.request.urlopen(source_url, timeout=300) as response:
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        print(f"ZIP file size: {int(content_length):,} bytes")

                    print(f"Content-Type: {response.headers.get('Content-Type', 'Unknown')}")

                    # Stream ZIP to temp file
                    shutil.copyfileobj(response, tmp_file)
                    tmp_file.flush()

                print(f"Downloaded ZIP to temporary file")

                # Upload original ZIP to S3 for lineage
                tmp_file.seek(0)
                s3_client.upload_file(tmp_file.name, bucket_name, zip_key)
                print(f"Uploaded original ZIP to: s3://{bucket_name}/{zip_key}")

                # Extract rxnorm_mappings.txt from ZIP and upload to S3
                tmp_file.seek(0)
                with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
                    # List all files in ZIP
                    all_files = zip_ref.namelist()
                    print(f"Files in ZIP: {all_files}")

                    # Look for rxnorm_mappings.txt specifically
                    target_file = "rxnorm_mappings.txt"
                    if target_file not in all_files:
                        raise Exception(f"Expected file '{target_file}' not found in ZIP. Available files: {all_files}")

                    print(f"Extracting: {target_file}")

                    with zip_ref.open(target_file) as extracted_file:
                        # Upload extracted text file to S3
                        s3_client.upload_fileobj(extracted_file, bucket_name, txt_key)
                        print(f"Uploaded extracted text file to: s3://{bucket_name}/{txt_key}")

            print(f"Successfully downloaded ZIP and extracted text file")
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

        # Read pipe-delimited file from S3 with proper encoding
        # Try different encodings in order of likelihood
        encodings_to_try = ["UTF-8", "ISO-8859-1", "Windows-1252"]
        df = None
        successful_encoding = None

        for encoding in encodings_to_try:
            try:
                print(f"Attempting to read with {encoding} encoding...")
                df = spark.read \
                    .option("header", "true") \
                    .option("delimiter", "|") \
                    .option("encoding", encoding) \
                    .option("charset", encoding) \
                    .schema(schema) \
                    .csv(txt_s3_path)

                # Test read by checking first few rows for garbled characters
                sample_data = df.limit(5).collect()
                if sample_data and len(sample_data) > 0:
                    # Check if we have readable text (no excessive question marks or weird chars)
                    first_row_str = str(sample_data[0])
                    if "��" not in first_row_str and not any(ord(c) > 1000 for c in first_row_str if isinstance(c, str)):
                        successful_encoding = encoding
                        print(f"Successfully read with {encoding} encoding")
                        break
                    else:
                        print(f"{encoding} encoding produced garbled characters")
                        df = None

            except Exception as e:
                print(f"Failed with {encoding} encoding: {str(e)}")
                df = None
                continue

        if df is None:
            raise Exception(f"Could not read file with any supported encoding: {encodings_to_try}")

        print(f"Using {successful_encoding} encoding for final processing")

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