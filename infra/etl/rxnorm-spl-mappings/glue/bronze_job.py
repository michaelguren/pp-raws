import sys
import os
import tempfile
import zipfile
import ftplib
import boto3
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
    'bucket_name'
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

# Generate run_id (human-readable timestamp)
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# FTP and file configuration
ftp_host = "public.nlm.nih.gov"
ftp_path = "/nlmdata/.dailymed/rxnorm_mappings.zip"
zip_filename = "rxnorm_mappings.zip"
csv_filename = "rxnorm_mappings.txt"

# Column mappings from Rails model
column_mappings = {
    "SETID": "set_id",
    "SPL_VERSION": "spl_version",
    "RXCUI": "rxcui",
    "RXSTRING": "rx_string",
    "RXTTY": "rx_tty"
}

def download_ftp_file():
    """Download the zip file from NLM FTP server"""
    print(f"Connecting to FTP server: {ftp_host}")

    temp_dir = tempfile.mkdtemp()
    local_zip_path = os.path.join(temp_dir, zip_filename)

    try:
        # Connect to FTP server
        ftp = ftplib.FTP(ftp_host)
        ftp.login()  # Anonymous login

        print(f"Downloading {ftp_path} to {local_zip_path}")
        with open(local_zip_path, 'wb') as f:
            ftp.retrbinary(f'RETR {ftp_path}', f.write)

        ftp.quit()
        print(f"Successfully downloaded {zip_filename}")
        return local_zip_path, temp_dir

    except Exception as e:
        print(f"FTP download failed: {str(e)}")
        raise

def extract_and_upload_raw(local_zip_path, temp_dir):
    """Extract CSV from zip and upload both raw files to S3"""
    print(f"Extracting {csv_filename} from {local_zip_path}")

    raw_csv_path = None

    try:
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            # Extract the specific file we need
            if csv_filename in zip_ref.namelist():
                zip_ref.extract(csv_filename, temp_dir)
                raw_csv_path = os.path.join(temp_dir, csv_filename)
                print(f"Extracted {csv_filename}")
            else:
                available_files = zip_ref.namelist()
                print(f"Available files in zip: {available_files}")
                raise FileNotFoundError(f"{csv_filename} not found in zip archive")

        # Upload raw files to S3
        s3_client = boto3.client('s3')
        zip_key = f"raw/{dataset}/run_id={run_id}/{zip_filename}"
        csv_key = f"raw/{dataset}/run_id={run_id}/{csv_filename}"

        # Upload original zip
        with open(local_zip_path, 'rb') as f:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=zip_key,
                Body=f.read()
            )

        # Upload extracted CSV for lineage
        with open(raw_csv_path, 'rb') as f:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=csv_key,
                Body=f.read()
            )

        print(f"Uploaded raw files to S3: {zip_key}, {csv_key}")
        return raw_csv_path

    except Exception as e:
        print(f"Error extracting/uploading raw files: {str(e)}")
        raise

def process_csv_to_bronze():
    """Read pipe-delimited CSV from S3 and transform to Bronze parquet"""
    print(f"Processing CSV from S3 raw layer")

    try:
        # Read CSV from S3 raw path (uploaded by extract_and_upload_raw)
        raw_s3_path = f"{raw_base_path}run_id={run_id}/"
        csv_s3_path = f"{raw_s3_path}*.txt"  # Match the txt file extension

        print(f"Reading CSV from: {csv_s3_path}")

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
            .csv(csv_s3_path)

        row_count = df.count()
        print(f"Read {row_count} records from CSV")

        if row_count == 0:
            raise ValueError("No data found in CSV file")

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
        bronze_table_name = f"bronze_{dataset.replace('-', '_')}"
        print(f"Bronze table will be available as: {bronze_database}.{bronze_table_name}")
        return row_count

    except Exception as e:
        print(f"Error processing CSV to Bronze: {str(e)}")
        raise

def cleanup_temp_files(temp_dir):
    """Clean up temporary files"""
    try:
        import shutil
        shutil.rmtree(temp_dir)
        print("Cleaned up temporary files")
    except Exception as e:
        print(f"Warning: Could not clean up temp files: {str(e)}")

# Main execution
def main():
    print(f"Starting RxNORM SPL Mappings Bronze ETL job")
    print(f"Dataset: {dataset}")
    print(f"Run ID: {run_id}")

    temp_dir = None
    try:
        # Download and extract
        local_zip_path, temp_dir = download_ftp_file()
        raw_csv_path = extract_and_upload_raw(local_zip_path, temp_dir)

        # Process to Bronze (reads from S3)
        record_count = process_csv_to_bronze()

        print(f"✅ ETL job completed successfully")
        print(f"   Records processed: {record_count}")
        print(f"   Raw data: {raw_base_path}run_id={run_id}/")
        print(f"   Bronze data: {bronze_base_path}")

    except Exception as e:
        print(f"❌ ETL job failed: {str(e)}")
        raise

    finally:
        if temp_dir:
            cleanup_temp_files(temp_dir)

if __name__ == "__main__":
    main()
    job.commit()