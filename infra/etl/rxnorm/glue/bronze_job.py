"""
Complete RxNORM ETL Job - Download, Transform, and Store RRF Files
Downloads RxNORM data using UMLS authentication, saves raw files for lineage,
transforms to Bronze layer with multiple RRF tables
"""
import sys
import boto3
import urllib.request
import zipfile
import requests
import json
from io import BytesIO
from datetime import datetime, date, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, when, to_date, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

# Get job parameters
required_args = ['JOB_NAME', 'bucket_name', 'dataset', 'bronze_database',
                 'raw_path', 'bronze_path', 'compression_codec',
                 'umls_api_secret', 'tables_to_process']
optional_args = ['release_date']

args = getResolvedOptions(sys.argv, required_args)

# Handle optional release_date parameter
if '--release_date' in sys.argv:
    optional = getResolvedOptions(sys.argv, optional_args)
    release_date = optional['release_date']
    print(f"Using provided release date: {release_date}")
else:
    # Auto-detect latest release date from RxNorm files page
    print("No release date provided, fetching latest from RxNorm files page...")
    try:
        release_date = get_latest_rxnorm_release_date()
        print(f"Auto-detected latest release date: {release_date}")
    except Exception as e:
        print(f"Failed to auto-detect release date: {str(e)}")
        # Fallback to first Monday of current month
        today = date.today()
        first_day = today.replace(day=1)
        days_ahead = 0 - first_day.weekday()  # Monday is 0
        if days_ahead <= 0:
            days_ahead += 7
        first_monday = first_day + timedelta(days_ahead)
        release_date = first_monday.strftime("%m%d%Y")
        print(f"Fallback to first Monday: {release_date}")

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])

# Generate human-readable runtime run_id
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Get configuration from job arguments
bucket_name = args['bucket_name']
dataset = args['dataset']
bronze_database = args['bronze_database']
umls_api_secret = args['umls_api_secret']
tables_to_process = args['tables_to_process'].split(',')

# Build paths
raw_path_fragment = args['raw_path']
bronze_path_fragment = args['bronze_path']
raw_s3_path = f"s3://{bucket_name}/{raw_path_fragment}run_id={run_id}/"

# Build download URL with date
base_url = "https://download.nlm.nih.gov/umls/kss/rxnorm"
download_url = f"{base_url}/RxNorm_full_{release_date}.zip"

print(f"Starting Complete RxNORM ETL for {dataset}")
print(f"Release date: {release_date}")
print(f"Download URL: {download_url}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze base path: s3://{bucket_name}/{bronze_path_fragment}")
print(f"Run ID: {run_id}")
print(f"Tables to process: {tables_to_process}")
print(f"Mode: overwrite (kill-and-fill)")

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {str(e)}")
        raise

def get_latest_rxnorm_release_date():
    """Scrape latest RxNorm release date from NLM files page"""
    import re

    files_page_url = "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html"
    print(f"Fetching latest release date from: {files_page_url}")

    try:
        # Fetch the page
        req = urllib.request.Request(files_page_url)
        req.add_header('User-Agent', 'AWS-Glue-ETL/1.0')

        with urllib.request.urlopen(req, timeout=30) as response:
            html_content = response.read().decode('utf-8')

        # Look for RxNorm_full_MMDDYYYY.zip pattern in the HTML
        # Pattern matches: RxNorm_full_01022024.zip, RxNorm_full_12042023.zip, etc.
        pattern = r'RxNorm_full_(\d{8})\.zip'
        matches = re.findall(pattern, html_content)

        if not matches:
            raise Exception("No RxNorm release dates found on files page")

        # Convert dates to datetime objects for sorting
        date_objects = []
        for date_str in matches:
            try:
                # Parse MMDDYYYY format
                date_obj = datetime.strptime(date_str, "%m%d%Y")
                date_objects.append((date_obj, date_str))
            except ValueError:
                print(f"Invalid date format found: {date_str}")
                continue

        if not date_objects:
            raise Exception("No valid release dates found")

        # Sort by date and get the latest
        date_objects.sort(key=lambda x: x[0], reverse=True)
        latest_date_str = date_objects[0][1]

        print(f"Found {len(matches)} release dates, latest: {latest_date_str}")
        return latest_date_str

    except Exception as e:
        print(f"Error fetching latest release date: {str(e)}")
        raise

def authenticate_umls(api_key):
    """Authenticate with UMLS and get service ticket"""
    print("Authenticating with UMLS...")

    # Step 1: Get TGT (Ticket Granting Ticket)
    tgt_url = "https://utslogin.nlm.nih.gov/cas/v1/api-key"
    tgt_response = requests.post(tgt_url, data={"apikey": api_key}, timeout=30)

    if tgt_response.status_code != 201:
        raise Exception(f"Failed to get TGT: {tgt_response.status_code} - {tgt_response.text}")

    # Extract TGT from response
    tgt_text = tgt_response.text
    start_index = tgt_text.find('TGT-')
    if start_index == -1:
        raise Exception(f"TGT not found in response: {tgt_text}")

    end_index = tgt_text.find('"', start_index)
    if end_index == -1:
        end_index = tgt_text.find('\n', start_index)

    tgt = tgt_text[start_index:end_index].strip()
    print(f"Got TGT: {tgt[:20]}...")

    # Step 2: Get Service Ticket
    st_url = f"https://utslogin.nlm.nih.gov/cas/v1/tickets/{tgt}"
    st_response = requests.post(st_url, data={"service": download_url}, timeout=30)

    if st_response.status_code != 200:
        raise Exception(f"Failed to get Service Ticket: {st_response.status_code} - {st_response.text}")

    service_ticket = st_response.text.strip()
    print(f"Got Service Ticket: {service_ticket[:20]}...")

    return service_ticket

def apply_column_mapping(df, column_mapping):
    """Apply column name mapping to DataFrame"""
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    return df

def clean_rrf_data(df):
    """Clean RRF data - trim whitespace and handle nulls"""
    for col_name in df.columns:
        df = df.withColumn(col_name,
                          when(trim(col(col_name)) == "", None)
                          .otherwise(trim(col(col_name))))
    return df

# Column mappings for each RRF table
COLUMN_MAPPINGS = {
    'RXNCONSO': {
        "RXCUI": "rxcui",
        "LAT": "language",
        "TS": "term_status",
        "LUI": "lui",
        "STT": "string_type",
        "SUI": "sui",
        "ISPREF": "is_preferred",
        "RXAUI": "rxaui",
        "SAUI": "source_aui",
        "SCUI": "source_cui",
        "SDUI": "source_dui",
        "SAB": "source",
        "TTY": "tty",
        "CODE": "code",
        "STR": "name",
        "SRL": "source_restriction",
        "SUPPRESS": "suppress",
        "CVF": "cvf"
    },
    'RXNSAT': {
        "RXCUI": "rxcui",
        "LUI": "lui",
        "SUI": "sui",
        "RXAUI": "rxaui",
        "STYPE": "stype",
        "CODE": "code",
        "ATUI": "atui",
        "SATUI": "satui",
        "ATN": "attribute_name",
        "SAB": "source",
        "ATV": "attribute_value",
        "SUPPRESS": "suppress",
        "CVF": "cvf"
    },
    'RXNREL': {
        "RXCUI1": "rxcui1",
        "RXAUI1": "rxaui1",
        "STYPE1": "stype1",
        "REL": "relationship",
        "RXCUI2": "rxcui2",
        "RXAUI2": "rxaui2",
        "STYPE2": "stype2",
        "RELA": "rela",
        "RUI": "rui",
        "SRUI": "srui",
        "SAB": "source",
        "SL": "source_label",
        "RG": "rg",
        "DIR": "dir",
        "SUPPRESS": "suppress",
        "CVF": "cvf"
    },
    'RXNSTY': {
        "RXCUI": "rxcui",
        "TUI": "tui",
        "STN": "stn",
        "STY": "sty",
        "ATUI": "atui",
        "CVF": "cvf"
    },
    'RXNCUI': {
        "RXCUI1": "rxcui1",
        "RXCUI2": "rxcui2"
    },
    'RXNATOMARCHIVE': {
        "RXAUI": "rxaui",
        "AUI": "aui",
        "STR": "str",
        "ARCHIVE_TIMESTAMP": "archive_timestamp",
        "CREATED_TIMESTAMP": "created_timestamp",
        "UPDATED_TIMESTAMP": "updated_timestamp",
        "CODE": "code",
        "IS_BRAND": "is_brand",
        "LAT": "lat",
        "LAST_RELEASED": "last_released",
        "SAUI": "saui",
        "VSAB": "vsab",
        "RXCUI": "rxcui",
        "SAB": "sab",
        "TTY": "tty",
        "MERGED_TO_RXCUI": "merged_to_rxcui"
    }
}

try:
    # Step 1: Authenticate and download
    print("Getting UMLS API key from Secrets Manager...")
    api_key = get_secret(umls_api_secret)

    print("Authenticating with UMLS...")
    service_ticket = authenticate_umls(api_key)

    # Download with authentication using requests (to handle cookies and redirects like Rails)
    download_url_with_ticket = f"{download_url}?ticket={service_ticket}"
    print(f"Downloading from: {download_url_with_ticket}")

    # Use requests to handle cookies and redirects like the Rails code
    session = requests.Session()
    session.headers.update({'User-Agent': 'AWS-Glue-ETL/1.0'})

    # Memory-efficient download using temporary file and streaming
    print("Starting streaming download with cookies and redirect following...")

    import tempfile
    import shutil
    from urllib.error import URLError, HTTPError
    import time

    s3_client = boto3.client('s3')
    zip_key = f"raw/{dataset}/run_id={run_id}/RxNorm_full_{release_date}.zip"

    max_retries = 3
    content_length = None

    for attempt in range(max_retries):
        try:
            print(f"Download attempt {attempt + 1}/{max_retries}")

            # Stream download with requests to temporary file
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=True) as tmp_file:
                print("Starting authenticated streaming download...")

                with session.get(download_url_with_ticket, timeout=600, allow_redirects=True, stream=True) as download_response:
                    print(f"Download response status: {download_response.status_code}")
                    print(f"Final URL after redirects: {download_response.url}")
                    print(f"Content-Type: {download_response.headers.get('Content-Type', 'Unknown')}")

                    content_length = download_response.headers.get('Content-Length')
                    if content_length:
                        print(f"Content-Length: {int(content_length):,} bytes")

                    if download_response.status_code != 200:
                        print(f"Download failed with status {download_response.status_code}")
                        print(f"Response content: {download_response.text[:1000]}")
                        raise Exception(f"Download failed with status {download_response.status_code}")

                    # Stream to temp file in chunks (memory efficient for large files)
                    for chunk in download_response.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive chunks
                            tmp_file.write(chunk)
                    tmp_file.flush()

                print(f"Downloaded to temporary file")

                # Basic validation: check file size
                tmp_file.seek(0, 2)  # Seek to end
                file_size = tmp_file.tell()
                tmp_file.seek(0)  # Reset to beginning

                if file_size < 100:
                    raise Exception(f"Downloaded file too small ({file_size} bytes) to be RxNORM zip")

                print(f"Downloaded {file_size:,} bytes")

                # Check zip file magic number
                magic_bytes = tmp_file.read(2)
                tmp_file.seek(0)  # Reset to beginning

                if magic_bytes != b'PK':
                    raise Exception("Downloaded file is not a valid zip file (missing PK magic number)")

                print(f"Validated zip file ({file_size:,} bytes)")

                # Upload original zip to S3 for lineage
                s3_client.upload_fileobj(tmp_file, bucket_name, zip_key)
                print(f"Uploaded original zip to: s3://{bucket_name}/{zip_key}")

                # Extract RRF files and upload to S3
                tmp_file.seek(0)
                rrf_files = {}

                print("Extracting RRF files from zip...")
                with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
                    for file_info in zip_ref.infolist():
                        if not file_info.is_dir() and file_info.filename.startswith('rrf/') and file_info.filename.upper().endswith('.RRF'):
                            print(f"Processing file: {file_info.filename}")

                            with zip_ref.open(file_info) as rrf_file:
                                # Clean filename for S3 key
                                clean_filename = file_info.filename.replace('rrf/', '')
                                rrf_key = f"raw/{dataset}/run_id={run_id}/{clean_filename}"

                                # Stream RRF file directly to S3
                                s3_client.upload_fileobj(
                                    rrf_file,
                                    bucket_name,
                                    rrf_key
                                )
                                rrf_files[clean_filename] = rrf_key

                print(f"Successfully extracted and uploaded {len(rrf_files)} RRF files: {list(rrf_files.keys())}")
                break  # Success, exit retry loop

        except (requests.RequestException, Exception) as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

                # Re-authenticate for retry
                print("Re-authenticating for retry...")
                service_ticket = authenticate_umls(api_key)
                download_url_with_ticket = f"{download_url}?ticket={service_ticket}"
            else:
                raise

    # Step 4: Process each RRF table
    for rrf_filename, rrf_key in rrf_files.items():
        table_name = rrf_filename.replace('.RRF', '').upper()

        # Only process tables we're configured for
        if table_name not in tables_to_process:
            print(f"Skipping {table_name} - not in tables_to_process")
            continue

        print(f"\nProcessing {table_name} table...")

        # Get column mapping for this table
        if table_name not in COLUMN_MAPPINGS:
            print(f"Warning: No column mapping for {table_name}, skipping")
            continue

        column_mapping = COLUMN_MAPPINGS[table_name]

        # Read RRF file from S3
        rrf_s3_path = f"s3://{bucket_name}/{rrf_key}"
        print(f"Reading RRF from: {rrf_s3_path}")

        # RRF files are pipe-delimited with no header and no quotes
        df = spark.read.option("header", "false") \
                      .option("inferSchema", "false") \
                      .option("sep", "|") \
                      .option("quote", "") \
                      .option("escape", "") \
                      .csv(rrf_s3_path)

        print(f"Loaded {df.count()} records for {table_name}")

        if df.count() == 0:
            print(f"Warning: No data found for {table_name}")
            continue

        # Apply column names based on mapping
        old_columns = list(column_mapping.keys())
        for i, old_col in enumerate(old_columns):
            if i < len(df.columns):
                df = df.withColumnRenamed(f"_c{i}", old_col)

        # Apply column mapping to get clean names
        df = apply_column_mapping(df, column_mapping)

        # Clean the data
        df = clean_rrf_data(df)

        # Add metadata
        df_bronze = df.withColumn("meta_run_id", lit(run_id)) \
                     .withColumn("meta_release_date", lit(release_date))

        print(f"{table_name} final columns: {df_bronze.columns}")

        # Write to bronze layer
        bronze_table_s3_path = f"s3://{bucket_name}/{bronze_path_fragment}_{table_name.lower()}"
        print(f"Writing {table_name} to: {bronze_table_s3_path}")

        df_bronze.write \
                 .mode("overwrite") \
                 .option("compression", args['compression_codec']) \
                 .parquet(bronze_table_s3_path)

        print(f"Successfully processed {df.count()} {table_name} records to bronze layer")

    print(f"\nComplete RxNORM ETL finished successfully:")
    print(f"  - Release date: {release_date}")
    print(f"  - Downloaded: {content_length or 'unknown'} bytes")
    print(f"  - RRF files processed: {len([t for t in tables_to_process if f'{t}.RRF' in [f.upper() for f in rrf_files.keys()]])}")
    print(f"  - Bronze tables created: {len(tables_to_process)}")
    print("Note: Run crawlers manually to update Athena catalog")

except Exception as e:
    print(f"RxNORM Bronze job error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()
    print("RxNORM Bronze job completed")