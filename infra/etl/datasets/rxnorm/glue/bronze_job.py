"""
Complete RxNORM ETL Job - Download, Transform, and Store RRF Files
Downloads RxNORM data using UMLS authentication, saves raw files for lineage,
transforms to Bronze layer with multiple RRF tables
"""
import sys
import posixpath
import tempfile
import shutil
import time
import zipfile
from datetime import datetime, date, timedelta
import boto3  # type: ignore[import-not-found]
import urllib.request
import requests  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit, col, when, trim  # type: ignore[import-not-found]

# ============================================================================
# HELPER FUNCTIONS (RxNORM-specific)
# ============================================================================

def get_latest_rxnorm_release_date():
    """Get latest RxNORM release date from NLM website"""
    import re
    files_page_url = "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html"

    try:
        req = urllib.request.Request(files_page_url)
        req.add_header('User-Agent', 'AWS-Glue-ETL/1.0')

        with urllib.request.urlopen(req, timeout=30) as response:
            html = response.read().decode('utf-8')

        pattern = r'RxNorm_full_(\d{8})\.zip'
        dates = re.findall(pattern, html)

        if dates:
            parsed = [(datetime.strptime(d, "%m%d%Y"), d) for d in dates if len(d) == 8]
            if parsed:
                parsed.sort(reverse=True)
                return parsed[0][1]

        raise Exception("No valid dates found")

    except Exception as e:
        # Fallback to first Monday of current month
        print(f"Could not fetch latest release ({e}), using first Monday of current month")
        today = date.today()
        first = today.replace(day=1)
        days_to_monday = (0 - first.weekday()) % 7
        monday = first + timedelta(days=days_to_monday)
        return monday.strftime("%m%d%Y")


def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {str(e)}")
        raise


def authenticate_umls(api_key, download_url):
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
        "RXCUI": "rxcui", "LAT": "language", "TS": "term_status", "LUI": "lui",
        "STT": "string_type", "SUI": "sui", "ISPREF": "is_preferred", "RXAUI": "rxaui",
        "SAUI": "source_aui", "SCUI": "source_cui", "SDUI": "source_dui", "SAB": "source",
        "TTY": "tty", "CODE": "code", "STR": "name", "SRL": "source_restriction",
        "SUPPRESS": "suppress", "CVF": "cvf"
    },
    'RXNSAT': {
        "RXCUI": "rxcui", "LUI": "lui", "SUI": "sui", "RXAUI": "rxaui",
        "STYPE": "stype", "CODE": "code", "ATUI": "atui", "SATUI": "satui",
        "ATN": "attribute_name", "SAB": "source", "ATV": "attribute_value",
        "SUPPRESS": "suppress", "CVF": "cvf"
    },
    'RXNREL': {
        "RXCUI1": "rxcui1", "RXAUI1": "rxaui1", "STYPE1": "stype1", "REL": "relationship",
        "RXCUI2": "rxcui2", "RXAUI2": "rxaui2", "STYPE2": "stype2", "RELA": "rela",
        "RUI": "rui", "SRUI": "srui", "SAB": "source", "SL": "source_label",
        "RG": "rg", "DIR": "dir", "SUPPRESS": "suppress", "CVF": "cvf"
    },
    'RXNSTY': {
        "RXCUI": "rxcui", "TUI": "tui", "STN": "stn", "STY": "sty",
        "ATUI": "atui", "CVF": "cvf"
    },
    'RXNCUI': {
        "RXCUI1": "rxcui1", "RXCUI2": "rxcui2"
    },
    'RXNATOMARCHIVE': {
        "RXAUI": "rxaui", "AUI": "aui", "STR": "str", "ARCHIVE_TIMESTAMP": "archive_timestamp",
        "CREATED_TIMESTAMP": "created_timestamp", "UPDATED_TIMESTAMP": "updated_timestamp",
        "CODE": "code", "IS_BRAND": "is_brand", "LAT": "lat", "LAST_RELEASED": "last_released",
        "SAUI": "saui", "VSAB": "vsab", "RXCUI": "rxcui", "SAB": "sab",
        "TTY": "tty", "MERGED_TO_RXCUI": "merged_to_rxcui"
    }
}

# ============================================================================
# INITIALIZATION
# ============================================================================

# Parse job arguments
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
    release_date = get_latest_rxnorm_release_date()
    print(f"Latest release date: {release_date}")

# Initialize Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])

# ============================================================================
# CONFIGURATION
# ============================================================================

# Dataset metadata
dataset = args['dataset']  # e.g., "rxnorm"
bronze_database = args['bronze_database']  # e.g., "pp_dw_bronze"
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g., "20250104_153045"

# Source configuration (UMLS-specific)
bucket_name = args['bucket_name']  # e.g., "pp-dw-123456789"
umls_api_secret = args['umls_api_secret']  # e.g., "umls-api-key"
tables_to_process = args['tables_to_process'].split(',')  # e.g., ["RXNCONSO", "RXNREL"]
base_url = "https://download.nlm.nih.gov/umls/kss/rxnorm"
download_url = f"{base_url}/RxNorm_full_{release_date}.zip"

# S3 paths (dataset-specific, raw includes run_id partition for lineage tracking)
raw_path_with_run = posixpath.join(args['raw_path'], f'run_id={run_id}') + '/'  # e.g., "s3://pp-dw-123/raw/rxnorm/run_id=20250104_153045/"
bronze_dataset_path = args['bronze_path']  # e.g., "s3://pp-dw-123/bronze/rxnorm/"

# ============================================================================
# JOB EXECUTION
# ============================================================================

print(f"Starting RxNORM Bronze ETL for {dataset}")
print(f"  Run ID: {run_id}")
print(f"  Database: {bronze_database}")
print(f"  Tables: {tables_to_process}")
print(f"  Release: {release_date}")
print(f"  Source: {download_url}")
print(f"  Raw path: {raw_path_with_run}")
print(f"  Bronze path: {bronze_dataset_path}")

try:
    # Step 1: Authenticate and download RxNORM ZIP with UMLS
    print(f"\n[1/2] Downloading from UMLS...")

    api_key = get_secret(umls_api_secret)
    service_ticket = authenticate_umls(api_key, download_url)
    download_url_with_ticket = f"{download_url}?ticket={service_ticket}"

    session = requests.Session()
    session.headers.update({'User-Agent': 'AWS-Glue-ETL/1.0'})

    s3_client = boto3.client('s3')
    zip_key = posixpath.join(args['raw_path'], f'run_id={run_id}', f'RxNorm_full_{release_date}.zip')

    max_retries = 3
    rrf_files = {}

    for attempt in range(max_retries):
        try:
            print(f"  Download attempt {attempt + 1}/{max_retries}")

            with tempfile.NamedTemporaryFile(suffix='.zip', delete=True) as tmp_file:
                # Stream download with authentication
                with session.get(download_url_with_ticket, timeout=600, allow_redirects=True, stream=True) as response:
                    if response.status_code != 200:
                        raise Exception(f"Download failed with status {response.status_code}")

                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        print(f"  Downloading {int(content_length):,} bytes...")

                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            tmp_file.write(chunk)
                    tmp_file.flush()

                # Validate zip file
                tmp_file.seek(0, 2)
                file_size = tmp_file.tell()
                tmp_file.seek(0)

                if file_size < 100:
                    raise Exception(f"File too small ({file_size} bytes)")

                magic_bytes = tmp_file.read(2)
                tmp_file.seek(0)

                if magic_bytes != b'PK':
                    raise Exception("Not a valid ZIP file")

                print(f"  ✓ Downloaded {file_size:,} bytes")

                # Upload original ZIP to S3 for lineage
                s3_client.upload_fileobj(tmp_file, bucket_name, zip_key)
                print(f"  ✓ Saved ZIP to raw layer")

                # Extract RRF files to S3
                tmp_file.seek(0)
                print("  Extracting RRF files...")

                with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
                    for file_info in zip_ref.infolist():
                        if not file_info.is_dir() and file_info.filename.startswith('rrf/') and file_info.filename.upper().endswith('.RRF'):
                            clean_filename = file_info.filename.replace('rrf/', '')
                            rrf_key = posixpath.join(args['raw_path'], f'run_id={run_id}', clean_filename)

                            with zip_ref.open(file_info) as rrf_file:
                                s3_client.upload_fileobj(rrf_file, bucket_name, rrf_key)
                                rrf_files[clean_filename] = rrf_key

                print(f"  ✓ Extracted {len(rrf_files)} RRF files to raw layer")
                break  # Success

        except Exception as e:
            print(f"  Error: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"  Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                # Re-authenticate
                service_ticket = authenticate_umls(api_key, download_url)
                download_url_with_ticket = f"{download_url}?ticket={service_ticket}"
            else:
                raise

    # Step 2: Transform RRF tables to bronze
    print(f"\n[2/2] Transforming {len(tables_to_process)} tables to bronze...")
    total_records = 0
    tables_processed = []

    for rrf_filename, rrf_key in rrf_files.items():
        table_name = rrf_filename.replace('.RRF', '').upper()

        if table_name not in tables_to_process:
            continue

        if table_name not in COLUMN_MAPPINGS:
            print(f"  Warning: No column mapping for {table_name}, skipping")
            continue

        print(f"\n  Processing: {table_name}")

        # Read RRF file (pipe-delimited, no header, no quotes)
        rrf_s3_path = f"s3://{bucket_name}/{rrf_key}"
        df = spark.read.option("header", "false") \
                      .option("inferSchema", "false") \
                      .option("sep", "|") \
                      .option("quote", "") \
                      .option("escape", "") \
                      .csv(rrf_s3_path)

        row_count = df.count()
        if row_count == 0:
            print(f"    Warning: No data found, skipping")
            continue

        print(f"    Loaded: {row_count:,} records")

        # Apply column names and mapping
        column_mapping = COLUMN_MAPPINGS[table_name]
        old_columns = list(column_mapping.keys())

        for i, old_col in enumerate(old_columns):
            if i < len(df.columns):
                df = df.withColumnRenamed(f"_c{i}", old_col)

        df = df.select(*old_columns)
        df = apply_column_mapping(df, column_mapping)
        df = clean_rrf_data(df)

        # Add metadata
        df = df.withColumn("meta_run_id", lit(run_id)) \
               .withColumn("meta_release_date", lit(release_date))

        # Write to bronze (multi-table dataset: bronze/{dataset}/{table}/)
        output_path = posixpath.join(bronze_dataset_path, table_name.lower())
        df.write.mode("overwrite").option("compression", args['compression_codec']).parquet(output_path)

        print(f"    Written: {output_path}")
        total_records += row_count
        tables_processed.append(table_name)

    # Summary
    print(f"\n✓ RxNORM Bronze ETL completed successfully")
    print(f"  Release: {release_date}")
    print(f"  Total records: {total_records:,}")
    print(f"  Tables: {', '.join(tables_processed)}")
    print(f"\nNote: Run crawlers to update Glue catalog")

except Exception as e:
    print(f"\n✗ RxNORM Bronze ETL failed: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()
    print("Job committed")
