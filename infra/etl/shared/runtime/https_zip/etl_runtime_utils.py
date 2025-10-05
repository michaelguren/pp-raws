"""
Shared ETL utilities for bronze layer jobs
Handles HTTP/HTTPS downloads, ZIP extraction, JSON APIs, and S3 operations
Used by both factory-based (Pattern A) and custom (Pattern B) bronze jobs
"""
import boto3 # type: ignore[import-not-found]
import urllib.request
import zipfile
import tempfile
import shutil
import time
import posixpath
import json
from botocore.config import Config # type: ignore[import-not-found]
from zipfile import BadZipFile
from urllib.error import URLError, HTTPError


def _extract_s3_path_parts(s3_path):
    """Extract path parts from S3 path like s3://bucket/key/path"""
    parts = s3_path.replace('s3://', '').split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    return bucket, prefix


def apply_schema(df, table_schema):
    """
    Apply column schema including renaming and type casting

    Args:
        df: PySpark DataFrame
        table_schema: Dict mapping source column names to config with target_name, type, format

    Returns:
        Transformed DataFrame (or original if table_schema is None/empty)
    """
    if not table_schema:
        return df

    # Import PySpark functions here (only available in Glue runtime)
    from pyspark.sql.functions import col, to_date, when, trim, length  # type: ignore[import-not-found]

    # Build list of column transformations
    select_cols = []
    processed_cols = set()

    for source_col, config in table_schema.items():
        if source_col not in df.columns:
            print(f"Warning: Column '{source_col}' not found in dataframe, skipping")
            continue

        target_col = config['target_name']
        col_type = config.get('type', 'string')

        # Build expression: rename + cast in one operation
        if col_type == 'date':
            date_format = config.get('format', 'yyyyMMdd')
            # Handle invalid dates: convert empty strings, nulls, and ancient dates (before 1900) to null
            # Ancient dates are almost always data entry errors and cause Spark 3.x Parquet write errors
            parsed_date = to_date(col(source_col), date_format)
            expr = when(
                (trim(col(source_col)) == "") |
                col(source_col).isNull() |
                (length(trim(col(source_col))) == 0) |
                (parsed_date < '1900-01-01') |
                parsed_date.isNull(),
                None
            ).otherwise(
                parsed_date
            ).alias(target_col)
        elif col_type == 'integer':
            expr = col(source_col).cast('integer').alias(target_col)
        elif col_type == 'long':
            expr = col(source_col).cast('long').alias(target_col)
        elif col_type == 'float':
            expr = col(source_col).cast('float').alias(target_col)
        elif col_type == 'decimal':
             precision = config.get('precision', '10,2')
             expr = col(source_col).cast(f'decimal({precision})').alias(target_col)
        elif col_type == 'double':
            expr = col(source_col).cast('double').alias(target_col)
        elif col_type == 'boolean':
            expr = col(source_col).cast('boolean').alias(target_col)
        else:  # string or default
            expr = col(source_col).alias(target_col)

        select_cols.append(expr)
        processed_cols.add(source_col)

    # Add remaining columns that weren't in schema (pass-through)
    for col_name in df.columns:
        if col_name not in processed_cols:
            select_cols.append(col(col_name))

    # Apply all transformations in a single select operation
    return df.select(*select_cols)


def _safe_key(prefix, name):
    """Prevent zip-slip: strip leading slashes, collapse .., keep posix separators"""
    name = name.lstrip("/").replace("\\", "/")
    parts = [p for p in name.split("/") if p not in ("", ".", "..")]
    return posixpath.join(prefix, *parts)


def build_raw_path_with_run_id(raw_base_path, run_id):
    """
    Build raw S3 path with run_id partition for lineage tracking

    Args:
        raw_base_path: Base raw path like "s3://bucket/raw/dataset/"
        run_id: Run identifier like "20240311_143022"

    Returns:
        Complete raw path like "s3://bucket/raw/dataset/run_id=20240311_143022/"
    """
    # Remove trailing slash if present, add run_id partition, add trailing slash
    base = raw_base_path.rstrip('/')
    return f"{base}/run_id={run_id}/"


def fetch_json_with_retry(url, max_retries=3, initial_delay=1):
    """
    Fetch JSON from URL with retry logic and exponential backoff

    Args:
        url: URL to fetch
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds between retries

    Returns:
        Parsed JSON data or None if all retries fail
    """
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'AWS-Glue-ETL-Job/1.0',
                'Accept': 'application/json',
                'Connection': 'close'
            }

            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                content = response.read().decode('utf-8')
                return json.loads(content)

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Fetch attempt {attempt + 1} failed: {str(e)}, retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"Failed to fetch {url} after {max_retries} attempts: {str(e)}")
                return None

    return None


def save_json_to_s3(data, s3_path):
    """
    Save JSON data to S3 for lineage tracking

    Args:
        data: Python dict/list to serialize as JSON
        s3_path: Complete S3 URL like "s3://bucket/raw/dataset/run_id=xxx/file.json"

    Returns:
        S3 path where file was saved
    """
    bucket, key = _extract_s3_path_parts(s3_path)

    json_content = json.dumps(data, indent=2)

    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_content.encode('utf-8'),
        ContentType='application/json'
    )

    print(f"Saved JSON to {s3_path}")
    return s3_path


def download_and_extract(source_url, raw_s3_path, file_table_mapping, max_retries=3):
    """
    Download file from source_url, extract if needed, upload to S3 raw path

    Args:
        source_url: URL to download from
        raw_s3_path: Complete S3 URL like "s3://bucket/raw/dataset/run_id=20240311_143022/"
        file_table_mapping: Dict mapping source files to table names
        max_retries: Number of download attempts

    Returns:
        Dict with extraction results: {"files_extracted": int, "content_length": str, "uploaded": dict}
    """
    # Extract bucket and prefix from S3 URL
    raw_bucket, raw_prefix = _extract_s3_path_parts(raw_s3_path)

    # S3 client with retry configuration
    s3_client = boto3.client(
        "s3",
        config=Config(retries={"max_attempts": 10, "mode": "standard"})
    )

    content_length = None
    files_extracted = 0
    uploaded = {}  # Track uploaded file paths

    for attempt in range(max_retries):
        try:
            print(f"Download attempt {attempt + 1}/{max_retries}")

            with tempfile.NamedTemporaryFile(suffix='.zip', mode='w+b', delete=True) as tmp_file:
                # Stream download to temporary file
                with urllib.request.urlopen(source_url, timeout=300) as response:
                    # Get content length if available
                    content_length = response.headers.get('Content-Length')
                    if content_length:
                        print(f"File size: {int(content_length):,} bytes")

                    # Stream to temp file in chunks (memory efficient)
                    shutil.copyfileobj(response, tmp_file)
                    tmp_file.flush()  # Make buffered bytes readable

                print(f"Downloaded to temporary file")

                # Extract files and upload to S3
                tmp_file.seek(0)  # Rewind for zipfile to read headers

                try:
                    with zipfile.ZipFile(tmp_file, 'r') as zip_ref:
                        # Build lowercase basename lookup from mapping keys
                        want = {posixpath.basename(k).lower(): k for k in file_table_mapping.keys()}

                        for file_info in zip_ref.infolist():
                            if file_info.is_dir():
                                continue

                            # Normalize ZIP entry name
                            zip_name = file_info.filename.replace("\\", "/")  # normalize slashes
                            zip_base = posixpath.basename(zip_name).lower()   # basename only

                            # Check if this file should be extracted based on mapping
                            should_extract = zip_base in want
                            if not should_extract:
                                continue

                            # Optional basic size guardrail (5 GB per file)
                            if file_info.file_size > 5_000_000_000:
                                raise ValueError(f"Zip member too large: {file_info.filename}")

                            # Flatten the path - use just the basename, no subdirectories
                            target_name = posixpath.basename(zip_name)
                            file_key = _safe_key(raw_prefix, target_name)

                            # Stream each file from zip to S3
                            with zip_ref.open(file_info) as extracted_file:
                                s3_client.upload_fileobj(
                                    extracted_file,
                                    raw_bucket,
                                    file_key,
                                    ExtraArgs={"ContentType": "text/csv"}
                                )
                                print(f"Extracted file: {file_info.filename} -> {target_name}")
                                files_extracted += 1

                                # Track the uploaded S3 path
                                uploaded[target_name] = f"s3://{raw_bucket}/{file_key}"

                except BadZipFile:
                    raise RuntimeError("Downloaded file is not a valid ZIP")

                print(f"Successfully extracted and uploaded {files_extracted} files")
                break  # Success, exit retry loop

        except (URLError, HTTPError) as e:
            print(f"Network error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise

    return {
        "files_extracted": files_extracted,
        "content_length": content_length,
        "uploaded": uploaded
    }