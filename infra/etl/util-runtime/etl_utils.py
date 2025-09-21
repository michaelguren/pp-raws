"""
Shared ETL utilities for downloading and extracting files to S3 raw paths
Used by all bronze Glue jobs for consistent file handling
"""
import boto3 # type: ignore[import-not-found]
import urllib.request
import zipfile
import tempfile
import shutil
import time
import posixpath
from botocore.config import Config # type: ignore[import-not-found]
from zipfile import BadZipFile
from urllib.error import URLError, HTTPError


def safe_key(prefix, name):
    """Prevent zip-slip: strip leading slashes, collapse .., keep posix separators"""
    name = name.lstrip("/").replace("\\", "/")
    parts = [p for p in name.split("/") if p not in ("", ".", "..")]
    return posixpath.join(prefix, *parts)


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
    raw_path_parts = raw_s3_path.replace('s3://', '').split('/', 1)
    raw_bucket = raw_path_parts[0]
    raw_prefix = raw_path_parts[1] if len(raw_path_parts) > 1 else ''

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
                            file_key = safe_key(raw_prefix, target_name)

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