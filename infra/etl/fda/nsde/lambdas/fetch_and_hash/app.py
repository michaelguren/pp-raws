"""
Simple fetch and hash Lambda - no layers, inline utilities
"""
import json
import urllib.request
import zipfile
from io import BytesIO
import time
import logging
import hashlib
import boto3
import os
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def compute_sha256(data):
    """Compute SHA256 hash of bytes data"""
    return hashlib.sha256(data).hexdigest()

def get_json_from_s3(bucket, key):
    """Get JSON object from S3"""
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read())
    except s3.exceptions.NoSuchKey:
        return None

def upload_to_s3(bucket, key, data, content_type='application/octet-stream'):
    """Upload data to S3"""
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type
    )

def handler(event, context):
    """
    Simple Lambda handler for fetch and hash
    
    Input: Step Functions context with dataset, source_url, run_id, bucket
    Output: { changed: bool, raw_path: str, bronze_path: str, source_sha256: str }
    """
    try:
        dataset = event['dataset']
        source_url = event['source_url']
        # Use raw bucket from environment or event
        bucket = event.get('bucket') or os.environ.get('RAW_BUCKET_NAME')
        
        # Generate run_id if not provided
        run_id = event.get('run_id') or f"{dataset}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        
        # Get other bucket names for output paths
        bronze_bucket = os.environ.get('BRONZE_BUCKET_NAME')
        silver_bucket = os.environ.get('SILVER_BUCKET_NAME')
        
        logger.info(f"Processing {dataset} from {source_url}")
        
        # Download source data (simple approach for ~50MB files)
        logger.info(f"Downloading from: {source_url}")
        with urllib.request.urlopen(source_url, timeout=60) as response:
            source_data = response.read()
        
        logger.info(f"Downloaded {len(source_data)} bytes")
        
        # Compute hash
        source_sha256 = compute_sha256(source_data)
        logger.info(f"SHA256: {source_sha256}")
        
        # Check if changed
        manifest = get_json_from_s3(bucket, f"manifests/{dataset}.json")
        changed = True
        
        if manifest and manifest.get('source_sha256') == source_sha256:
            changed = False
            logger.info("Source unchanged")
        else:
            logger.info("Source changed or first run")
        
        # Upload raw data
        raw_prefix = f"raw/{dataset}/{run_id}/"
        
        # Save original ZIP
        zip_key = f"{raw_prefix}source.zip"
        upload_to_s3(bucket, zip_key, source_data, 'application/zip')
        
        # Extract and save CSVs
        csv_count = 0
        with zipfile.ZipFile(BytesIO(source_data)) as z:
            for file_name in z.namelist():
                if file_name.lower().endswith('.csv'):
                    with z.open(file_name) as csv_file:
                        csv_data = csv_file.read()
                        csv_key = f"{raw_prefix}{file_name}"
                        upload_to_s3(bucket, csv_key, csv_data, 'text/csv')
                        csv_count += 1
        
        logger.info(f"Extracted {csv_count} CSV files")
        
        return {
            'dataset': dataset,
            'source_url': source_url,
            'force': event.get('force', False),
            'run_id': run_id,
            'changed': changed,
            'source_sha256': source_sha256,
            'raw_path': f"s3://{bucket}/{raw_prefix}",
            'bronze_path': f"s3://{bronze_bucket}/{dataset}/run={run_id}/",
            'silver_path': f"s3://{silver_bucket}/{dataset}/",
            'csv_count': csv_count
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise