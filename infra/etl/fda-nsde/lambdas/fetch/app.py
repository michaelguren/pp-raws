"""
Simple fetch Lambda - downloads and stores raw data
"""
import json
import urllib.request
import zipfile
from io import BytesIO
import time
import logging
import boto3
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
    Simple Lambda handler for data fetch
    
    Input: dataset, source_url, run_id (optional)
    Output: { raw_path: str, bronze_path: str }
    """
    try:
        dataset = event['dataset']
        source_url = event['source_url']
        
        # Use environment variable for data warehouse bucket
        bucket = os.environ.get('DATA_WAREHOUSE_BUCKET_NAME')
        
        # Generate run_id if not provided
        run_id = f"{dataset}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        
        # Get current datetime for partitioning (allows multiple runs per day)
        partition_datetime = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M%S')
        
        logger.info(f"Processing {dataset} from {source_url}")
        
        # Download source data (simple approach for ~50MB files)
        logger.info(f"Downloading from: {source_url}")
        with urllib.request.urlopen(source_url, timeout=60) as response:
            source_data = response.read()
        
        logger.info(f"Downloaded {len(source_data)} bytes")
        
        logger.info("Processing file (always)")
        
        # Upload raw data
        raw_prefix = f"raw/{dataset}/run_id={run_id}/"
        
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
            'run_id': run_id,
            'raw_path': f"s3://{bucket}/{raw_prefix}",
            'bronze_path': f"s3://{bucket}/bronze/{dataset}/partition_datetime={partition_datetime}/",
            'silver_path': f"s3://{bucket}/silver/{dataset}/",
            'csv_count': csv_count
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise