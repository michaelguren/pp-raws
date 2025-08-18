"""
Simple update manifest Lambda - no layers, inline utilities
"""
import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Simple manifest update handler
    
    Input: Step Functions context with dataset, run_id, source_sha256, bucket
    Output: { success: bool }
    """
    try:
        dataset = event['dataset']
        run_id = event['run_id']
        source_sha256 = event['Payload']['source_sha256']
        bucket = event['bucket']
        
        logger.info(f"Updating manifest for {dataset}")
        
        # Create manifest
        manifest = {
            'dataset': dataset,
            'source_sha256': source_sha256,
            'last_run_id': run_id,
            'last_processed_date': datetime.utcnow().strftime('%Y-%m-%d'),
            'last_processed_timestamp': datetime.utcnow().isoformat(),
            'status': 'completed'
        }
        
        # Save to S3
        s3 = boto3.client('s3')
        manifest_key = f"manifests/{dataset}.json"
        
        s3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Updated manifest: s3://{bucket}/{manifest_key}")
        
        return {
            'success': True,
            'manifest_key': manifest_key
        }
        
    except Exception as e:
        logger.error(f"Error updating manifest: {str(e)}")
        raise