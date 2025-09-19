"""
Complete RxClass ETL Job - API Collection, Transform, and Store
Collects RxClass data from NLM RxNav API, saves raw JSON for lineage, transforms to Bronze layer
"""
import sys
import boto3  # type: ignore[import-not-found]
import urllib.request
import urllib.parse
import json
import time
from io import StringIO
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit, col  # type: ignore[import-not-found]
from pyspark.sql.types import StructType, StructField, StringType  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'bucket_name', 'dataset',
    'class_types_url', 'all_classes_url', 'bronze_database',
    'raw_path', 'bronze_path', 'compression_codec'
])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])

# Generate human-readable runtime run_id
from datetime import datetime
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: 20240311_143022

# Get configuration from job arguments
bucket_name = args['bucket_name']
dataset = args['dataset']
class_types_url = args['class_types_url']
all_classes_url = args['all_classes_url']
bronze_database = args['bronze_database']

# Build full S3 paths from path fragments
raw_path_fragment = args['raw_path']
bronze_path_fragment = args['bronze_path']

raw_s3_path = f"s3://{bucket_name}/{raw_path_fragment}run_id={run_id}/"
bronze_s3_path = f"s3://{bucket_name}/{bronze_path_fragment}"

# Initialize S3 client
s3_client = boto3.client('s3')

print(f"Starting RxClass ETL job")
print(f"Dataset: {dataset}")
print(f"Run ID: {run_id}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze path: {bronze_s3_path}")

def fetch_json_with_retry(url, max_retries=3, delay=1):
    """Fetch JSON from URL with retry logic and respectful delays"""
    for attempt in range(max_retries):
        try:
            print(f"Fetching: {url} (attempt {attempt + 1})")

            # Add headers to be respectful to the API
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
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {str(e)}")

def save_raw_json_to_s3(data, filename):
    """Save JSON data to S3 raw layer for lineage"""
    json_content = json.dumps(data, indent=2)
    s3_key = f"{raw_path_fragment}run_id={run_id}/{filename}"

    print(f"Saving raw JSON to s3://{bucket_name}/{s3_key}")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json_content.encode('utf-8'),
        ContentType='application/json'
    )
    return s3_key

try:
    # Step 1: Fetch all class types
    print("Step 1: Fetching class types...")
    class_types_data = fetch_json_with_retry(class_types_url)

    # Save raw class types response
    save_raw_json_to_s3(class_types_data, "class_types_response.json")

    # Extract class type names
    class_types = class_types_data.get('classTypeList', {}).get('classTypeName', [])
    print(f"Found {len(class_types)} class types: {class_types}")

    # Step 2: Collect all classes for each type
    print("Step 2: Collecting classes for each type...")
    all_rxclasses = []

    for i, class_type in enumerate(class_types):
        print(f"Processing class type {i+1}/{len(class_types)}: {class_type}")

        # Build URL for this class type
        encoded_class_type = urllib.parse.quote(class_type)
        class_url = f"{all_classes_url}?classTypes={encoded_class_type}"

        # Fetch classes for this type
        class_data = fetch_json_with_retry(class_url)

        # Save raw response for this class type
        safe_filename = class_type.replace('/', '_').replace(' ', '_')
        save_raw_json_to_s3(class_data, f"classes_{safe_filename}.json")

        # Extract concepts
        concept_list = class_data.get('rxclassMinConceptList', {})
        concepts = concept_list.get('rxclassMinConcept', [])

        if concepts:
            print(f"Found {len(concepts)} classes for type '{class_type}'")
            all_rxclasses.extend(concepts)
        else:
            print(f"No classes found for type '{class_type}'")

        # Be respectful to the API - small delay between requests
        if i < len(class_types) - 1:  # Don't delay after the last request
            time.sleep(0.5)

    print(f"Total classes collected: {len(all_rxclasses)}")

    # Save complete aggregated dataset
    aggregated_data = {
        "metadata": {
            "run_id": run_id,
            "collection_timestamp": datetime.now().isoformat(),
            "total_class_types": len(class_types),
            "total_classes": len(all_rxclasses),
            "class_types": class_types
        },
        "rxclasses": all_rxclasses
    }
    save_raw_json_to_s3(aggregated_data, "rxclasses_complete.json")

    # Step 3: Transform to Bronze layer
    print("Step 3: Transforming to Bronze layer...")

    if not all_rxclasses:
        raise Exception("No RxClass data collected - cannot proceed to Bronze layer")

    # Define schema for consistent typing
    rxclass_schema = StructType([
        StructField("class_id", StringType(), True),
        StructField("class_name", StringType(), True),
        StructField("class_type", StringType(), True),
        StructField("meta_run_id", StringType(), True)
    ])

    # Convert to Spark DataFrame with explicit schema
    rxclass_records = []
    for rxclass in all_rxclasses:
        record = (
            rxclass.get('classId', ''),
            rxclass.get('className', ''),
            rxclass.get('classType', ''),
            run_id
        )
        rxclass_records.append(record)

    # Create DataFrame
    df = spark.createDataFrame(rxclass_records, schema=rxclass_schema)

    print(f"Created DataFrame with {df.count()} records")
    df.show(10, truncate=False)

    # Data quality checks
    null_class_ids = df.filter(col("class_id").isNull() | (col("class_id") == "")).count()
    null_class_names = df.filter(col("class_name").isNull() | (col("class_name") == "")).count()

    print(f"Data quality check - Null class_ids: {null_class_ids}, Null class_names: {null_class_names}")

    if null_class_ids > len(all_rxclasses) * 0.01:  # More than 1% null IDs
        raise Exception(f"Too many null class_ids: {null_class_ids}")

    # Write to Bronze layer (kill-and-fill approach)
    print(f"Writing to Bronze layer: {bronze_s3_path}")

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", args['compression_codec']) \
        .parquet(bronze_s3_path)

    print("Successfully wrote Bronze layer data")

    print("RxClass ETL job completed successfully!")
    print(f"Total records processed: {len(all_rxclasses)}")
    print(f"Bronze data location: {bronze_s3_path}")
    print("Note: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"ERROR: RxClass ETL job failed: {str(e)}")
    raise e

finally:
    job.commit()