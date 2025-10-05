"""
RxClass Drug Members ETL Job - API Collection, Transform, and Store
Reads rxclass bronze table, fetches drug members for each class via RxNav API
"""
import sys
import urllib.parse
import time
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.types import StructType, StructField, StringType  # type: ignore[import-not-found]
from etl_runtime_utils import build_raw_path_with_run_id, fetch_json_with_retry, save_json_to_s3  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset', 'bronze_database', 'rxclass_table', 'api_base_url',
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
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Get configuration from job arguments
dataset = args['dataset']
bronze_database = args['bronze_database']
rxclass_table = args['rxclass_table']
api_base_url = args['api_base_url']

# Build S3 paths (raw includes run_id partition for lineage tracking)
raw_s3_path = build_raw_path_with_run_id(args['raw_path'], run_id)
bronze_s3_path = args['bronze_path']

print(f"Starting RxClass Drug Members ETL job")
print(f"Dataset: {dataset}")
print(f"Run ID: {run_id}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze path: {bronze_s3_path}")
print(f"Source table: {bronze_database}.{rxclass_table}")


def get_api_params(class_type):
    """
    Map class_type to RxNav API parameters (relaSource, rela, trans)
    Based on NLM RxNav API documentation and legacy patterns
    """
    class_type_upper = class_type.upper() if class_type else ""

    # ATC classifications
    if 'ATC' in class_type_upper:
        return {'relaSource': 'ATC', 'trans': '1'}

    # DAILYMED relationships - Established Pharmacologic Class
    if class_type_upper == 'EPC':
        return {'relaSource': 'DAILYMED', 'rela': 'has_EPC', 'trans': '1'}

    # DAILYMED relationships - Mechanism of Action
    if class_type_upper == 'MOA':
        return {'relaSource': 'DAILYMED', 'rela': 'has_MoA', 'trans': '1'}

    # DAILYMED relationships - Physiologic Effect
    if class_type_upper == 'PE':
        return {'relaSource': 'DAILYMED', 'rela': 'has_PE', 'trans': '1'}

    # DAILYMED relationships - Chemical Structure
    if class_type_upper == 'CHEM':
        return {'relaSource': 'DAILYMED', 'rela': 'has_chemical_structure', 'trans': '1'}

    # VA classifications
    if 'VA' in class_type_upper:
        return {'relaSource': 'VA', 'trans': '1'}

    # MESH/Disease classifications
    if class_type_upper in ['DISEASE', 'DISPOS']:
        return {'relaSource': 'MEDRT', 'trans': '1'}

    # MeSH Disease
    if 'MESH' in class_type_upper:
        return {'relaSource': 'MESH', 'trans': '1'}

    # SNOMED
    if 'SNOMED' in class_type_upper:
        return {'relaSource': 'SNOMEDCT', 'trans': '1'}

    # Schedule classifications
    if 'SCHEDULE' in class_type_upper:
        return {'relaSource': 'RXNORM', 'trans': '1'}

    # Default: try with class_type as relaSource
    return {'relaSource': class_type, 'trans': '1'}


def build_api_url(class_id, class_type):
    """Build RxNav classMembers API URL with appropriate parameters"""
    params = get_api_params(class_type)

    # Build query string
    query_params = {'classId': class_id}
    query_params.update(params)

    url = f"{api_base_url}?{urllib.parse.urlencode(query_params)}"
    return url


def process_partition(partition_rows):
    """
    Process a partition of rxclass rows - fetch drug members for each class.
    This function runs in parallel across Spark workers.

    Args:
        partition_rows: Iterator of Row objects from rxclass DataFrame

    Yields:
        Tuples of (class_id, rxcui, name, tty, source_id, rela_source, rela, meta_run_id)
    """
    import time

    partition_count = 0
    partition_success = 0
    partition_failure = 0

    for row in partition_rows:
        partition_count += 1
        class_id = row.class_id
        class_type = row.class_type

        # Get API params so we can store rela_source and rela with results
        api_params = get_api_params(class_type)

        # Build API URL
        url = build_api_url(class_id, class_type)

        # Fetch data
        data = fetch_json_with_retry(url)

        if data is None:
            partition_failure += 1
            continue

        # Extract drug members from response
        drug_member_group = data.get('drugMemberGroup', {})
        drug_members = drug_member_group.get('drugMember', [])

        if drug_members:
            for member in drug_members:
                min_concept = member.get('minConcept', {})

                # Extract source_id from nodeAttr
                source_id = None
                node_attrs = member.get('nodeAttr', [])
                for attr in node_attrs:
                    if attr.get('attrName') == 'SourceId':
                        source_id = attr.get('attrValue')
                        break

                # Only yield if rxcui exists
                rxcui = min_concept.get('rxcui')
                if rxcui:
                    yield (
                        class_id,
                        rxcui,
                        min_concept.get('name'),
                        min_concept.get('tty'),
                        source_id,
                        api_params.get('relaSource'),
                        api_params.get('rela'),
                        run_id
                    )

            partition_success += 1

        # Be respectful to API - delay between requests
        time.sleep(0.2)

    # Log partition completion
    print(f"Partition complete: processed {partition_count} classes (success: {partition_success}, failures: {partition_failure})")


try:
    # Step 1: Read rxclass bronze table
    print("Step 1: Reading rxclass bronze table from Glue catalog...")
    print(f"Database: {bronze_database}, Table: {rxclass_table}")

    # Load from Glue catalog using GlueContext (not spark.sql)
    rxclass_df = glueContext.create_dynamic_frame.from_catalog(
        database=bronze_database,
        table_name=rxclass_table
    ).toDF()

    # Filter for non-null class_ids
    rxclass_df = rxclass_df.filter(rxclass_df.class_id.isNotNull())

    rxclass_count = rxclass_df.count()
    print(f"Found {rxclass_count} classes to process across all class types")

    if rxclass_count == 0:
        raise Exception("No rxclass data found - cannot proceed")

    # Step 2: Fetch drug members using distributed processing (mapPartitions)
    print("Step 2: Fetching drug members using Spark distributed processing...")
    print(f"Processing {rxclass_count} classes across worker partitions...")

    # Use mapPartitions for parallel processing across Spark workers
    # Each partition processes its classes independently and yields drug member tuples
    drug_members_rdd = rxclass_df.rdd.mapPartitions(process_partition)

    # Convert RDD to DataFrame with explicit schema
    drug_member_schema = StructType([
        StructField("class_id", StringType(), True),
        StructField("rxcui", StringType(), True),
        StructField("name", StringType(), True),
        StructField("tty", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("rela_source", StringType(), True),
        StructField("rela", StringType(), True),
        StructField("meta_run_id", StringType(), True)
    ])

    df = spark.createDataFrame(drug_members_rdd, schema=drug_member_schema)

    drug_member_count = df.count()
    print(f"Total drug members collected: {drug_member_count}")

    # Save summary to raw layer
    summary = {
        "metadata": {
            "run_id": run_id,
            "collection_timestamp": datetime.now().isoformat(),
            "total_classes_available": rxclass_count,
            "total_drug_members": drug_member_count,
            "processing_mode": "distributed_mapPartitions"
        }
    }
    save_json_to_s3(summary, f"{raw_s3_path}collection_summary.json")

    # Step 3: Write to Bronze layer
    print("Step 3: Writing to Bronze layer...")

    if drug_member_count == 0:
        raise Exception("No drug member data collected - cannot proceed to Bronze layer")

    print(f"DataFrame has {drug_member_count} records")
    df.show(10, truncate=False)

    # Write to Bronze layer (kill-and-fill approach)
    print(f"Writing to Bronze layer: {bronze_s3_path}")

    df.write \
        .mode("overwrite") \
        .option("compression", args['compression_codec']) \
        .parquet(bronze_s3_path)

    print("Successfully wrote Bronze layer data")

    print("RxClass Drug Members ETL job completed successfully!")
    print(f"Total records processed: {drug_member_count}")
    print(f"Bronze data location: {bronze_s3_path}")
    print("Note: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"ERROR: RxClass Drug Members ETL job failed: {str(e)}")
    raise e

finally:
    job.commit()
