"""
RxClass Drug Members ETL Job - Drug-First API Collection
Reads rxnorm_products silver table, fetches ALL class relationships for each drug via RxNav byRxcui API

Attribution: This product uses publicly available data from the U.S. National Library of Medicine (NLM),
National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the
product and does not endorse or recommend this or any other product.
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
    'JOB_NAME', 'dataset', 'bronze_database', 'silver_database', 'rxnorm_products_table',
    'api_base_url', 'raw_path', 'bronze_path', 'compression_codec'
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
silver_database = args['silver_database']
rxnorm_products_table = args['rxnorm_products_table']
api_base_url = args['api_base_url']

# Build S3 paths (raw includes run_id partition for lineage tracking)
raw_s3_path = build_raw_path_with_run_id(args['raw_path'], run_id)
bronze_s3_path = args['bronze_path']

print(f"Starting RxClass Drug Members ETL job (Drug-First Approach)")
print(f"Dataset: {dataset}")
print(f"Run ID: {run_id}")
print(f"Raw path: {raw_s3_path}")
print(f"Bronze path: {bronze_s3_path}")
print(f"Source table: {silver_database}.{rxnorm_products_table}")
print(f"API: {api_base_url}")
print(f"Attribution: Data from U.S. National Library of Medicine (NLM)")


def build_api_url(rxcui):
    """
    Build RxNav byRxcui API URL for a given drug
    Returns ALL class relationships for the drug in a single API call
    """
    url = f"{api_base_url}?rxcui={rxcui}"
    return url


def process_partition(partition_rows):
    """
    Process a partition of rxnorm_products rows - fetch ALL class relationships for each drug.
    This function runs in parallel across Spark workers.

    Args:
        partition_rows: Iterator of Row objects from rxnorm_products DataFrame

    Yields:
        Tuples of (product_rxcui, product_tty, minConcept_rxcui, minConcept_name, minConcept_tty,
                   rxclassMinConceptItem_classId, rxclassMinConceptItem_className,
                   rxclassMinConceptItem_classType, rela, relaSource, meta_run_id)
    """
    import time

    partition_count = 0
    partition_success = 0
    partition_failure = 0
    partition_relationships = 0

    for row in partition_rows:
        partition_count += 1
        product_rxcui = row.rxcui  # The product RXCUI we're querying
        product_tty = row.tty      # The product TTY (SCD, SBD, GPCK, BPCK)

        # Build API URL
        url = build_api_url(product_rxcui)

        # Fetch data
        data = fetch_json_with_retry(url)

        if data is None:
            partition_failure += 1
            # API rate limiting: 0.6s delay = 1.67 req/sec per worker
            # 10 workers × 1.67 = ~16.7 req/sec (under 20 req/sec limit)
            time.sleep(0.6)
            continue

        # Extract class relationships from response
        # API returns rxclassDrugInfoList.rxclassDrugInfo[] array with ALL relationships
        # NOTE: minConcept usually contains normalized ingredient (IN/MIN/PIN), not the product
        drug_info_list = data.get('rxclassDrugInfoList', {})
        drug_info_array = drug_info_list.get('rxclassDrugInfo', [])

        if drug_info_array:
            for relationship in drug_info_array:
                # Extract complete minConcept (normalized drug concept - usually ingredient)
                min_concept = relationship.get('minConcept', {})

                # Extract complete rxclassMinConceptItem (classification details)
                rxclass_item = relationship.get('rxclassMinConceptItem', {})

                # Extract relationship info
                rela = relationship.get('rela')
                rela_source = relationship.get('relaSource')

                # Only yield if we have both minConcept rxcui and class_id
                min_concept_rxcui = min_concept.get('rxcui')
                class_id = rxclass_item.get('classId')

                if min_concept_rxcui and class_id:
                    yield (
                        product_rxcui,                          # Product RXCUI we queried
                        product_tty,                            # Product TTY (SCD/SBD/GPCK/BPCK)
                        min_concept_rxcui,                      # minConcept.rxcui
                        min_concept.get('name'),                # minConcept.name
                        min_concept.get('tty'),                 # minConcept.tty
                        class_id,                               # rxclassMinConceptItem.classId
                        rxclass_item.get('className'),          # rxclassMinConceptItem.className
                        rxclass_item.get('classType'),          # rxclassMinConceptItem.classType
                        rela,                                   # rela
                        rela_source,                            # relaSource
                        run_id                                  # meta_run_id
                    )
                    partition_relationships += 1

            partition_success += 1

        # API rate limiting: 0.6s delay = 1.67 req/sec per worker
        # 10 workers × 1.67 = ~16.7 req/sec total (safe buffer under 20 req/sec limit)
        time.sleep(0.6)

    # Log partition completion
    print(f"Partition complete: processed {partition_count} drugs (success: {partition_success}, failures: {partition_failure}, relationships: {partition_relationships})")


try:
    # Step 1: Read rxnorm_products silver table
    print("Step 1: Reading rxnorm_products silver table from Glue catalog...")
    print(f"Database: {silver_database}, Table: {rxnorm_products_table}")

    # Load from Glue catalog using GlueContext
    products_df = glueContext.create_dynamic_frame.from_catalog(
        database=silver_database,
        table_name=rxnorm_products_table
    ).toDF()

    # Filter for non-null RXCUIs
    products_df = products_df.filter(products_df.rxcui.isNotNull())

    products_count = products_df.count()
    print(f"Found {products_count} prescribable products to process")

    if products_count == 0:
        raise Exception("No rxnorm_products data found - cannot proceed")

    # Step 2: Fetch class relationships using distributed processing (mapPartitions)
    print("Step 2: Fetching class relationships using Spark distributed processing...")
    print(f"Processing {products_count} drugs across 10 worker partitions...")
    print(f"API rate limiting: 0.6s delay per call = 1.67 req/sec per worker = ~16.7 req/sec total")

    # Use mapPartitions for parallel processing across Spark workers
    # Each partition processes its drugs independently and yields relationship tuples
    relationships_rdd = products_df.rdd.mapPartitions(process_partition)

    # Convert RDD to DataFrame with explicit schema
    relationship_schema = StructType([
        StructField("product_rxcui", StringType(), True),
        StructField("product_tty", StringType(), True),
        StructField("minConcept_rxcui", StringType(), True),
        StructField("minConcept_name", StringType(), True),
        StructField("minConcept_tty", StringType(), True),
        StructField("rxclassMinConceptItem_classId", StringType(), True),
        StructField("rxclassMinConceptItem_className", StringType(), True),
        StructField("rxclassMinConceptItem_classType", StringType(), True),
        StructField("rela", StringType(), True),
        StructField("relaSource", StringType(), True),
        StructField("meta_run_id", StringType(), True)
    ])

    df = spark.createDataFrame(relationships_rdd, schema=relationship_schema)

    relationship_count = df.count()
    print(f"Total class relationships collected: {relationship_count}")

    # Save summary to raw layer
    summary = {
        "metadata": {
            "run_id": run_id,
            "collection_timestamp": datetime.now().isoformat(),
            "total_products_available": products_count,
            "total_class_relationships": relationship_count,
            "processing_mode": "distributed_mapPartitions_drug_first",
            "api_rate_limiting": "0.6s delay per call, ~16.7 req/sec total across 10 workers"
        }
    }
    save_json_to_s3(summary, f"{raw_s3_path}collection_summary.json")

    # Step 3: Write to Bronze layer
    print("Step 3: Writing to Bronze layer...")

    if relationship_count == 0:
        raise Exception("No class relationship data collected - cannot proceed to Bronze layer")

    print(f"DataFrame has {relationship_count} records")
    df.show(10, truncate=False)

    # Write to Bronze layer (kill-and-fill approach)
    print(f"Writing to Bronze layer: {bronze_s3_path}")

    df.write \
        .mode("overwrite") \
        .option("compression", args['compression_codec']) \
        .parquet(bronze_s3_path)

    print("Successfully wrote Bronze layer data")

    print("RxClass Drug Members ETL job completed successfully!")
    print(f"Total relationships processed: {relationship_count}")
    print(f"Total drugs processed: {products_count}")
    print(f"Average relationships per drug: {relationship_count / products_count:.1f}")
    print(f"Bronze data location: {bronze_s3_path}")
    print("Note: Run crawler manually via console if schema changes are needed")

except Exception as e:
    print(f"ERROR: RxClass Drug Members ETL job failed: {str(e)}")
    raise e

finally:
    job.commit()
