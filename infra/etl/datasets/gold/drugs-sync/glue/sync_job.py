"""
Gold → DynamoDB Sync Job
Syncs drug_product_codesets from gold layer to DynamoDB operational table

Writes two entity types:
1. Drug records: PK=DRUG#{id}, SK=METADATA#latest
2. Search tokens: PK=SEARCH#DRUG, SK=token#{word}#{field}#DRUG#{id}

Following RAWS principles:
- Kill-and-fill: Delete old data, write fresh data
- Tokenization: Uniform processing across all fields
- Batch writes: Parallel batch_writer with foreachPartition
"""

import sys
import re
from datetime import datetime, timezone
from pyspark.sql import functions as F  # type: ignore[import-not-found]
from pyspark.sql.types import ArrayType, StringType  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]

# ========================
# Initialize Glue Context
# ========================

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TABLE_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

TABLE_NAME = args['TABLE_NAME']

# Generate RUN_ID inside the job (runtime, not deploy-time)
RUN_ID = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

logger.info(f"Starting Gold → DynamoDB sync job")
logger.info(f"Target table: {TABLE_NAME}")
logger.info(f"Run ID: {RUN_ID}")

# ========================
# Kill-and-Fill: Delete Old Search Tokens
# ========================

logger.info("Kill-and-fill: Deleting old search tokens (PK=SEARCH#DRUG)")

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

# Query all search tokens
deleted_count = 0
try:
    # Query items with PK=SEARCH#DRUG
    response = table.query(
        KeyConditionExpression=Key('PK').eq('SEARCH#DRUG'),
        ProjectionExpression='PK, SK',
        Limit=1000  # Test query to check if search tokens exist
    )

    if response['Items']:
        logger.info(f"Found {len(response['Items'])} search tokens in first batch, deleting all...")

        # Delete in batches
        while True:
            response = table.query(
                KeyConditionExpression=Key('PK').eq('SEARCH#DRUG'),
                ProjectionExpression='PK, SK'
            )

            items = response['Items']
            if not items:
                break

            # Batch delete
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={'PK': item['PK'], 'SK': item['SK']})
                    deleted_count += 1

            logger.info(f"Deleted {deleted_count} search tokens so far...")

            # Check for more items
            if 'LastEvaluatedKey' not in response:
                break
    else:
        logger.info("No existing search tokens found (first run or empty table)")

except Exception as e:
    logger.warning(f"Error during search token deletion (may be first run): {e}")

logger.info(f"Total search tokens deleted: {deleted_count}")

# ========================
# Tokenization Logic
# ========================

# Stop words (60+ common English words)
STOP_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
    'for', 'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are',
    'were', 'been', 'be', 'have', 'has', 'had', 'do', 'does', 'did',
    'will', 'would', 'should', 'could', 'may', 'might', 'must', 'can',
    'that', 'this', 'these', 'those', 'which', 'what', 'who', 'when',
    'where', 'why', 'how', 'all', 'each', 'every', 'both', 'few',
    'more', 'most', 'other', 'some', 'such', 'than', 'too', 'very'
}

def tokenize_field(text):
    """
    Split on space/slash/pipe, filter by length/stop words.
    All fields treated uniformly - no special cases.

    Rules:
    - Split on: space, slash, pipe
    - Normalize: lowercase, trim
    - Filter: min 3 chars, remove stop words
    - Keep: digits (e.g., "500" is valid)
    """
    if not text:
        return []

    return [
        token for token in
        (t.strip().lower() for t in re.split(r'[\s/|]+', text))
        if len(token) >= 3
        and token not in STOP_WORDS
    ]

# Register as Spark UDF
tokenize_udf = F.udf(tokenize_field, ArrayType(StringType()))

# ========================
# Read Gold Layer
# ========================

logger.info("Reading gold layer: pp_dw_gold.drug_product_codesets")

# Use GlueContext to read from catalog (not spark.read.table)
gold_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="pp_dw_gold",
    table_name="drug_product_codesets"
)

# Convert to DataFrame for PySpark operations
gold_df = gold_dynamic_frame.toDF()

initial_count = gold_df.count()
logger.info(f"Gold layer record count: {initial_count}")

# Cache to avoid redundant scans
gold_df.cache()

# ========================
# Searchable Fields Mapping
# ========================

searchable_fields = {
    "ndc": "fda_ndc_11",
    "proprietary_name": "fda_proprietary_name",
    "rxcui": "rxnorm_rxcui",
    "rxnorm_str": "rxnorm_str",
    "ingredient": "rxnorm_ingredient_names",
    "brand": "rxnorm_brand_names"
}

# ========================
# Apply Tokenization
# ========================

logger.info("Applying tokenization to searchable fields")

for field_name, col_name in searchable_fields.items():
    gold_df = gold_df.withColumn(f"{field_name}_tokens", tokenize_udf(F.col(col_name)))

# Add entity_id (prefer FDA NDC, fallback to RxNORM RxCUI)
gold_df = gold_df.withColumn("entity_id", F.coalesce(F.col("fda_ndc_11"), F.col("rxnorm_rxcui")))

# Filter out records with no ID (shouldn't happen, but defensive)
gold_df = gold_df.filter(F.col("entity_id").isNotNull())

# ========================
# Explode Tokens for Search Index
# ========================

logger.info("Exploding tokens for search index")

exploded_dfs = []

for field_name in searchable_fields.keys():
    token_col = f"{field_name}_tokens"
    exploded_dfs.append(
        gold_df.select(
            F.col("entity_id"),
            F.lit(field_name).alias("field"),
            F.explode_outer(F.col(token_col)).alias("token")
        )
    )

# Union all exploded dataframes
tokens_df = exploded_dfs[0]
for next_df in exploded_dfs[1:]:
    tokens_df = tokens_df.unionByName(next_df)

# Filter nulls and deduplicate
tokens_df = tokens_df.filter(F.col("token").isNotNull()).distinct()

# Cache tokens for reuse
tokens_df.cache()

token_count = tokens_df.count()
logger.info(f"Total unique search tokens: {token_count}")

# ========================
# Format Search Tokens for DynamoDB
# ========================

logger.info("Formatting search tokens for DynamoDB")

search_tokens_df = (
    tokens_df
    .withColumn("PK", F.lit("SEARCH#DRUG"))
    .withColumn("SK", F.concat_ws("#",
                                  F.lit("token"),
                                  F.col("token"),
                                  F.col("field"),
                                  F.lit("DRUG"),
                                  F.col("entity_id")))
    .select("PK", "SK")
)

# ========================
# Format Drug Records for DynamoDB
# ========================

logger.info("Formatting drug records for DynamoDB")

# Select all fields and format for DynamoDB
# Use SK=METADATA#latest for kill-and-fill semantics (always overwrites)
drug_records_df = (
    gold_df
    .withColumn("PK", F.concat(F.lit("DRUG#"), F.col("entity_id")))
    .withColumn("SK", F.lit("METADATA#latest"))
    .withColumn("meta_sync_timestamp", F.current_timestamp())
    .drop("entity_id")  # Remove temp column
    # Drop token columns (temp processing artifacts)
    .drop(*[f"{field}_tokens" for field in searchable_fields.keys()])
)

# Convert DataFrame to format suitable for DynamoDB
# Keep all original gold layer columns + sync metadata
drug_records_for_dynamo = drug_records_df.select(
    "PK", "SK",
    # FDA fields
    "fda_ndc_11", "fda_ndc_5", "fda_marketing_category", "fda_product_type",
    "fda_proprietary_name", "fda_dosage_form", "fda_application_number",
    "fda_dea_schedule", "fda_package_description", "fda_active_numerator_strength",
    "fda_active_ingredient_unit", "fda_spl_id", "fda_marketing_start_date",
    "fda_marketing_end_date", "fda_billing_unit", "fda_nsde_flag",
    # RxNORM fields
    "rxnorm_rxcui", "rxnorm_tty", "rxnorm_str", "rxnorm_strength",
    "rxnorm_ingredient_names", "rxnorm_brand_names", "rxnorm_dosage_forms",
    "rxnorm_psn", "rxnorm_sbdf_rxcui", "rxnorm_sbdf_name",
    "rxnorm_scdf_rxcui", "rxnorm_scdf_name", "rxnorm_sbd_rxcui",
    "rxnorm_bpck_rxcui", "rxnorm_multi_ingredient",
    # Metadata
    "meta_run_id", "meta_sync_timestamp"
)

# ========================
# Batch Write to DynamoDB
# ========================

def batch_write_partition(partition_items):
    """
    Write partition to DynamoDB with error handling.
    Uses batch_writer for efficient batch operations (25 items per API call).
    """
    import boto3
    from botocore.exceptions import ClientError

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    success_count = 0
    error_count = 0

    with table.batch_writer(overwrite_by_pkeys=['PK', 'SK']) as batch:
        for row in partition_items:
            try:
                # Convert row to dict, handling None values
                item = {k: v for k, v in row.asDict().items() if v is not None}
                batch.put_item(Item=item)
                success_count += 1
            except ClientError as e:
                error_count += 1
                print(f"Write failed for {row['PK']}: {e}")

    return [(success_count, error_count)]

# ========================
# Write Search Tokens
# ========================

logger.info("Writing search tokens to DynamoDB")

# Partition sizing: ~5K items per partition for optimal batch writes
num_partitions_tokens = max(4, int(token_count / 5000))
logger.info(f"Using {num_partitions_tokens} partitions for search tokens")

write_stats = (
    search_tokens_df
    .repartition(num_partitions_tokens)
    .rdd.mapPartitions(batch_write_partition)
    .collect()
)

total_tokens_written = sum(s[0] for s in write_stats)
total_tokens_failed = sum(s[1] for s in write_stats)

logger.info(f"Search tokens written: {total_tokens_written}")
logger.info(f"Search tokens failed: {total_tokens_failed}")

# ========================
# Write Drug Records
# ========================

logger.info("Writing drug records to DynamoDB")

num_partitions_drugs = max(4, int(initial_count / 5000))
logger.info(f"Using {num_partitions_drugs} partitions for drug records")

write_stats = (
    drug_records_for_dynamo
    .repartition(num_partitions_drugs)
    .rdd.mapPartitions(batch_write_partition)
    .collect()
)

total_drugs_written = sum(s[0] for s in write_stats)
total_drugs_failed = sum(s[1] for s in write_stats)

logger.info(f"Drug records written: {total_drugs_written}")
logger.info(f"Drug records failed: {total_drugs_failed}")

# ========================
# Final Summary
# ========================

logger.info("=" * 80)
logger.info("SYNC JOB COMPLETE")
logger.info(f"Gold layer records: {initial_count}")
logger.info(f"Old search tokens deleted: {deleted_count}")
logger.info(f"Drug records written: {total_drugs_written}")
logger.info(f"Search tokens written: {total_tokens_written}")
logger.info(f"Total failures: {total_drugs_failed + total_tokens_failed}")
logger.info("=" * 80)

# Fail the job if we had significant failures
if total_drugs_failed > 0 or total_tokens_failed > (total_tokens_written * 0.01):
    raise Exception(f"Sync job had excessive failures: {total_drugs_failed + total_tokens_failed} items failed")

# Commit the Glue job
job.commit()
logger.info("Glue job committed successfully")
