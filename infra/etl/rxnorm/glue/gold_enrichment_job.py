"""
RxNORM GOLD Enrichment Job - NDC to RxCUI/TTY Mapping
Creates optimized mapping table for FDA data enrichment with RxCUI and TTY
Handles current mappings and historical changes (remapped/quantified RxCUIs)
"""
import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, when, row_number, desc, length, regexp_replace, upper
from pyspark.sql.window import Window
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'bucket_name', 'dataset',
    'bronze_database', 'gold_database', 'compression_codec'
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
bucket_name = args['bucket_name']
dataset = args['dataset']
bronze_database = args['bronze_database']
gold_database = args['gold_database']

print(f"Starting RxNORM GOLD Enrichment for {dataset}")
print(f"Bronze database: {bronze_database}")
print(f"Gold database: {gold_database}")
print(f"Run ID: {run_id}")

def normalize_ndc(ndc_value):
    """Normalize NDC to 11-digit format"""
    # Remove any non-numeric characters
    clean_ndc = regexp_replace(ndc_value, "[^0-9]", "")

    # Pad to 11 digits if shorter
    return when(length(clean_ndc) == 10,
                regexp_replace(clean_ndc, "^(.{4})(.{4})(.{2})$", "0$1$2$3"))  # 4-4-2 format
           .when(length(clean_ndc) == 11, clean_ndc)  # Already 11 digits
           .when(length(clean_ndc) == 9,
                 regexp_replace(clean_ndc, "^(.{4})(.{3})(.{2})$", "0$1$2$3"))   # 4-3-2 format
           .when(length(clean_ndc) < 11,
                 regexp_replace(clean_ndc, "^(.*)$", "00000000000"))  # Pad with zeros if too short
           .otherwise(clean_ndc)  # Leave as-is if longer than 11

def create_rxnorm_ndc_mapping():
    """Create comprehensive NDC to RxCUI/TTY mapping with historical changes"""

    try:
        # Read required tables
        print("Reading bronze tables...")

        # Check if tables exist
        tables_to_check = [
            f"{bronze_database}.{dataset}_rxnsat",
            f"{bronze_database}.{dataset}_rxnconso"
        ]

        for table_name in tables_to_check:
            try:
                spark.sql(f"DESCRIBE {table_name}")
                print(f"✓ Table exists: {table_name}")
            except Exception as e:
                print(f"✗ Table missing: {table_name} - {str(e)}")
                raise Exception(f"Required table {table_name} not found. Run bronze job first.")

        # Read tables
        rxnsat_df = spark.table(f"{bronze_database}.{dataset}_rxnsat")
        rxnconso_df = spark.table(f"{bronze_database}.{dataset}_rxnconso")

        print(f"RXNSAT records: {rxnsat_df.count()}")
        print(f"RXNCONSO records: {rxnconso_df.count()}")

        # Try to read RxCUI changes (optional)
        rxcui_changes_df = None
        try:
            rxcui_changes_df = spark.table(f"{gold_database}.rxcui_changes")
            print(f"RxCUI changes records: {rxcui_changes_df.count()}")
        except Exception as e:
            print(f"RxCUI changes table not found: {e}")
            print("Proceeding without historical changes")

        # 1. Extract NDC mappings from RXNSAT (based on Rails logic)
        print("Extracting NDC mappings from RXNSAT...")

        ndc_mappings_raw = rxnsat_df.filter(
            (col("source") == "RXNORM") &
            (col("attribute_name") == "NDC") &
            (col("rxcui").isNotNull()) &
            (col("attribute_value").isNotNull())
        ).select(
            "rxcui",
            col("attribute_value").alias("ndc_raw")
        )

        # Normalize NDC values to 11-digit format
        ndc_mappings = ndc_mappings_raw.withColumn(
            "ndc_11",
            normalize_ndc(col("ndc_raw"))
        ).filter(
            # Only keep valid 11-digit NDCs
            length(col("ndc_11")) == 11
        ).select("rxcui", "ndc_11").distinct()

        ndc_count = ndc_mappings.count()
        print(f"Found {ndc_count} NDC to RxCUI mappings")

        if ndc_count == 0:
            print("Warning: No NDC mappings found in RXNSAT")

        # 2. Get TTY information from RXNCONSO
        print("Getting TTY information from RXNCONSO...")

        rxnconso_tty = rxnconso_df.filter(
            (col("source") == "RXNORM") &
            (col("rxcui").isNotNull()) &
            (col("tty").isNotNull())
        ).select(
            "rxcui",
            "tty",
            "name",
            "suppress"
        ).distinct()

        # Prioritize non-suppressed entries, then by TTY preference
        tty_priority = rxnconso_tty.withColumn(
            "priority",
            when(col("suppress") == "N", 1).otherwise(2)  # Non-suppressed first
        ).withColumn(
            "tty_rank",
            when(col("tty") == "SCD", 1)      # Semantic Clinical Drug
            .when(col("tty") == "SBD", 2)     # Semantic Branded Drug
            .when(col("tty") == "GPCK", 3)    # Generic Pack
            .when(col("tty") == "BPCK", 4)    # Branded Pack
            .otherwise(5)                     # Other TTYs
        )

        # Get best TTY per RxCUI
        window_spec = Window.partitionBy("rxcui").orderBy("priority", "tty_rank")
        best_tty = tty_priority.withColumn(
            "row_num",
            row_number().over(window_spec)
        ).filter(col("row_num") == 1).select(
            "rxcui", "tty", "name", "suppress"
        )

        print(f"Found TTY information for {best_tty.count()} RxCUIs")

        # 3. Create primary mapping (current RxCUIs)
        print("Creating primary NDC mapping...")

        primary_mapping = ndc_mappings.join(
            best_tty,
            "rxcui",
            "left"
        ).select(
            "ndc_11",
            "rxcui",
            "tty",
            "name",
            "suppress",
            lit("Current").alias("mapping_status"),
            lit(None).alias("changed_from_rxcui")
        )

        primary_count = primary_mapping.count()
        print(f"Primary mapping records: {primary_count}")

        # 4. Add historical mappings if RxCUI changes available
        final_mapping = primary_mapping

        if rxcui_changes_df is not None and rxcui_changes_df.count() > 0:
            print("Adding historical mappings from RxCUI changes...")

            # Create historical mappings for changed RxCUIs
            historical_mapping = ndc_mappings.alias("ndc") \
                .join(
                    rxcui_changes_df.alias("changes"),
                    col("ndc.rxcui") == col("changes.rxcui"),
                    "inner"
                ).join(
                    best_tty.alias("new_tty"),
                    col("changes.changed_to_rxcui") == col("new_tty.rxcui"),
                    "left"
                ).select(
                    col("ndc.ndc_11"),
                    col("changes.changed_to_rxcui").alias("rxcui"),
                    col("new_tty.tty"),
                    col("new_tty.name"),
                    col("new_tty.suppress"),
                    col("changes.status").alias("mapping_status"),
                    col("changes.rxcui").alias("changed_from_rxcui")
                )

            historical_count = historical_mapping.count()
            print(f"Historical mapping records: {historical_count}")

            if historical_count > 0:
                # Combine current and historical mappings
                all_mappings = primary_mapping.union(historical_mapping)

                # Deduplicate, preferring current mappings over historical
                window_dedup = Window.partitionBy("ndc_11").orderBy(
                    when(col("mapping_status") == "Current", 1)
                    .when(col("mapping_status") == "Remapped", 2)
                    .when(col("mapping_status") == "Quantified", 3)
                    .otherwise(4)
                )

                final_mapping = all_mappings.withColumn(
                    "row_num",
                    row_number().over(window_dedup)
                ).filter(col("row_num") == 1).drop("row_num")

        # 5. Add metadata and final processing
        final_mapping_with_meta = final_mapping.withColumn("meta_run_id", lit(run_id)) \
                                              .withColumn("created_at", lit(datetime.now().isoformat()))

        final_count = final_mapping_with_meta.count()
        print(f"Final mapping records: {final_count}")

        if final_count > 0:
            print(f"Final mapping columns: {final_mapping_with_meta.columns}")

            # Show sample mappings
            print("Sample NDC to RxCUI mappings:")
            final_mapping_with_meta.show(10, truncate=False)

            # Show breakdown by mapping status
            print("Breakdown by mapping status:")
            final_mapping_with_meta.groupBy("mapping_status").count().show()

            # Show breakdown by TTY
            print("Breakdown by TTY:")
            final_mapping_with_meta.groupBy("tty").count().show()

        # 6. Write to GOLD layer
        gold_path = f"s3://{bucket_name}/gold/rxnorm_ndc_mapping/"
        print(f"Writing NDC mapping to: {gold_path}")

        # Convert to DynamicFrame for writing
        from awsglue.dynamicframe import DynamicFrame
        mapping_dynamic_frame = DynamicFrame.fromDF(
            final_mapping_with_meta,
            glueContext,
            "rxnorm_ndc_mapping"
        )

        # Write to S3 (kill-and-fill)
        glueContext.write_dynamic_frame.from_options(
            frame=mapping_dynamic_frame,
            connection_type="s3",
            connection_options={"path": gold_path},
            format="parquet",
            transformation_ctx="write_rxnorm_ndc_mapping"
        )

        print(f"Successfully wrote {final_count} NDC mappings to gold layer")

        # 7. Update catalog via crawler
        print("Starting NDC mapping crawler...")
        glue_client = boto3.client('glue')
        crawler_name = f"pp-dw-gold-rxnorm-ndc-mapping-crawler"

        try:
            response = glue_client.start_crawler(Name=crawler_name)
            print(f"Crawler started successfully: {response}")
        except Exception as crawler_error:
            print(f"Warning: Could not start crawler {crawler_name}: {crawler_error}")
            print("Manual catalog update may be required")

        return final_mapping_with_meta

    except Exception as e:
        print(f"Error creating NDC mapping: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

try:
    # Create NDC to RxCUI/TTY mapping
    mapping_df = create_rxnorm_ndc_mapping()

    print("RxNORM GOLD Enrichment completed successfully")
    print(f"Summary:")
    print(f"  - NDC to RxCUI mappings created: {mapping_df.count()}")
    print(f"  - Run ID: {run_id}")

    if mapping_df.count() > 0:
        # Calculate some useful statistics
        total_ndcs = mapping_df.select("ndc_11").distinct().count()
        total_rxcuis = mapping_df.select("rxcui").distinct().count()

        print(f"  - Unique NDCs mapped: {total_ndcs}")
        print(f"  - Unique RxCUIs: {total_rxcuis}")

        # Show TTY distribution
        print("\nTTY Distribution:")
        mapping_df.groupBy("tty").count().orderBy(desc("count")).show()

except Exception as e:
    print(f"RxNORM GOLD Enrichment job failed: {str(e)}")
    raise e

finally:
    job.commit()
    print("RxNORM GOLD Enrichment job completed")