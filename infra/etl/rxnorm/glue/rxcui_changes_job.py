"""
RxCUI Changes Tracking Job
Processes RXNATOMARCHIVE and RXNREL to identify remapped and quantified RxCUIs
Based on legacy Rails logic for maintaining historical RxCUI mappings
"""
import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col, when, to_date, current_timestamp, row_number
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

print(f"Starting RxCUI Changes Tracking for {dataset}")
print(f"Bronze database: {bronze_database}")
print(f"Gold database: {gold_database}")
print(f"Run ID: {run_id}")

def process_rxcui_changes():
    """Process RxCUI changes from RXNATOMARCHIVE and RXNREL"""

    try:
        # Read required bronze tables
        print("Reading bronze tables...")

        # Check if tables exist before reading
        tables_to_check = [
            f"{bronze_database}.{dataset}_rxnatomarchive",
            f"{bronze_database}.{dataset}_rxnconso",
            f"{bronze_database}.{dataset}_rxnrel"
        ]

        for table_name in tables_to_check:
            try:
                spark.sql(f"DESCRIBE {table_name}")
                print(f"✓ Table exists: {table_name}")
            except Exception as e:
                print(f"✗ Table missing: {table_name} - {str(e)}")
                raise Exception(f"Required table {table_name} not found. Run bronze job first.")

        # Read tables
        rxnatomarchive_df = spark.table(f"{bronze_database}.{dataset}_rxnatomarchive")
        rxnconso_df = spark.table(f"{bronze_database}.{dataset}_rxnconso")
        rxnrel_df = spark.table(f"{bronze_database}.{dataset}_rxnrel")

        print(f"RXNATOMARCHIVE records: {rxnatomarchive_df.count()}")
        print(f"RXNCONSO records: {rxnconso_df.count()}")
        print(f"RXNREL records: {rxnrel_df.count()}")

        # 1. REMAPPED RxCUIs (from RXNATOMARCHIVE)
        print("Processing remapped RxCUIs...")

        remapped_rxcuis = rxnatomarchive_df.filter(
            (col("sab") == "RXNORM") &
            (col("tty").isin(["SCD", "SBD", "GPCK", "BPCK", "SCDF", "SBDF"])) &
            (col("rxcui") != col("merged_to_rxcui")) &
            (col("rxcui").isNotNull()) &
            (col("merged_to_rxcui").isNotNull()) &
            (to_date(col("archive_timestamp"), "yyyy/MM/dd") > lit("2016-01-01"))
        ).select(
            col("tty"),
            col("rxcui"),
            col("merged_to_rxcui").alias("changed_to_rxcui"),
            lit("Remapped").alias("status"),
            col("str").alias("drug_name"),
            col("archive_timestamp")
        ).distinct()

        remapped_count = remapped_rxcuis.count()
        print(f"Found {remapped_count} remapped RxCUIs")

        # 2. QUANTIFIED RxCUIs (from RXNCONSO + RXNREL)
        print("Processing quantified RxCUIs...")

        # Find suppressed RxCUIs with quantified forms
        suppressed_rxcuis = rxnconso_df.filter(
            (col("source") == "RXNORM") &
            (col("suppress") == "E") &
            (col("tty").isin(["SCD", "SBD"]))
        ).select("rxcui", "tty", "name")

        # Join with RXNREL to find quantified relationships
        quantified_rxcuis = suppressed_rxcuis.join(
            rxnrel_df.filter(col("rela") == "has_quantified_form"),
            suppressed_rxcuis["rxcui"] == rxnrel_df["rxcui2"],
            "inner"
        ).select(
            suppressed_rxcuis["tty"],
            suppressed_rxcuis["rxcui"],
            rxnrel_df["rxcui1"].alias("changed_to_rxcui"),
            lit("Quantified").alias("status"),
            suppressed_rxcuis["name"].alias("drug_name"),
            lit(None).alias("archive_timestamp")  # No timestamp for quantified
        ).distinct()

        quantified_count = quantified_rxcuis.count()
        print(f"Found {quantified_count} quantified RxCUIs")

        # 3. Combine all changes
        print("Combining RxCUI changes...")

        if remapped_count > 0 and quantified_count > 0:
            all_changes = remapped_rxcuis.union(quantified_rxcuis)
        elif remapped_count > 0:
            all_changes = remapped_rxcuis
        elif quantified_count > 0:
            all_changes = quantified_rxcuis
        else:
            print("No RxCUI changes found, creating empty DataFrame")
            # Create empty DataFrame with correct schema
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("tty", StringType(), True),
                StructField("rxcui", StringType(), True),
                StructField("changed_to_rxcui", StringType(), True),
                StructField("status", StringType(), True),
                StructField("drug_name", StringType(), True),
                StructField("archive_timestamp", StringType(), True)
            ])
            all_changes = spark.createDataFrame([], schema)

        # Add metadata
        rxcui_changes_df = all_changes.withColumn("meta_run_id", lit(run_id)) \
                                     .withColumn("created_at", current_timestamp()) \
                                     .withColumn("updated_at", current_timestamp())

        total_changes = rxcui_changes_df.count()
        print(f"Total RxCUI changes: {total_changes}")

        if total_changes > 0:
            print(f"RxCUI changes columns: {rxcui_changes_df.columns}")

            # Show sample of changes
            print("Sample RxCUI changes:")
            rxcui_changes_df.show(10, truncate=False)

        # 4. Write to GOLD layer
        gold_path = f"s3://{bucket_name}/gold/rxcui_changes/"
        print(f"Writing RxCUI changes to: {gold_path}")

        # Convert to DynamicFrame for writing
        from awsglue.dynamicframe import DynamicFrame
        changes_dynamic_frame = DynamicFrame.fromDF(
            rxcui_changes_df,
            glueContext,
            "rxcui_changes"
        )

        # Write to S3 (kill-and-fill)
        glueContext.write_dynamic_frame.from_options(
            frame=changes_dynamic_frame,
            connection_type="s3",
            connection_options={"path": gold_path},
            format="parquet",
            transformation_ctx="write_rxcui_changes"
        )

        print(f"Successfully wrote {total_changes} RxCUI changes to gold layer")

        # 5. Update catalog via crawler
        print("Starting RxCUI changes crawler...")
        glue_client = boto3.client('glue')
        crawler_name = f"pp-dw-gold-rxcui-changes-crawler"

        try:
            response = glue_client.start_crawler(Name=crawler_name)
            print(f"Crawler started successfully: {response}")
        except Exception as crawler_error:
            print(f"Warning: Could not start crawler {crawler_name}: {crawler_error}")
            print("Manual catalog update may be required")

        return rxcui_changes_df

    except Exception as e:
        print(f"Error processing RxCUI changes: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

try:
    # Process RxCUI changes
    changes_df = process_rxcui_changes()

    print("RxCUI Changes tracking completed successfully")
    print(f"Summary:")
    print(f"  - Total changes tracked: {changes_df.count()}")
    print(f"  - Run ID: {run_id}")

    # Show breakdown by status
    if changes_df.count() > 0:
        print("\nBreakdown by status:")
        changes_df.groupBy("status").count().show()

        print("\nBreakdown by TTY:")
        changes_df.groupBy("tty").count().show()

except Exception as e:
    print(f"RxCUI Changes job failed: {str(e)}")
    raise e

finally:
    job.commit()
    print("RxCUI Changes job completed")