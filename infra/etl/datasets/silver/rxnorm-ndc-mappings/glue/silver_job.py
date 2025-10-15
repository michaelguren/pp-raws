"""
Silver RxNORM NDC Mapping Job - Extract and validate NDC mappings from RXNSAT
Reads bronze RXNSAT and RXNCONSO tables, filters for NDC attributes, validates formats
"""
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]
from pyspark.sql.types import IntegerType  # type: ignore[import-not-found]

# ============================================================================
# INITIALIZATION
# ============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'bronze_database', 'silver_database', 'silver_path',
    'compression_codec', 'dataset'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])

# ============================================================================
# CONFIGURATION
# ============================================================================

dataset = args['dataset']
bronze_database = args['bronze_database']
silver_database = args['silver_database']
silver_path = args['silver_path']
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Starting RxNORM NDC Mapping Silver ETL")
print(f"  Dataset: {dataset}")
print(f"  Bronze DB: {bronze_database}")
print(f"  Silver DB: {silver_database}")
print(f"  Output: {silver_path}")
print(f"  Run ID: {run_id}")

# ============================================================================
# DATA EXTRACTION
# ============================================================================

print("\n[1/4] Reading bronze tables...")

# Read RXNSAT (contains NDC mappings)
rxnsat_df = glueContext.create_dynamic_frame.from_catalog(
    database=bronze_database,
    table_name="rxnsat"
).toDF()
print(f"  RXNSAT: {rxnsat_df.count():,} records")

# Read RXNCONSO (for drug names and term types)
rxnconso_df = glueContext.create_dynamic_frame.from_catalog(
    database=bronze_database,
    table_name="rxnconso"
).toDF()
print(f"  RXNCONSO: {rxnconso_df.count():,} records")

# ============================================================================
# EXTRACT NDC MAPPINGS
# ============================================================================

print("\n[2/4] Extracting NDC mappings from RXNSAT...")

# Filter for NDC attributes only
ndc_mappings = rxnsat_df.filter(F.col("ATN") == "NDC").select(
    F.col("RXCUI").alias("rxcui"),
    F.col("ATV").alias("ndc_raw"),
    F.col("meta_run_id"),
    F.col("meta_release_date")
)

initial_count = ndc_mappings.count()
print(f"  Extracted {initial_count:,} NDC mappings")

# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

print("\n[3/4] Validating NDC formats...")

# Add validation columns (QA checks without transformation)
ndc_mappings = ndc_mappings.withColumn(
    "ndc_length", F.length(F.col("ndc_raw"))
).withColumn(
    "ndc_is_numeric", F.col("ndc_raw").rlike("^[0-9]+$")
).withColumn(
    "ndc_is_valid",
    (F.col("ndc_length") == 12) & (F.col("ndc_is_numeric"))
)

# Print validation stats
valid_count = ndc_mappings.filter(F.col("ndc_is_valid")).count()
invalid_count = initial_count - valid_count
valid_pct = (valid_count / initial_count * 100) if initial_count > 0 else 0

print(f"  Valid NDCs (12 digits, numeric): {valid_count:,} ({valid_pct:.1f}%)")
print(f"  Invalid NDCs: {invalid_count:,} ({100-valid_pct:.1f}%)")

if invalid_count > 0:
    print("\n  Sample invalid NDCs:")
    ndc_mappings.filter(~F.col("ndc_is_valid")).select(
        "rxcui", "ndc_raw", "ndc_length", "ndc_is_numeric"
    ).show(10, truncate=False)

# Keep only valid NDCs for silver layer
ndc_mappings = ndc_mappings.filter(F.col("ndc_is_valid"))

# Convert 12-digit to 11-digit format (remove leading zero if present)
# This matches FDA's 11-digit format: 5-4-2 (labeler-product-package)
ndc_mappings = ndc_mappings.withColumn(
    "ndc_11",
    F.when(F.col("ndc_raw").startswith("0"), F.substring(F.col("ndc_raw"), 2, 11))
     .otherwise(F.col("ndc_raw"))
)

# ============================================================================
# ENRICH WITH DRUG NAMES
# ============================================================================

print("\n[4/4] Enriching with drug names from RXNCONSO...")

# Get preferred English names for each RXCUI
rxnconso_preferred = rxnconso_df.filter(
    (F.col("LAT") == "ENG") &
    (F.col("ISPREF") == "Y")
).select(
    F.col("RXCUI").alias("rxcui_conso"),
    F.col("STR").alias("str"),
    F.col("TTY").alias("tty")
)

# Join with mappings
ndc_mappings_enriched = ndc_mappings.join(
    rxnconso_preferred,
    ndc_mappings.rxcui == rxnconso_preferred.rxcui_conso,
    "left"
).select(
    ndc_mappings.rxcui,
    ndc_mappings.ndc_raw,
    ndc_mappings.ndc_11,
    rxnconso_preferred.str,
    rxnconso_preferred.tty,
    F.col("ndc_length").alias("qa_ndc_length"),
    F.col("ndc_is_numeric").alias("qa_ndc_is_numeric"),
    ndc_mappings.meta_run_id,
    ndc_mappings.meta_release_date
)

final_count = ndc_mappings_enriched.count()
enriched_count = ndc_mappings_enriched.filter(F.col("str").isNotNull()).count()
enriched_pct = (enriched_count / final_count * 100) if final_count > 0 else 0

print(f"  Total mappings: {final_count:,}")
print(f"  Enriched with names: {enriched_count:,} ({enriched_pct:.1f}%)")
print(f"  Missing names: {final_count - enriched_count:,} ({100-enriched_pct:.1f}%)")

# ============================================================================
# WRITE TO SILVER
# ============================================================================

print(f"\nWriting to silver layer: {silver_path}")

ndc_mappings_enriched.write.mode("overwrite").option(
    "compression", args['compression_codec']
).parquet(silver_path)

# ============================================================================
# SUMMARY
# ============================================================================

print(f"\n✓ Silver RxNORM NDC Mapping ETL completed")
print(f"  Input records (RXNSAT): {initial_count:,}")
print(f"  Valid NDCs (12-digit): {valid_count:,}")
print(f"  Invalid NDCs filtered: {invalid_count:,}")
print(f"  Final silver records: {final_count:,}")
print(f"  Enriched with drug names: {enriched_count:,}")
print(f"\nSample output:")
ndc_mappings_enriched.show(10, truncate=False)

print("\nNote: Run crawler to update Glue catalog")

job.commit()
