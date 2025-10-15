"""
Silver RxNORM NDC Mapping Job - Extract and validate NDC mappings from RXNSAT

Reads bronze RXNSAT table and silver rxnorm_products table.
Filters for SAB='RXNORM' AND CVF='4096' NDC attributes:
- SAB='RXNORM': NLM-asserted, normalized 11-digit NDCs in HIPAA format (Section 6.0)
- CVF='4096': Current Prescribable Content subset (prescribable drugs only)
Other sources (CVX, GS, MMSL, MMX, MTHSPL, NDDF, VANDF) have various formats and are excluded.

Enriches with drug names from rxnorm_products (prescribable products only: SCD, SBD, GPCK, BPCK).

Output: Clean rxcui-to-ndc_11 mapping table for gold layer temporal versioning
"""
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]

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

print("\n[1/4] Reading source tables...")

# Read RXNSAT (contains NDC mappings)
rxnsat_df = glueContext.create_dynamic_frame.from_catalog(
    database=bronze_database,
    table_name="rxnsat"
).toDF()
print(f"  RXNSAT: {rxnsat_df.count():,} records")

# Read rxnorm_products silver table (already filtered to prescribable products)
rxnorm_products_df = glueContext.create_dynamic_frame.from_catalog(
    database=silver_database,
    table_name="rxnorm_products"
).toDF()
print(f"  rxnorm_products: {rxnorm_products_df.count():,} records")

# ============================================================================
# EXTRACT NDC MAPPINGS
# ============================================================================

print("\n[2/4] Extracting NDC mappings from RXNSAT...")

# Filter for NDC attributes from SAB='RXNORM' with CVF='4096' (prescribable only)
# Per RxNORM documentation:
# - Section 6.0: SAB='RXNORM' provides NLM-asserted, normalized 11-digit NDCs in HIPAA format
# - CVF='4096': Current Prescribable Content subset (prescribable drugs only)
# Other sources (CVX, GS, MMSL, MMX, MTHSPL, NDDF, VANDF) have various formats and are excluded.
ndc_mappings = rxnsat_df.filter(
    (F.col("ATN") == "NDC") &
    (F.col("SAB") == "RXNORM") &
    (F.col("CVF") == "4096")
).select(
    F.col("RXCUI").alias("rxcui"),
    F.col("ATV").alias("ndc_11"),  # Already 11-digit format from RXNORM
    F.col("meta_run_id"),
    F.col("meta_release_date")
)

initial_count = ndc_mappings.count()
print(f"  Extracted {initial_count:,} prescribable NDC mappings (SAB=RXNORM, CVF=4096)")

# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

print("\n[3/4] Validating NDC formats...")

# Add validation columns (QA checks)
# RXNORM SAB should already provide 11-digit HIPAA format
ndc_mappings = ndc_mappings.withColumn(
    "ndc_length", F.length(F.col("ndc_11"))
).withColumn(
    "ndc_is_numeric", F.col("ndc_11").rlike("^[0-9]+$")
).withColumn(
    "ndc_is_valid",
    (F.col("ndc_length") == 11) & (F.col("ndc_is_numeric"))
)

# Print validation stats
valid_count = ndc_mappings.filter(F.col("ndc_is_valid")).count()
invalid_count = initial_count - valid_count
valid_pct = (valid_count / initial_count * 100) if initial_count > 0 else 0

print(f"  Valid NDCs (11 digits, numeric): {valid_count:,} ({valid_pct:.1f}%)")
print(f"  Invalid NDCs: {invalid_count:,} ({100-valid_pct:.1f}%)")

if invalid_count > 0:
    print("\n  Sample invalid NDCs:")
    ndc_mappings.filter(~F.col("ndc_is_valid")).select(
        "rxcui", "ndc_11", "ndc_length", "ndc_is_numeric"
    ).show(10, truncate=False)

# Keep only valid NDCs for silver layer
ndc_mappings = ndc_mappings.filter(F.col("ndc_is_valid"))

# ============================================================================
# ENRICH WITH DRUG NAMES
# ============================================================================

print("\n[4/4] Enriching with drug names from rxnorm_products...")

# Join with rxnorm_products to get drug names (already filtered to SCD, SBD, GPCK, BPCK)
ndc_mappings_enriched = ndc_mappings.join(
    rxnorm_products_df.select(
        F.col("rxnorm_rxcui").alias("product_rxcui"),
        F.col("rxnorm_str").alias("str"),
        F.col("rxnorm_tty").alias("tty")
    ),
    ndc_mappings.rxcui == F.col("product_rxcui"),
    "left"
).select(
    ndc_mappings.rxcui,
    ndc_mappings.ndc_11,
    F.col("str"),
    F.col("tty"),
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
print(f"  Input records (SAB=RXNORM, CVF=4096): {initial_count:,}")
print(f"  Valid NDCs (11-digit HIPAA format): {valid_count:,}")
print(f"  Invalid NDCs filtered: {invalid_count:,}")
print(f"  Final silver records: {final_count:,}")
print(f"  Enriched with drug names: {enriched_count:,}")
print(f"\nSample output:")
ndc_mappings_enriched.show(10, truncate=False)

print("\nNote: Run crawler to update Glue catalog")

job.commit()
