"""
FDA SILVER Layer ETL Job - Combine NSDE and CDER Data
Creates unified fda_all_ndc table with INNER JOIN of both datasets
Only includes NDCs present in both NSDE and CDER data
"""
import sys
import boto3  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql.functions import lit, col, when, substring, expr, split, lpad, regexp_replace, concat  # type: ignore[import-not-found]

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'dataset',
    'bronze_database', 'silver_database', 'silver_base_path',
    'compression_codec', 'crawler_name',
    'nsde_table', 'cder_products_table', 'cder_packages_table'
])

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark
spark.conf.set("spark.sql.parquet.compression.codec", args['compression_codec'])
spark.conf.set("spark.sql.parquet.summary.metadata.level", "ALL")

# Generate human-readable runtime run_id
from datetime import datetime
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Get configuration from job arguments
dataset = args['dataset']
bronze_database = args['bronze_database']
silver_database = args['silver_database']
silver_path = args['silver_base_path']
crawler_name = args['crawler_name']

# Get table names from source dataset configs
nsde_table = args['nsde_table']
cder_products_table = args['cder_products_table']
cder_packages_table = args['cder_packages_table']

print(f"Starting SILVER ETL for {dataset} (INNER JOIN NSDE + CDER)")
print(f"Bronze database: {bronze_database}")
print(f"Silver database: {silver_database}")
print(f"Silver path: {silver_path}")
print(f"Source tables: {nsde_table}, {cder_products_table}, {cder_packages_table}")
print(f"Run ID: {run_id}")
print(f"Mode: overwrite (kill-and-fill)")

try:
    # Step 1: Read Bronze tables
    print("Reading bronze tables...")

    # Read NSDE data
    nsde_df = glueContext.create_dynamic_frame.from_catalog(
        database=bronze_database,
        table_name=nsde_table
    ).toDF()
    print(f"NSDE records: {nsde_df.count()}")

    # Read CDER Products
    products_df = glueContext.create_dynamic_frame.from_catalog(
        database=bronze_database,
        table_name=cder_products_table
    ).toDF()
    print(f"CDER Products records: {products_df.count()}")

    # Read CDER Packages
    packages_df = glueContext.create_dynamic_frame.from_catalog(
        database=bronze_database,
        table_name=cder_packages_table
    ).toDF()
    print(f"CDER Packages records: {packages_df.count()}")

    # Step 2: Format NDC in packages table
    print("Formatting NDC from CDER packages...")

    # Format ndc_package_code (XXXXX-XXXX-XX) to 11-digit format (XXXXXXXXXXX)
    # Split on hyphen, pad each segment, then concatenate
    packages_formatted = packages_df.withColumn(
        "formatted_ndc",
        concat(
            lpad(split(col("ndc_package_code"), "-").getItem(0), 5, "0"),  # First segment: 5 digits
            lpad(split(col("ndc_package_code"), "-").getItem(1), 4, "0"),  # Second segment: 4 digits
            lpad(split(col("ndc_package_code"), "-").getItem(2), 2, "0")   # Third segment: 2 digits
        )
    )

    # Step 3: Join CDER Products + Packages (excluding NDCs marked for exclusion)
    print("Joining CDER products and packages...")

    # Filter out excluded NDCs
    products_filtered = products_df.filter(
        (col("ndc_exclude_flag") != "E") | col("ndc_exclude_flag").isNull()
    )

    cder_joined = products_filtered.join(
        packages_formatted,
        "product_id",
        "inner"
    ).select(
        # Use package NDC as primary key (11-digit)
        packages_formatted["formatted_ndc"].alias("ndc_11"),

        # Product info from CDER products table
        products_filtered["marketing_category_name"].alias("marketing_category"),
        products_filtered["product_type_name"].alias("product_type"),
        products_filtered["proprietary_name"].alias("proprietary_name"),
        products_filtered["dosage_form_name"].alias("dosage_form"),
        products_filtered["application_number"].alias("application_number"),
        products_filtered["dea_schedule"].alias("dea_schedule"),
        products_filtered["active_numerator_strength"].alias("active_numerator_strength"),
        products_filtered["active_ingredient_unit"].alias("active_ingredient_unit"),

        # Derive spl_id from product_id (part after underscore)
        split(products_filtered["product_id"], "_").getItem(1).alias("spl_id"),

        # Package info from CDER packages table
        packages_formatted["package_description"].alias("package_description"),
        packages_formatted["start_marketing_date"].alias("marketing_start_date"),
        packages_formatted["end_marketing_date"].alias("marketing_end_date"),

        # Default values
        lit("").alias("billing_unit"),  # Not in CDER data

        # Metadata
        packages_formatted["meta_run_id"]
    )

    print(f"CDER joined records: {cder_joined.count()}")

    # Step 4: INNER JOIN with NSDE (only NDCs present in both datasets)
    print("Performing INNER JOIN with NSDE data...")

    # Create alias for nsde columns to avoid ambiguity
    nsde_selected = nsde_df.select(
        col("ndc_11").alias("nsde_ndc_11"),
        col("proprietary_name").alias("nsde_proprietary_name"),
        col("dosage_form").alias("nsde_dosage_form"),
        col("marketing_category").alias("nsde_marketing_category"),
        col("application number or monograph id").alias("nsde_application_number"),
        col("product_type").alias("nsde_product_type"),
        col("marketing_start_date").alias("nsde_marketing_start_date"),
        col("marketing_end_date").alias("nsde_marketing_end_date"),
        col("billing_unit").alias("nsde_billing_unit")
    )

    silver_df = cder_joined.join(
        nsde_selected,
        cder_joined["ndc_11"] == nsde_selected["nsde_ndc_11"],
        "inner"
    ).select(
        # Primary key
        cder_joined["ndc_11"],

        # Prioritize NSDE data where available, fallback to CDER
        when(col("nsde_marketing_category").isNotNull() & (col("nsde_marketing_category") != ""),
             col("nsde_marketing_category")).otherwise(cder_joined["marketing_category"]).alias("marketing_category"),

        when(col("nsde_product_type").isNotNull() & (col("nsde_product_type") != ""),
             col("nsde_product_type")).otherwise(cder_joined["product_type"]).alias("product_type"),

        when(col("nsde_proprietary_name").isNotNull() & (col("nsde_proprietary_name") != ""),
             col("nsde_proprietary_name")).otherwise(cder_joined["proprietary_name"]).alias("proprietary_name"),

        when(col("nsde_dosage_form").isNotNull() & (col("nsde_dosage_form") != ""),
             col("nsde_dosage_form")).otherwise(cder_joined["dosage_form"]).alias("dosage_form"),

        when(col("nsde_application_number").isNotNull() & (col("nsde_application_number") != ""),
             col("nsde_application_number")).otherwise(cder_joined["application_number"]).alias("application_number"),

        # CDER-only fields
        cder_joined["dea_schedule"],
        cder_joined["package_description"],
        cder_joined["active_numerator_strength"],
        cder_joined["active_ingredient_unit"],
        cder_joined["spl_id"],

        # NSDE-prioritized dates and billing
        when(col("nsde_marketing_start_date").isNotNull(),
             col("nsde_marketing_start_date")).otherwise(cder_joined["marketing_start_date"]).alias("marketing_start_date"),

        when(col("nsde_marketing_end_date").isNotNull(),
             col("nsde_marketing_end_date")).otherwise(cder_joined["marketing_end_date"]).alias("marketing_end_date"),

        when(col("nsde_billing_unit").isNotNull() & (col("nsde_billing_unit") != ""),
             col("nsde_billing_unit")).otherwise(cder_joined["billing_unit"]).alias("billing_unit"),

        # Flag indicating NSDE presence (always TRUE since this is INNER JOIN)
        lit(True).alias("nsde_flag"),

        # Metadata
        cder_joined["meta_run_id"]
    )

    print(f"Final SILVER records (INNER JOIN): {silver_df.count()}")
    print(f"Final SILVER columns: {silver_df.columns}")

    # Step 5: Write to SILVER layer
    print(f"Writing SILVER data to: {silver_path}")
    print("Mode: overwrite (kill-and-fill)")

    # Write using DataFrame for proper overwrite behavior
    # This ensures old files are deleted before writing new ones
    silver_df.write \
        .mode("overwrite") \
        .parquet(silver_path)

    print("SILVER data written successfully")
    print(f"Note: Run crawler manually when needed: {crawler_name}")
    print("Command: aws glue start-crawler --name " + crawler_name)

    print("SILVER ETL completed successfully")

except Exception as e:
    print(f"SILVER ETL failed: {str(e)}")
    raise e

finally:
    job.commit()