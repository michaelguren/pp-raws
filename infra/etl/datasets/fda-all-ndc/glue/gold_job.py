"""
FDA GOLD Layer ETL Job - Combine NSDE and CDER Data
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
    'bronze_database', 'gold_database', 'gold_base_path',
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
gold_database = args['gold_database']
gold_path = args['gold_base_path']
crawler_name = args['crawler_name']

# Get table names from source dataset configs
nsde_table = args['nsde_table']
cder_products_table = args['cder_products_table']
cder_packages_table = args['cder_packages_table']

print(f"Starting GOLD ETL for {dataset} (INNER JOIN NSDE + CDER)")
print(f"Bronze database: {bronze_database}")
print(f"Gold database: {gold_database}")
print(f"Gold path: {gold_path}")
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
        packages_formatted["formatted_ndc"].alias("fda_ndc_11"),

        # Product info from CDER products table
        products_filtered["marketing_category_name"].alias("fda_marketing_category"),
        products_filtered["product_type_name"].alias("fda_product_type"),
        products_filtered["proprietary_name"].alias("fda_proprietary_name"),
        products_filtered["dosage_form_name"].alias("fda_dosage_form"),
        products_filtered["application_number"].alias("fda_application_number"),
        products_filtered["dea_schedule"].alias("fda_dea_schedule"),
        products_filtered["active_numerator_strength"],
        products_filtered["active_ingredient_unit"],

        # Derive spl_id from product_id (part after underscore)
        split(products_filtered["product_id"], "_").getItem(1).alias("spl_id"),

        # Package info from CDER packages table
        packages_formatted["package_description"].alias("fda_package_description"),
        packages_formatted["start_marketing_date"].alias("fda_marketing_start_date"),
        packages_formatted["end_marketing_date"].alias("fda_marketing_end_date"),

        # Computed fields
        substring(packages_formatted["formatted_ndc"], 1, 5).alias("fda_ndc_5"),

        # Default values
        lit("").alias("fda_billing_unit"),  # Not in CDER data
        lit(False).alias("repackager"),     # Will be computed later if needed

        # Metadata
        packages_formatted["meta_run_id"]
    )

    print(f"CDER joined records: {cder_joined.count()}")

    # Step 4: INNER JOIN with NSDE (only NDCs present in both datasets)
    print("Performing INNER JOIN with NSDE data...")

    gold_df = cder_joined.join(
        nsde_df.select("ndc_11", "proprietary_name", "dosage_form", "marketing_category",
                      "application number or monograph id", "product_type", "marketing_start_date",
                      "marketing_end_date", "billing_unit"),
        cder_joined["fda_ndc_11"] == nsde_df["ndc_11"],
        "inner"
    ).select(
        # Primary key
        col("fda_ndc_11"),

        # Prioritize NSDE data where available, fallback to CDER
        when(nsde_df["marketing_category"].isNotNull() & (nsde_df["marketing_category"] != ""),
             nsde_df["marketing_category"]).otherwise(col("fda_marketing_category")).alias("fda_marketing_category"),

        when(nsde_df["product_type"].isNotNull() & (nsde_df["product_type"] != ""),
             nsde_df["product_type"]).otherwise(col("fda_product_type")).alias("fda_product_type"),

        when(nsde_df["proprietary_name"].isNotNull() & (nsde_df["proprietary_name"] != ""),
             nsde_df["proprietary_name"]).otherwise(col("fda_proprietary_name")).alias("fda_proprietary_name"),

        when(nsde_df["dosage_form"].isNotNull() & (nsde_df["dosage_form"] != ""),
             nsde_df["dosage_form"]).otherwise(col("fda_dosage_form")).alias("fda_dosage_form"),

        when(nsde_df["application number or monograph id"].isNotNull() & (nsde_df["application number or monograph id"] != ""),
             nsde_df["application number or monograph id"]).otherwise(col("fda_application_number")).alias("fda_application_number"),

        # CDER-only fields
        col("fda_dea_schedule"),
        col("fda_package_description"),
        col("active_numerator_strength"),
        col("active_ingredient_unit"),
        col("spl_id"),
        col("fda_ndc_5"),
        col("repackager"),

        # NSDE-prioritized dates and billing
        when(nsde_df["marketing_start_date"].isNotNull(),
             nsde_df["marketing_start_date"]).otherwise(col("fda_marketing_start_date")).alias("fda_marketing_start_date"),

        when(nsde_df["marketing_end_date"].isNotNull(),
             nsde_df["marketing_end_date"]).otherwise(col("fda_marketing_end_date")).alias("fda_marketing_end_date"),

        when(nsde_df["billing_unit"].isNotNull() & (nsde_df["billing_unit"] != ""),
             nsde_df["billing_unit"]).otherwise(col("fda_billing_unit")).alias("fda_billing_unit"),

        # Flag indicating NSDE presence (always TRUE since this is INNER JOIN)
        lit(True).alias("fda_nsde_flag"),

        # Metadata
        col("meta_run_id")
    )

    print(f"Final GOLD records (INNER JOIN): {gold_df.count()}")
    print(f"Final GOLD columns: {gold_df.columns}")

    # Step 5: Write to GOLD layer
    print(f"Writing GOLD data to: {gold_path}")

    # Convert to DynamicFrame for writing
    from awsglue.dynamicframe import DynamicFrame  # type: ignore[import-not-found]
    gold_dynamic_frame = DynamicFrame.fromDF(
        gold_df,
        glueContext,
        dataset
    )

    # Write to S3 (kill-and-fill)
    glueContext.write_dynamic_frame.from_options(
        frame=gold_dynamic_frame,
        connection_type="s3",
        connection_options={"path": gold_path},
        format="parquet",
        transformation_ctx=f"write_{dataset}"
    )

    print("GOLD data written successfully")
    print(f"Note: Run crawler manually via console if schema changes are needed: {crawler_name}")

    print("GOLD ETL completed successfully")

except Exception as e:
    print(f"GOLD ETL failed: {str(e)}")
    raise e

finally:
    job.commit()