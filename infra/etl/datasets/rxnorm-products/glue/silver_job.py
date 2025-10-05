"""
RxNORM Products Silver Layer Job

Transforms normalized RxNORM bronze tables into denormalized prescribable drug products.

This job consolidates the legacy Rails ETL logic (13+ sequential UPDATEs) into a single
Spark transformation with multiple joins and window functions.

Input: Bronze RxNORM tables (rxnconso, rxnrel, rxnsat)
Output: Silver rxnorm_products table (denormalized, one row per prescribable product)
"""

import sys
from awsglue.transforms import *  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext  # type: ignore[import-not-found]
from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]
from datetime import datetime

# Parse job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'dataset',
    'bucket_name',
    'bronze_database',
    'silver_database',
    'silver_base_path',
    'rxnconso_table',
    'rxnrel_table',
    'rxnsat_table',
    'output_table',
    'compression_codec',
    'crawler_name'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting RxNORM Products silver job for dataset: {args['dataset']}")
print(f"Bronze database: {args['bronze_database']}")
print(f"Silver database: {args['silver_database']}")
print(f"Output path: {args['silver_base_path']}")

# Timestamps for created_at/updated_at
run_timestamp = datetime.now()

# ============================================================================
# STEP 1: Load Bronze RxNORM Tables
# ============================================================================

print("Loading bronze RxNORM tables from Glue catalog...")

# Load RXNCONSO (concepts and names)
rxnconso = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name=args['rxnconso_table']
).toDF()

# Load RXNREL (relationships)
rxnrel = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name=args['rxnrel_table']
).toDF()

# Load RXNSAT (attributes like strength)
rxnsat = glueContext.create_dynamic_frame.from_catalog(
    database=args['bronze_database'],
    table_name=args['rxnsat_table']
).toDF()

print(f"Loaded {rxnconso.count()} RXNCONSO records")
print(f"Loaded {rxnrel.count()} RXNREL records")
print(f"Loaded {rxnsat.count()} RXNSAT records")

# ============================================================================
# STEP 2: Filter to Prescribable Products (CVF='4096')
# ============================================================================

print("Filtering to prescribable products (SCD, SBD, GPCK, BPCK with CVF='4096')...")

# Base product selection (equivalent to legacy lines 37-46)
base_products = rxnconso.filter(
    (F.col('TTY').isin(['SCD', 'SBD', 'GPCK', 'BPCK'])) &
    (F.col('SAB') == 'RXNORM') &
    (F.col('CVF') == '4096')
).select(
    'RXCUI',
    'TTY',
    'STR'
).distinct()

print(f"Found {base_products.count()} prescribable products")

# ============================================================================
# STEP 3: Extract Ingredient Names (MIN > PIN > IN Priority)
# ============================================================================

print("Extracting ingredient names with priority: MIN > PIN > IN...")

# Filter relationships to RXNORM + CVF='4096'
rxnrel_filtered = rxnrel.filter(
    (F.col('SAB') == 'RXNORM') &
    (F.col('CVF') == '4096')
)

rxnconso_filtered = rxnconso.filter(
    (F.col('SAB') == 'RXNORM') &
    (F.col('CVF') == '4096')
)

# For SCD: Get ingredients via consists_of -> has_precise_ingredient (PIN)
scd_pin = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'consists_of').alias('scdc_rel'),
        F.col('scd.RXCUI') == F.col('scdc_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_precise_ingredient').alias('pin_rel'),
        F.col('scdc_rel.RXCUI1') == F.col('pin_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'PIN').alias('pin_conso'),
        F.col('pin_rel.RXCUI1') == F.col('pin_conso.RXCUI'),
        'left'
    ).groupBy('scd.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.lower(F.col('pin_conso.STR')))), ' / ').alias('pin')
    )

# For SCD: Get multiple ingredients via has_ingredients (MIN)
scd_min = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_ingredients').alias('min_rel'),
        F.col('scd.RXCUI') == F.col('min_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'MIN').alias('min_conso'),
        F.col('min_rel.RXCUI1') == F.col('min_conso.RXCUI'),
        'left'
    ).groupBy('scd.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.lower(F.col('min_conso.STR')))), ' / ').alias('min')
    )

# For SCD: Get ingredients via consists_of -> has_ingredient (IN)
scd_in = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'consists_of').alias('scdc_rel'),
        F.col('scd.RXCUI') == F.col('scdc_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_ingredient').alias('in_rel'),
        F.col('scdc_rel.RXCUI1') == F.col('in_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'IN').alias('in_conso'),
        F.col('in_rel.RXCUI1') == F.col('in_conso.RXCUI'),
        'left'
    ).groupBy('scd.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.lower(F.col('in_conso.STR')))), ' / ').alias('in')
    )

# Combine SCD ingredients with priority logic (MIN > PIN > IN)
scd_ingredients = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(scd_min, 'RXCUI', 'left') \
    .join(scd_pin, 'RXCUI', 'left') \
    .join(scd_in, 'RXCUI', 'left') \
    .select(
        'scd.RXCUI',
        F.when(F.col('min').isNotNull() & (F.col('min') != ''), F.col('min'))
         .when(F.col('pin').isNotNull() & (F.col('pin') != ''), F.col('pin'))
         .otherwise(F.col('in')).alias('rxnorm_ingredient_names')
    )

# For SBD: Get ingredients via tradename_of -> SCD logic
sbd_scd = base_products.filter(F.col('TTY') == 'SBD').alias('sbd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'tradename_of').alias('scd_rel'),
        F.col('sbd.RXCUI') == F.col('scd_rel.RXCUI2'),
        'left'
    ).select(
        F.col('sbd.RXCUI').alias('sbd_rxcui'),
        F.col('scd_rel.RXCUI1').alias('scd_rxcui')
    )

sbd_ingredients = sbd_scd.join(scd_ingredients, sbd_scd.scd_rxcui == scd_ingredients.RXCUI, 'left') \
    .select(
        F.col('sbd_rxcui').alias('RXCUI'),
        F.col('rxnorm_ingredient_names')
    )

# For GPCK/BPCK: Get ingredients via contains -> SCD logic
pack_ingredients = base_products.filter(F.col('TTY').isin(['GPCK', 'BPCK'])).alias('pack') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'contains').alias('scd_rel'),
        F.col('pack.RXCUI') == F.col('scd_rel.RXCUI2'),
        'left'
    ).join(
        scd_ingredients,
        F.col('scd_rel.RXCUI1') == scd_ingredients.RXCUI,
        'left'
    ).groupBy('pack.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.col('rxnorm_ingredient_names'))), ' / ').alias('rxnorm_ingredient_names')
    )

# Union all ingredient results
all_ingredients = scd_ingredients \
    .unionByName(sbd_ingredients, allowMissingColumns=True) \
    .unionByName(pack_ingredients, allowMissingColumns=True)

print("Ingredient names extracted")

# ============================================================================
# STEP 4: Extract Strength from RXNSAT
# ============================================================================

print("Extracting drug strengths from RXNSAT...")

strengths = rxnsat.filter(
    (F.col('ATN') == 'RXN_AVAILABLE_STRENGTH') &
    (F.col('CVF') == '4096')
).select(
    'RXCUI',
    F.lower(F.col('ATV')).alias('raw_strength')
).groupBy('RXCUI').agg(
    F.first('raw_strength').alias('raw_strength')
)

# Clean strength: Remove "(EXPRESSED AS ...)" patterns (legacy line 273)
def clean_strength_expr(strength_col, ingredient_col):
    # Remove "(EXPRESSED AS <ingredient>)" pattern
    cleaned = F.regexp_replace(
        F.upper(strength_col),
        F.concat(F.lit(r'\(EXPRESSED AS '), F.upper(ingredient_col), F.lit(r'\)')),
        ''
    )
    return F.trim(F.lower(cleaned))

strengths_with_ingredients = strengths.join(all_ingredients, 'RXCUI', 'left')
strengths_cleaned = strengths_with_ingredients.select(
    'RXCUI',
    F.when(
        F.col('raw_strength').contains('expressed as'),
        clean_strength_expr(F.col('raw_strength'), F.col('rxnorm_ingredient_names'))
    ).otherwise(F.col('raw_strength')).alias('rxnorm_strength')
)

print("Strengths extracted and cleaned")

# ============================================================================
# STEP 5: Extract Brand Names (BN)
# ============================================================================

print("Extracting brand names...")

# For SCD: SCD --[has_tradename]--> SBD --[has_ingredient]--> BN
scd_brands = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_tradename').alias('sbd_rel'),
        F.col('scd.RXCUI') == F.col('sbd_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_ingredient').alias('bn_rel'),
        F.col('sbd_rel.RXCUI1') == F.col('bn_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'BN').alias('bn_conso'),
        F.col('bn_rel.RXCUI1') == F.col('bn_conso.RXCUI'),
        'left'
    ).groupBy('scd.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.upper(F.col('bn_conso.STR')))), ' / ').alias('brand_names')
    )

# For SBD: SBD --[has_ingredient]--> BN
sbd_brands = base_products.filter(F.col('TTY') == 'SBD').alias('sbd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_ingredient').alias('bn_rel'),
        F.col('sbd.RXCUI') == F.col('bn_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'BN').alias('bn_conso'),
        F.col('bn_rel.RXCUI1') == F.col('bn_conso.RXCUI'),
        'left'
    ).groupBy('sbd.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.upper(F.col('bn_conso.STR')))), ' / ').alias('brand_names')
    )

# Fallback for NULLs: Try without CVF filter (legacy lines 381-405)
# This is a simplified version - in production we'd do a second pass
rxnrel_no_cvf = rxnrel.filter(F.col('SAB') == 'RXNORM')
rxnconso_no_cvf = rxnconso.filter(F.col('SAB') == 'RXNORM')

scd_brands_fallback = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_no_cvf.filter(F.col('RELA') == 'has_tradename').alias('sbd_rel'),
        F.col('scd.RXCUI') == F.col('sbd_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_no_cvf.filter(F.col('RELA') == 'has_ingredient').alias('bn_rel'),
        F.col('sbd_rel.RXCUI1') == F.col('bn_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_no_cvf.filter(F.col('TTY') == 'BN').alias('bn_conso'),
        F.col('bn_rel.RXCUI1') == F.col('bn_conso.RXCUI'),
        'left'
    ).groupBy('scd.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.upper(F.col('bn_conso.STR')))), ' / ').alias('brand_names_fallback')
    )

sbd_brands_fallback = base_products.filter(F.col('TTY') == 'SBD').alias('sbd') \
    .join(
        rxnrel_no_cvf.filter(F.col('RELA') == 'has_ingredient').alias('bn_rel'),
        F.col('sbd.RXCUI') == F.col('bn_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_no_cvf.filter(F.col('TTY') == 'BN').alias('bn_conso'),
        F.col('bn_rel.RXCUI1') == F.col('bn_conso.RXCUI'),
        'left'
    ).groupBy('sbd.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.upper(F.col('bn_conso.STR')))), ' / ').alias('brand_names_fallback')
    )

# Combine with fallback logic
all_brands = scd_brands.unionByName(sbd_brands, allowMissingColumns=True) \
    .join(scd_brands_fallback.unionByName(sbd_brands_fallback, allowMissingColumns=True), 'RXCUI', 'left') \
    .select(
        'RXCUI',
        F.when(
            (F.col('brand_names').isNotNull()) & (F.col('brand_names') != ''),
            F.col('brand_names')
        ).otherwise(F.col('brand_names_fallback')).alias('rxnorm_brand_names')
    )

print("Brand names extracted")

# ============================================================================
# STEP 6: Extract Dosage Forms (DF)
# ============================================================================

print("Extracting dosage forms...")

# For SCD/SBD: --[has_dose_form]--> DF
scd_sbd_dosage_forms = base_products.filter(F.col('TTY').isin(['SCD', 'SBD'])).alias('prod') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_dose_form').alias('df_rel'),
        F.col('prod.RXCUI') == F.col('df_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'DF').alias('df_conso'),
        F.col('df_rel.RXCUI1') == F.col('df_conso.RXCUI'),
        'left'
    ).groupBy('prod.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.col('df_conso.STR'))), ', ').alias('dosage_forms')
    )

# Fallback without CVF (legacy lines 428-439)
scd_sbd_dosage_forms_fallback = base_products.filter(F.col('TTY').isin(['SCD', 'SBD'])).alias('prod') \
    .join(
        rxnrel_no_cvf.filter(F.col('RELA') == 'has_dose_form').alias('df_rel'),
        F.col('prod.RXCUI') == F.col('df_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_no_cvf.filter(F.col('TTY') == 'DF').alias('df_conso'),
        F.col('df_rel.RXCUI1') == F.col('df_conso.RXCUI'),
        'left'
    ).groupBy('prod.RXCUI').agg(
        F.array_join(F.array_sort(F.collect_set(F.col('df_conso.STR'))), ', ').alias('dosage_forms_fallback')
    )

# For GPCK/BPCK: Pack (form1 / form2) format (legacy lines 441-454)
pack_dosage_forms = base_products.filter(F.col('TTY').isin(['GPCK', 'BPCK'])).alias('pack') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'contains').alias('scd_rel'),
        F.col('pack.RXCUI') == F.col('scd_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_dose_form').alias('dose_rel'),
        F.col('scd_rel.RXCUI1') == F.col('dose_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.alias('df_conso'),
        F.col('dose_rel.RXCUI1') == F.col('df_conso.RXCUI'),
        'left'
    ).groupBy('pack.RXCUI').agg(
        F.concat(
            F.lit('Pack ('),
            F.array_join(F.array_sort(F.collect_set(F.trim(F.col('df_conso.STR')))), ' / '),
            F.lit(')')
        ).alias('dosage_forms')
    )

# Combine dosage forms with fallback
all_dosage_forms = scd_sbd_dosage_forms \
    .join(scd_sbd_dosage_forms_fallback, 'RXCUI', 'left') \
    .select(
        'RXCUI',
        F.when(
            (F.col('dosage_forms').isNotNull()) & (F.col('dosage_forms') != ''),
            F.col('dosage_forms')
        ).otherwise(F.col('dosage_forms_fallback')).alias('rxnorm_dosage_forms')
    ).unionByName(pack_dosage_forms, allowMissingColumns=True)

print("Dosage forms extracted")

# ============================================================================
# STEP 7: Extract Prescribable Name (PSN)
# ============================================================================

print("Extracting prescribable names...")

psn = base_products.alias('prod') \
    .join(
        rxnconso_filtered.filter(F.col('TTY') == 'PSN').alias('psn_conso'),
        F.col('prod.RXCUI') == F.col('psn_conso.RXCUI'),
        'left'
    ).select(
        F.col('prod.RXCUI'),
        F.col('psn_conso.STR').alias('rxnorm_psn')
    ).groupBy('RXCUI').agg(F.first('rxnorm_psn').alias('rxnorm_psn'))

print("Prescribable names extracted")

# ============================================================================
# STEP 8: Extract Semantic Forms (SCDF, SBDF, SBD_RXCUI, BPCK_RXCUI)
# ============================================================================

print("Extracting semantic forms...")

# SCDF for SCD: SCD --[isa]--> SCDF
scdf_for_scd = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'isa').alias('scdf_rel'),
        F.col('scd.RXCUI') == F.col('scdf_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'SCDF').alias('scdf_conso'),
        F.col('scdf_rel.RXCUI1') == F.col('scdf_conso.RXCUI'),
        'left'
    ).select(
        F.col('scd.RXCUI'),
        F.col('scdf_conso.RXCUI').alias('scdf_rxcui'),
        F.col('scdf_conso.STR').alias('scdf_name')
    ).groupBy('RXCUI').agg(
        F.first('scdf_rxcui').alias('scdf_rxcui'),
        F.first('scdf_name').alias('scdf_name')
    )

# SCDF for SBD: SBD --[tradename_of]--> SCD --[isa]--> SCDF
scdf_for_sbd = base_products.filter(F.col('TTY') == 'SBD').alias('sbd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'tradename_of').alias('scd_rel'),
        F.col('sbd.RXCUI') == F.col('scd_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_filtered.filter(F.col('RELA') == 'isa').alias('scdf_rel'),
        F.col('scd_rel.RXCUI1') == F.col('scdf_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'SCDF').alias('scdf_conso'),
        F.col('scdf_rel.RXCUI1') == F.col('scdf_conso.RXCUI'),
        'left'
    ).select(
        F.col('sbd.RXCUI'),
        F.col('scdf_conso.RXCUI').alias('scdf_rxcui'),
        F.col('scdf_conso.STR').alias('scdf_name')
    ).groupBy('RXCUI').agg(
        F.first('scdf_rxcui').alias('scdf_rxcui'),
        F.first('scdf_name').alias('scdf_name')
    )

all_scdf = scdf_for_scd.unionByName(scdf_for_sbd, allowMissingColumns=True)

# SBDF for SBD: SBD --[isa]--> SBDF
sbdf_for_sbd = base_products.filter(F.col('TTY') == 'SBD').alias('sbd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'isa').alias('sbdf_rel'),
        F.col('sbd.RXCUI') == F.col('sbdf_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'SBDF').alias('sbdf_conso'),
        F.col('sbdf_rel.RXCUI1') == F.col('sbdf_conso.RXCUI'),
        'left'
    ).select(
        F.col('sbd.RXCUI'),
        F.col('sbdf_conso.RXCUI').alias('sbdf_rxcui')
    ).groupBy('RXCUI').agg(F.first('sbdf_rxcui').alias('sbdf_rxcui'))

# SBDF for SCD: SCD --[has_tradename]--> SBD --[isa]--> SBDF
sbdf_for_scd = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_tradename').alias('sbd_rel'),
        F.col('scd.RXCUI') == F.col('sbd_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_filtered.filter(F.col('RELA') == 'isa').alias('sbdf_rel'),
        F.col('sbd_rel.RXCUI1') == F.col('sbdf_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'SBDF').alias('sbdf_conso'),
        F.col('sbdf_rel.RXCUI1') == F.col('sbdf_conso.RXCUI'),
        'left'
    ).select(
        F.col('scd.RXCUI'),
        F.col('sbdf_conso.RXCUI').alias('sbdf_rxcui')
    ).groupBy('RXCUI').agg(F.first('sbdf_rxcui').alias('sbdf_rxcui'))

all_sbdf = sbdf_for_sbd.unionByName(sbdf_for_scd, allowMissingColumns=True)

# SBD_RXCUI for SCD: SCD --[has_tradename]--> SBD
sbd_rxcui = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_tradename').alias('sbd_rel'),
        F.col('scd.RXCUI') == F.col('sbd_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'SBD').alias('sbd_conso'),
        F.col('sbd_rel.RXCUI1') == F.col('sbd_conso.RXCUI'),
        'left'
    ).select(
        F.col('scd.RXCUI'),
        F.col('sbd_conso.RXCUI').alias('sbd_rxcui')
    ).groupBy('RXCUI').agg(F.first('sbd_rxcui').alias('sbd_rxcui'))

# BPCK_RXCUI for GPCK: GPCK --[has_tradename]--> BPCK
bpck_rxcui = base_products.filter(F.col('TTY') == 'GPCK').alias('gpck') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_tradename').alias('bpck_rel'),
        F.col('gpck.RXCUI') == F.col('bpck_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'BPCK').alias('bpck_conso'),
        F.col('bpck_rel.RXCUI1') == F.col('bpck_conso.RXCUI'),
        'left'
    ).select(
        F.col('gpck.RXCUI'),
        F.col('bpck_conso.RXCUI').alias('bpck_rxcui')
    ).groupBy('RXCUI').agg(F.first('bpck_rxcui').alias('bpck_rxcui'))

print("Semantic forms extracted")

# ============================================================================
# STEP 9: Calculate Multi-Ingredient Flag
# ============================================================================

print("Calculating multi-ingredient flags...")

# For SCD: Check if has_ingredients -> MIN exists
scd_multi = base_products.filter(F.col('TTY') == 'SCD').alias('scd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_ingredients').alias('min_rel'),
        F.col('scd.RXCUI') == F.col('min_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'MIN').alias('min_conso'),
        F.col('min_rel.RXCUI1') == F.col('min_conso.RXCUI'),
        'left'
    ).groupBy('scd.RXCUI').agg(
        (F.count(F.col('min_conso.RXCUI')) > 0).alias('multi_ingredient')
    )

# For SBD: Check via tradename_of -> SCD -> has_ingredients -> MIN
sbd_multi = base_products.filter(F.col('TTY') == 'SBD').alias('sbd') \
    .join(
        rxnrel_filtered.filter(F.col('RELA') == 'tradename_of').alias('scd_rel'),
        F.col('sbd.RXCUI') == F.col('scd_rel.RXCUI2'),
        'left'
    ).join(
        rxnrel_filtered.filter(F.col('RELA') == 'has_ingredients').alias('min_rel'),
        F.col('scd_rel.RXCUI1') == F.col('min_rel.RXCUI2'),
        'left'
    ).join(
        rxnconso_filtered.filter(F.col('TTY') == 'MIN').alias('min_conso'),
        F.col('min_rel.RXCUI1') == F.col('min_conso.RXCUI'),
        'left'
    ).groupBy('sbd.RXCUI').agg(
        (F.count(F.col('min_conso.RXCUI')) > 0).alias('multi_ingredient')
    )

all_multi = scd_multi.unionByName(sbd_multi, allowMissingColumns=True)

print("Multi-ingredient flags calculated")

# ============================================================================
# STEP 10: Assemble Final Silver Table
# ============================================================================

print("Assembling final rxnorm_products table...")

# Start with base products
final_df = base_products

# Join all enrichment dataframes
final_df = final_df \
    .join(all_ingredients, 'RXCUI', 'left') \
    .join(strengths_cleaned, 'RXCUI', 'left') \
    .join(all_brands, 'RXCUI', 'left') \
    .join(all_dosage_forms, 'RXCUI', 'left') \
    .join(psn, 'RXCUI', 'left') \
    .join(all_scdf, 'RXCUI', 'left') \
    .join(all_sbdf, 'RXCUI', 'left') \
    .join(sbd_rxcui, 'RXCUI', 'left') \
    .join(bpck_rxcui, 'RXCUI', 'left') \
    .join(all_multi, 'RXCUI', 'left')

# Compute sbdf_name as brand_names + ' ' + dosage_forms (legacy line 547)
final_df = final_df.withColumn(
    'sbdf_name',
    F.when(
        F.col('sbdf_rxcui').isNotNull(),
        F.concat_ws(' ', F.col('rxnorm_brand_names'), F.col('rxnorm_dosage_forms'))
    ).otherwise(F.lit(None))
)

# Add timestamps
final_df = final_df \
    .withColumn('created_at', F.lit(run_timestamp)) \
    .withColumn('updated_at', F.lit(run_timestamp))

# Fill NULLs for multi_ingredient flag (default to false)
final_df = final_df.withColumn(
    'multi_ingredient',
    F.coalesce(F.col('multi_ingredient'), F.lit(False))
)

# Select final columns in desired order (with lowercase aliases for base columns)
final_df = final_df.select(
    F.col('TTY').alias('tty'),
    F.col('RXCUI').alias('rxcui'),
    F.col('STR').alias('str'),
    'rxnorm_strength',
    'rxnorm_ingredient_names',
    'rxnorm_brand_names',
    'rxnorm_dosage_forms',
    'rxnorm_psn',
    'sbdf_rxcui',
    'sbdf_name',
    'scdf_rxcui',
    'scdf_name',
    'sbd_rxcui',
    'bpck_rxcui',
    'multi_ingredient',
    'created_at',
    'updated_at'
)

print(f"Final rxnorm_products count: {final_df.count()}")

# ============================================================================
# STEP 11: Write to S3 (Kill-and-Fill)
# ============================================================================

print(f"Writing to {args['silver_base_path']}...")

final_df.write \
    .mode('overwrite') \
    .format('parquet') \
    .option('compression', args['compression_codec'].lower()) \
    .save(args['silver_base_path'])

print("Silver table written successfully")

# ============================================================================
# STEP 12: Run Crawler to Update Glue Catalog
# ============================================================================

print(f"Starting crawler: {args['crawler_name']}")

import boto3  # type: ignore[import-not-found]
glue_client = boto3.client('glue')

try:
    glue_client.start_crawler(Name=args['crawler_name'])
    print(f"Crawler {args['crawler_name']} started successfully")
except Exception as e:
    print(f"Warning: Could not start crawler: {e}")
    print("You may need to run the crawler manually")

# ============================================================================
# Complete Job
# ============================================================================

job.commit()
print("RxNORM Products silver job completed successfully")
