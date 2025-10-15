"""
Temporal Versioning Utility for PySpark Gold Layer Jobs

Implements temporal validity pattern (SCD Type 2) for tracking historical changes.
Uses end-of-time pattern (9999-12-31) instead of NULL for current records.

Usage:
    from temporal_versioning import apply_temporal_versioning

    # Apply versioning to incoming data
    versioned_df = apply_temporal_versioning(
        spark=spark,
        incoming_df=silver_df,
        existing_table="pp_dw_gold.fda_all_ndc",
        business_key="ndc_11",
        business_columns=["proprietary_name", "dosage_form", "marketing_start_date"],
        run_date="2025-10-14",
        run_id="20251014-123456"
    )

    # Write with status partitioning
    versioned_df.write.partitionBy("status").parquet("s3://...")
"""

import hashlib
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window


# Constants
END_OF_TIME = "9999-12-31"
STATUS_CURRENT = "current"
STATUS_HISTORICAL = "historical"


def compute_content_hash(row, columns):
    """
    Compute MD5 hash of concatenated column values for change detection.

    Args:
        row: PySpark Row object
        columns: List of column names to include in hash

    Returns:
        MD5 hash string (32 chars)
    """
    values = [str(row[col]) if row[col] is not None else 'NULL' for col in columns]
    concat_str = '||'.join(values)
    return hashlib.md5(concat_str.encode()).hexdigest()


def apply_temporal_versioning(
    spark: SparkSession,
    incoming_df: DataFrame,
    existing_table: str,
    business_key: str,
    business_columns: list,
    run_date: str,
    run_id: str
) -> DataFrame:
    """
    Apply temporal versioning (SCD Type 2) to incoming data.

    Identifies NEW, CHANGED, UNCHANGED, and EXPIRED records, then generates
    appropriate versioned records with active_from/active_to dates and status partitions.

    Args:
        spark: SparkSession instance
        incoming_df: New data from Silver layer
        existing_table: Fully qualified table name (e.g., "pp_dw_gold.fda_all_ndc")
        business_key: Primary business identifier column (e.g., "ndc_11")
        business_columns: List of columns to track for changes (excludes metadata)
        run_date: Current run date as string (YYYY-MM-DD)
        run_id: Unique run identifier (e.g., "20251014-123456")

    Returns:
        DataFrame with versioned records ready to write (includes status partition column)

    Logic:
        - NEW: Insert with active_from=today, active_to=9999-12-31, status=current
        - CHANGED: Expire old (active_to=today, status=historical), insert new (active_to=9999-12-31, status=current)
        - UNCHANGED: No action (not included in output)
        - EXPIRED: Update to active_to=today, status=historical
    """

    print(f"[TEMPORAL] Starting temporal versioning for {existing_table}")
    print(f"[TEMPORAL] Business key: {business_key}")
    print(f"[TEMPORAL] Tracking {len(business_columns)} business columns")
    print(f"[TEMPORAL] Run date: {run_date}")

    # ========================
    # 1. Prepare Incoming Data
    # ========================

    # Add run metadata if not present
    if "run_date" not in incoming_df.columns:
        incoming_df = incoming_df.withColumn("run_date", F.lit(run_date))
    if "run_id" not in incoming_df.columns:
        incoming_df = incoming_df.withColumn("run_id", F.lit(run_id))

    # Compute content hash for change detection
    # Use concat_ws to concatenate columns, then hash
    hash_expr = F.md5(F.concat_ws("||", *[
        F.when(F.col(c).isNull(), F.lit("NULL")).otherwise(F.col(c).cast(StringType()))
        for c in business_columns
    ]))
    incoming_df = incoming_df.withColumn("content_hash", hash_expr)

    incoming_count = incoming_df.count()
    print(f"[TEMPORAL] Incoming records: {incoming_count}")

    # ========================
    # 2. Read Current Gold Data
    # ========================

    try:
        # Read only current records (active_to = 9999-12-31)
        current_df = spark.sql(f"""
            SELECT * FROM {existing_table}
            WHERE active_to = '{END_OF_TIME}'
        """)
        current_count = current_df.count()
        print(f"[TEMPORAL] Current records in gold: {current_count}")
    except Exception as e:
        # Table doesn't exist or is empty - treat as initial load
        print(f"[TEMPORAL] No existing data found (initial load): {e}")
        current_df = None

    # ========================
    # 3. Handle Initial Load
    # ========================

    if current_df is None or current_df.count() == 0:
        print("[TEMPORAL] Initial load - all records are NEW")

        result_df = (
            incoming_df
            .withColumn("version_id", F.expr("uuid()"))
            .withColumn("active_from", F.lit(run_date).cast(DateType()))
            .withColumn("active_to", F.lit(END_OF_TIME).cast(DateType()))
            .withColumn("status", F.lit(STATUS_CURRENT))
        )

        total_count = result_df.count()
        print(f"[TEMPORAL] Total records to write: {total_count} (all new)")
        return result_df

    # ========================
    # 4. Identify Changes
    # ========================

    # Full outer join to identify all scenarios
    comparison_df = incoming_df.alias("incoming").join(
        current_df.alias("current"),
        on=business_key,
        how="full_outer"
    )

    # Classify each record
    comparison_df = comparison_df.withColumn(
        "change_type",
        F.when(
            F.col(f"current.{business_key}").isNull(),
            F.lit("NEW")  # In incoming, not in current
        ).when(
            F.col(f"incoming.{business_key}").isNull(),
            F.lit("EXPIRED")  # In current, not in incoming
        ).when(
            F.col("incoming.content_hash") == F.col("current.content_hash"),
            F.lit("UNCHANGED")  # Same hash - no change
        ).otherwise(
            F.lit("CHANGED")  # Different hash - record changed
        )
    )

    # Count by change type
    print("[TEMPORAL] Change detection results:")
    change_counts = comparison_df.groupBy("change_type").count().collect()
    for row in change_counts:
        print(f"[TEMPORAL]   {row['change_type']}: {row['count']} records")

    # ========================
    # 5. Generate Versioned Records
    # ========================

    output_dfs = []

    # --- NEW RECORDS ---
    new_records = comparison_df.filter(F.col("change_type") == "NEW")
    if new_records.count() > 0:
        new_df = new_records.select("incoming.*")
        new_df = (
            new_df
            .withColumn("version_id", F.expr("uuid()"))
            .withColumn("active_from", F.lit(run_date).cast(DateType()))
            .withColumn("active_to", F.lit(END_OF_TIME).cast(DateType()))
            .withColumn("status", F.lit(STATUS_CURRENT))
        )
        output_dfs.append(new_df)

    # --- CHANGED RECORDS (Expire Old + Insert New) ---
    changed_records = comparison_df.filter(F.col("change_type") == "CHANGED")
    if changed_records.count() > 0:
        # Expire old versions (move to historical partition)
        expired_df = changed_records.select("current.*")
        expired_df = (
            expired_df
            .withColumn("active_to", F.lit(run_date).cast(DateType()))
            .withColumn("status", F.lit(STATUS_HISTORICAL))
        )
        output_dfs.append(expired_df)

        # Insert new versions (current partition)
        new_versions_df = changed_records.select("incoming.*")
        new_versions_df = (
            new_versions_df
            .withColumn("version_id", F.expr("uuid()"))
            .withColumn("active_from", F.lit(run_date).cast(DateType()))
            .withColumn("active_to", F.lit(END_OF_TIME).cast(DateType()))
            .withColumn("status", F.lit(STATUS_CURRENT))
        )
        output_dfs.append(new_versions_df)

    # --- EXPIRED RECORDS (No Longer in Source) ---
    expired_records = comparison_df.filter(F.col("change_type") == "EXPIRED")
    if expired_records.count() > 0:
        expired_df = expired_records.select("current.*")
        expired_df = (
            expired_df
            .withColumn("active_to", F.lit(run_date).cast(DateType()))
            .withColumn("status", F.lit(STATUS_HISTORICAL))
        )
        output_dfs.append(expired_df)

    # UNCHANGED records - no action needed (not included in output)
    unchanged_count = comparison_df.filter(F.col("change_type") == "UNCHANGED").count()
    print(f"[TEMPORAL] Skipping {unchanged_count} UNCHANGED records (no write needed)")

    # ========================
    # 6. Union and Return
    # ========================

    if not output_dfs:
        print("[TEMPORAL] No changes detected - nothing to write")
        # Return empty dataframe with correct schema
        return spark.createDataFrame([], incoming_df.schema)

    result_df = output_dfs[0]
    for df in output_dfs[1:]:
        result_df = result_df.unionByName(df, allowMissingColumns=True)

    total_count = result_df.count()
    print(f"[TEMPORAL] Total records to write: {total_count}")

    return result_df


def get_current_records(spark: SparkSession, table: str) -> DataFrame:
    """
    Helper: Query all current (active) records.

    Args:
        spark: SparkSession
        table: Fully qualified table name

    Returns:
        DataFrame with only current records (active_to = 9999-12-31)
    """
    return spark.sql(f"""
        SELECT * FROM {table}
        WHERE active_to = '{END_OF_TIME}'
    """)


def get_changes_for_date(spark: SparkSession, table: str, target_date: str) -> DataFrame:
    """
    Helper: Get all records that changed on a specific date.
    Useful for incremental DynamoDB syncs.

    Args:
        spark: SparkSession
        table: Fully qualified table name
        target_date: Date string (YYYY-MM-DD)

    Returns:
        DataFrame with records where active_from = date OR active_to = date
        (excludes active_to = 9999-12-31 in the date check)
    """
    return spark.sql(f"""
        SELECT *
        FROM {table}
        WHERE run_date = '{target_date}'
          AND (
            active_from = '{target_date}'
            OR (active_to = '{target_date}' AND active_to < '{END_OF_TIME}')
          )
    """)


def get_historical_versions(
    spark: SparkSession,
    table: str,
    business_key_value: str,
    business_key_col: str
) -> DataFrame:
    """
    Helper: Get all historical versions of a specific record.

    Args:
        spark: SparkSession
        table: Fully qualified table name
        business_key_value: Value to filter (e.g., "00002322702")
        business_key_col: Column name (e.g., "ndc_11")

    Returns:
        DataFrame with all versions, ordered by active_from DESC
    """
    return spark.sql(f"""
        SELECT *,
            CASE
                WHEN active_to = '{END_OF_TIME}' THEN 'CURRENT'
                ELSE 'HISTORICAL'
            END as version_status
        FROM {table}
        WHERE {business_key_col} = '{business_key_value}'
        ORDER BY active_from DESC
    """)


def validate_temporal_table(spark: SparkSession, table: str, business_key: str):
    """
    Helper: Validate temporal table integrity.
    Checks for common issues like duplicate current records or gaps in history.

    Args:
        spark: SparkSession
        table: Fully qualified table name
        business_key: Primary business identifier

    Prints validation results
    """
    print(f"[VALIDATION] Checking temporal integrity for {table}")

    # Check 1: Each business key should have exactly one current record
    dupe_check = spark.sql(f"""
        SELECT {business_key}, COUNT(*) as current_count
        FROM {table}
        WHERE active_to = '{END_OF_TIME}'
        GROUP BY {business_key}
        HAVING COUNT(*) > 1
    """)

    dupe_count = dupe_check.count()
    if dupe_count > 0:
        print(f"[VALIDATION] ⚠️  Found {dupe_count} business keys with multiple current records!")
        dupe_check.show(10, truncate=False)
    else:
        print("[VALIDATION] ✅ No duplicate current records")

    # Check 2: Verify status partition matches active_to
    status_mismatch = spark.sql(f"""
        SELECT COUNT(*) as mismatch_count
        FROM {table}
        WHERE (active_to = '{END_OF_TIME}' AND status != '{STATUS_CURRENT}')
           OR (active_to < '{END_OF_TIME}' AND status != '{STATUS_HISTORICAL}')
    """)

    mismatch_count = status_mismatch.collect()[0]['mismatch_count']
    if mismatch_count > 0:
        print(f"[VALIDATION] ⚠️  Found {mismatch_count} records with status/active_to mismatch!")
    else:
        print("[VALIDATION] ✅ All status values match active_to dates")

    # Check 3: Summary statistics
    stats = spark.sql(f"""
        SELECT
            status,
            COUNT(*) as record_count,
            COUNT(DISTINCT {business_key}) as unique_keys
        FROM {table}
        GROUP BY status
    """)

    print("[VALIDATION] Summary statistics:")
    stats.show(truncate=False)
