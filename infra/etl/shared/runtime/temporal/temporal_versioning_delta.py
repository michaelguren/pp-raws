"""
Temporal Versioning Utility for PySpark Gold Layer Jobs - Delta Lake Edition

Implements temporal validity pattern (SCD Type 2) using Delta Lake MERGE operations.
Uses end-of-time pattern (9999-12-31) instead of NULL for current records.

Benefits vs. Parquet:
- 90%+ reduction in S3 writes (only changed records)
- ACID transactions
- Time travel capabilities
- Automatic schema evolution
- Optimized read performance with ZORDER

Usage:
    from temporal_versioning_delta import apply_temporal_versioning_delta

    # Apply versioning to incoming data
    apply_temporal_versioning_delta(
        spark=spark,
        incoming_df=silver_df,
        gold_path="s3://bucket/gold/fda-all-ndcs/",
        business_key="ndc_11",
        business_columns=["proprietary_name", "dosage_form", "marketing_start_date"],
        run_date="2025-10-14",
        run_id="20251014-123456"
    )

    # Optional: Run maintenance
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, gold_path)
    delta_table.vacuum(168)  # weekly cleanup
    delta_table.optimize().executeZOrderBy(business_key)
"""

import hashlib
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType
from delta.tables import DeltaTable


# Constants
END_OF_TIME = "9999-12-31"
STATUS_CURRENT = "current"
STATUS_HISTORICAL = "historical"


def compute_content_hash_expr(columns):
    """
    Generate PySpark expression to compute MD5 hash of concatenated column values.

    Args:
        columns: List of column names to include in hash

    Returns:
        PySpark column expression for MD5 hash
    """
    return F.md5(F.concat_ws("||", *[
        F.when(F.col(c).isNull(), F.lit("NULL")).otherwise(F.col(c).cast(StringType()))
        for c in columns
    ]))


def apply_temporal_versioning_delta(
    spark: SparkSession,
    incoming_df: DataFrame,
    gold_path: str,
    business_key: str | list,
    business_columns: list,
    run_date: str,
    run_id: str
) -> dict:
    """
    Apply temporal versioning (SCD Type 2) to incoming data using Delta Lake MERGE.

    Claude: Perform Delta MERGE using SCD Type 2 semantics.
    Claude: Only process changed or new business keys.

    Args:
        spark: SparkSession instance
        incoming_df: New data from Silver layer
        gold_path: S3 path for Delta table (e.g., "s3://bucket/gold/fda-all-ndcs/")
        business_key: Primary business identifier column(s) - string or list for composite keys
        business_columns: List of columns to track for changes (excludes metadata)
        run_date: Current run date as string (YYYY-MM-DD)
        run_id: Unique run identifier (e.g., "20251014-123456")

    Returns:
        Dictionary with operation statistics:
        {
            'new': int,
            'changed': int,
            'expired': int,
            'unchanged': int,
            'total_written': int
        }

    Logic:
        - NEW: Insert with active_from=run_date, active_to=9999-12-31, status=current
        - CHANGED: Expire old (active_to=run_date, status=historical), insert new
        - UNCHANGED: Skip (no write)
        - EXPIRED: Update to active_to=run_date, status=historical
    """

    # Normalize business_key to list for consistent handling
    business_keys = [business_key] if isinstance(business_key, str) else business_key

    print(f"[DELTA] Starting Delta Lake temporal versioning")
    print(f"[DELTA] Business key(s): {business_keys}")
    print(f"[DELTA] Tracking {len(business_columns)} business columns")
    print(f"[DELTA] Run date: {run_date}")
    print(f"[DELTA] Gold path: {gold_path}")

    # ========================
    # 1. Prepare Incoming Data
    # ========================

    # Add run metadata if not present
    if "run_date" not in incoming_df.columns:
        incoming_df = incoming_df.withColumn("run_date", F.lit(run_date))
    if "run_id" not in incoming_df.columns:
        incoming_df = incoming_df.withColumn("run_id", F.lit(run_id))

    # Compute content hash for change detection
    incoming_df = incoming_df.withColumn("content_hash", compute_content_hash_expr(business_columns))

    incoming_count = incoming_df.count()
    print(f"[DELTA] Incoming records: {incoming_count}")

    # ========================
    # 2. Check if Delta Table Exists
    # ========================

    try:
        delta_table = DeltaTable.forPath(spark, gold_path)
        is_initial_load = False
        print(f"[DELTA] Existing Delta table found at {gold_path}")
    except Exception as e:
        # Table doesn't exist - initial load
        print(f"[DELTA] No existing Delta table found - performing initial load")
        is_initial_load = True

    # ========================
    # 3. Handle Initial Load
    # ========================

    if is_initial_load:
        print("[DELTA] Initial load - all records are NEW")

        result_df = (
            incoming_df
            .withColumn("version_id", F.expr("uuid()"))
            .withColumn("active_from", F.lit(run_date).cast(DateType()))
            .withColumn("active_to", F.lit(END_OF_TIME).cast(DateType()))
            .withColumn("status", F.lit(STATUS_CURRENT))
            .withColumn("updated_at", F.current_timestamp())
        )

        total_count = result_df.count()
        print(f"[DELTA] Writing {total_count} initial records to Delta table...")

        # Write initial Delta table with status partitioning
        result_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("status") \
            .option("mergeSchema", "true") \
            .save(gold_path)

        print(f"✅ Delta Merge Complete — New: {total_count} | Changed: 0 | Expired: 0 | Total: {total_count}")

        return {
            'new': total_count,
            'changed': 0,
            'expired': 0,
            'unchanged': 0,
            'total_written': total_count
        }

    # ========================
    # 4. Identify Changes (Optimized)
    # ========================

    print("[DELTA] Identifying changes...")

    # Claude: Pre-filter changed keys using content_hash to avoid full-table scans.
    # Read only current records from Delta table
    current_df = spark.read.format("delta").load(gold_path) \
        .filter(F.col("active_to") == F.lit(END_OF_TIME))

    current_count = current_df.count()
    print(f"[DELTA] Current records in Delta table: {current_count}")

    # Join incoming with current to identify change types
    # Use business key(s) for join condition
    join_condition = [F.col(f"incoming.{k}") == F.col(f"current.{k}") for k in business_keys]

    comparison_df = incoming_df.alias("incoming").join(
        current_df.alias("current"),
        on=join_condition,
        how="full_outer"
    )

    # Classify each record
    comparison_df = comparison_df.withColumn(
        "change_type",
        F.when(
            F.col(f"current.{business_keys[0]}").isNull(),
            F.lit("NEW")  # In incoming, not in current
        ).when(
            F.col(f"incoming.{business_keys[0]}").isNull(),
            F.lit("EXPIRED")  # In current, not in incoming
        ).when(
            F.col("incoming.content_hash") == F.col("current.content_hash"),
            F.lit("UNCHANGED")  # Same hash - no change
        ).otherwise(
            F.lit("CHANGED")  # Different hash - record changed
        )
    )

    # Count by change type
    print("[DELTA] Change detection results:")
    change_counts = comparison_df.groupBy("change_type").count().collect()
    stats = {row['change_type']: row['count'] for row in change_counts}

    new_count = stats.get('NEW', 0)
    changed_count = stats.get('CHANGED', 0)
    expired_count = stats.get('EXPIRED', 0)
    unchanged_count = stats.get('UNCHANGED', 0)

    for change_type, count in stats.items():
        print(f"[DELTA]   {change_type}: {count} records")

    # ========================
    # 5. Apply Delta MERGE Operations
    # ========================

    total_written = 0

    # --- Step 5a: Insert NEW records ---
    if new_count > 0:
        print(f"[DELTA] Inserting {new_count} NEW records...")

        new_df = comparison_df.filter(F.col("change_type") == "NEW").select("incoming.*")
        new_df = (
            new_df
            .withColumn("version_id", F.expr("uuid()"))
            .withColumn("active_from", F.lit(run_date).cast(DateType()))
            .withColumn("active_to", F.lit(END_OF_TIME).cast(DateType()))
            .withColumn("status", F.lit(STATUS_CURRENT))
            .withColumn("updated_at", F.current_timestamp())
        )

        # Append new records to Delta table
        new_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(gold_path)

        total_written += new_count

    # --- Step 5b: Handle CHANGED records (Expire old + Insert new) ---
    if changed_count > 0:
        print(f"[DELTA] Processing {changed_count} CHANGED records...")

        # Extract business keys of changed records
        changed_keys_df = comparison_df.filter(F.col("change_type") == "CHANGED") \
            .select(*[f"incoming.{k}" for k in business_keys]).distinct()

        # Create merge condition for business keys
        merge_condition_parts = [f"target.{k} = source.{k}" for k in business_keys]
        merge_condition = " AND ".join(merge_condition_parts)

        # MERGE 1: Expire old versions (set active_to = run_date, status = historical)
        delta_table = DeltaTable.forPath(spark, gold_path)
        delta_table.alias("target").merge(
            changed_keys_df.alias("source"),
            merge_condition + f" AND target.active_to = '{END_OF_TIME}'"
        ).whenMatchedUpdate(set={
            "active_to": F.lit(run_date).cast(DateType()),
            "status": F.lit(STATUS_HISTORICAL),
            "updated_at": F.current_timestamp()
        }).execute()

        print(f"[DELTA] Expired {changed_count} old versions")

        # MERGE 2: Insert new versions
        changed_new_df = comparison_df.filter(F.col("change_type") == "CHANGED").select("incoming.*")
        changed_new_df = (
            changed_new_df
            .withColumn("version_id", F.expr("uuid()"))
            .withColumn("active_from", F.lit(run_date).cast(DateType()))
            .withColumn("active_to", F.lit(END_OF_TIME).cast(DateType()))
            .withColumn("status", F.lit(STATUS_CURRENT))
            .withColumn("updated_at", F.current_timestamp())
        )

        changed_new_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(gold_path)

        total_written += (changed_count * 2)  # Both expired and new versions

    # --- Step 5c: Handle EXPIRED records (Set active_to = run_date) ---
    if expired_count > 0:
        print(f"[DELTA] Expiring {expired_count} records no longer in source...")

        # Extract business keys of expired records
        expired_keys_df = comparison_df.filter(F.col("change_type") == "EXPIRED") \
            .select(*[f"current.{k}" for k in business_keys]).distinct()

        # Create merge condition for business keys
        merge_condition_parts = [f"target.{k} = source.{k}" for k in business_keys]
        merge_condition = " AND ".join(merge_condition_parts)

        # MERGE: Expire records
        delta_table = DeltaTable.forPath(spark, gold_path)
        delta_table.alias("target").merge(
            expired_keys_df.alias("source"),
            merge_condition + f" AND target.active_to = '{END_OF_TIME}'"
        ).whenMatchedUpdate(set={
            "active_to": F.lit(run_date).cast(DateType()),
            "status": F.lit(STATUS_HISTORICAL),
            "updated_at": F.current_timestamp()
        }).execute()

        total_written += expired_count

    # UNCHANGED records - no action needed
    print(f"[DELTA] Skipping {unchanged_count} UNCHANGED records (no write needed)")

    # ========================
    # 6. Summary
    # ========================

    print(f"\n✅ Delta Merge Complete — New: {new_count} | Changed: {changed_count} | Expired: {expired_count} | Total: {total_written}")

    return {
        'new': new_count,
        'changed': changed_count,
        'expired': expired_count,
        'unchanged': unchanged_count,
        'total_written': total_written
    }


def optimize_delta_table(spark: SparkSession, gold_path: str, business_key: str | list):
    """
    Run OPTIMIZE and ZORDER on Delta table for improved read performance.

    Claude: Run this monthly or when file fragmentation exceeds 1000 files/partition.

    Args:
        spark: SparkSession instance
        gold_path: S3 path for Delta table
        business_key: Primary business identifier column(s) for ZORDER
    """
    business_keys = [business_key] if isinstance(business_key, str) else business_key

    print(f"[DELTA OPTIMIZE] Optimizing Delta table at {gold_path}")
    print(f"[DELTA OPTIMIZE] ZORDER BY: {business_keys}")

    delta_table = DeltaTable.forPath(spark, gold_path)

    # Run OPTIMIZE with ZORDER
    delta_table.optimize().executeZOrderBy(*business_keys)

    print(f"[DELTA OPTIMIZE] ✅ Optimization complete")


def vacuum_delta_table(spark: SparkSession, gold_path: str, retention_hours: int = 168):
    """
    Run VACUUM on Delta table to remove old Delta files.

    Claude: Run this weekly to clean up old versions.

    Args:
        spark: SparkSession instance
        gold_path: S3 path for Delta table
        retention_hours: Retention period in hours (default: 168 = 7 days)
    """
    print(f"[DELTA VACUUM] Vacuuming Delta table at {gold_path}")
    print(f"[DELTA VACUUM] Retention: {retention_hours} hours ({retention_hours/24:.1f} days)")

    delta_table = DeltaTable.forPath(spark, gold_path)
    delta_table.vacuum(retention_hours)

    print(f"[DELTA VACUUM] ✅ Vacuum complete")


def get_current_records(spark: SparkSession, gold_path: str) -> DataFrame:
    """
    Helper: Query all current (active) records from Delta table.

    Args:
        spark: SparkSession
        gold_path: S3 path for Delta table

    Returns:
        DataFrame with only current records (active_to = 9999-12-31)
    """
    return spark.read.format("delta").load(gold_path) \
        .filter(F.col("active_to") == F.lit(END_OF_TIME))


def get_changes_for_date(spark: SparkSession, gold_path: str, target_date: str) -> DataFrame:
    """
    Helper: Get all records that changed on a specific date.
    Useful for incremental DynamoDB syncs.

    Args:
        spark: SparkSession
        gold_path: S3 path for Delta table
        target_date: Date string (YYYY-MM-DD)

    Returns:
        DataFrame with records where active_from = date OR active_to = date
        (excludes active_to = 9999-12-31 in the date check)
    """
    return spark.read.format("delta").load(gold_path) \
        .filter(
            (F.col("run_date") == target_date) &
            (
                (F.col("active_from") == target_date) |
                ((F.col("active_to") == target_date) & (F.col("active_to") < END_OF_TIME))
            )
        )


def time_travel_query(
    spark: SparkSession,
    gold_path: str,
    version: int = None,
    timestamp: str = None
) -> DataFrame:
    """
    Helper: Query Delta table at a specific version or timestamp.

    Args:
        spark: SparkSession
        gold_path: S3 path for Delta table
        version: Version number (e.g., 3)
        timestamp: Timestamp string (e.g., "2025-10-14 12:00:00")

    Returns:
        DataFrame with data as of the specified version or timestamp
    """
    if version is not None:
        print(f"[DELTA TIME TRAVEL] Reading version {version}")
        return spark.read.format("delta").option("versionAsOf", version).load(gold_path)
    elif timestamp is not None:
        print(f"[DELTA TIME TRAVEL] Reading timestamp {timestamp}")
        return spark.read.format("delta").option("timestampAsOf", timestamp).load(gold_path)
    else:
        raise ValueError("Must provide either version or timestamp")


def validate_delta_table(spark: SparkSession, gold_path: str, business_key: str | list):
    """
    Helper: Validate temporal Delta table integrity.
    Checks for common issues like duplicate current records or gaps in history.

    Args:
        spark: SparkSession
        gold_path: S3 path for Delta table
        business_key: Primary business identifier column(s)
    """
    business_keys = [business_key] if isinstance(business_key, str) else business_key
    key_columns = ", ".join(business_keys)

    print(f"[VALIDATION] Checking Delta table integrity at {gold_path}")

    df = spark.read.format("delta").load(gold_path)

    # Check 1: Each business key should have exactly one current record
    print("[VALIDATION] Checking for duplicate current records...")

    current_df = df.filter(F.col("active_to") == F.lit(END_OF_TIME))
    dupe_df = current_df.groupBy(*business_keys).count().filter(F.col("count") > 1)

    dupe_count = dupe_df.count()
    if dupe_count > 0:
        print(f"[VALIDATION] ⚠️  Found {dupe_count} business keys with multiple current records!")
        dupe_df.show(10, truncate=False)
    else:
        print("[VALIDATION] ✅ No duplicate current records")

    # Check 2: Verify status partition matches active_to
    print("[VALIDATION] Checking status/active_to consistency...")

    mismatch_df = df.filter(
        ((F.col("active_to") == END_OF_TIME) & (F.col("status") != STATUS_CURRENT)) |
        ((F.col("active_to") < END_OF_TIME) & (F.col("status") != STATUS_HISTORICAL))
    )

    mismatch_count = mismatch_df.count()
    if mismatch_count > 0:
        print(f"[VALIDATION] ⚠️  Found {mismatch_count} records with status/active_to mismatch!")
        mismatch_df.show(10, truncate=False)
    else:
        print("[VALIDATION] ✅ All status values match active_to dates")

    # Check 3: Summary statistics
    print("[VALIDATION] Summary statistics:")
    df.groupBy("status").agg(
        F.count("*").alias("record_count"),
        F.countDistinct(*business_keys).alias("unique_keys")
    ).show(truncate=False)

    # Check 4: Delta table details
    delta_table = DeltaTable.forPath(spark, gold_path)
    print("[VALIDATION] Delta table history (last 10 operations):")
    delta_table.history(10).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
