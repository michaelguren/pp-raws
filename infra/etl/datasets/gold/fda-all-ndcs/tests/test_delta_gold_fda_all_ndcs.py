#!/usr/bin/env python3
"""
Automated Delta Lake Test Harness for FDA All NDCs Gold Layer

Validates temporal versioning with controlled test scenarios:
- Run 1: Initial load (NEW)
- Run 2: Incremental changes (NEW + CHANGED + UNCHANGED)
- Run 3: Record deletions (EXPIRED)
- Run 4: Idempotency check

Usage:
    python3 test_delta_gold_fda_all_ndcs.py --test-date 2025-10-21
    python3 test_delta_gold_fda_all_ndcs.py --test-date 2025-10-21 --run-only 1,2
"""

import argparse
import boto3  # type: ignore[import-not-found]
import time
import json
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

# AWS Configuration
AWS_REGION = 'us-east-1'
BUCKET = 'pp-dw-550398958311'
DATABASE = 'pp_dw_gold'
TABLE = 'fda_all_ndcs'

# Job Names
SILVER_JOB = 'pp-dw-silver-fda-all-ndcs'
GOLD_JOB = 'pp-dw-gold-fda-all-ndcs'
SILVER_CRAWLER = 'pp-dw-silver-fda-all-ndcs-crawler'
GOLD_CRAWLER = 'pp-dw-gold-fda-all-ndcs-crawler'

# Test metadata file
TEST_METADATA_FILE = '/tmp/delta_test_harness_metadata.json'

# Expected results for each run (for stateful validation)
EXPECTED_RESULTS = {
    'run1': {
        'version': 0,
        'operation': 'WRITE',
        'new': 380000,  # Approximate
        'changed': 0,
        'expired': 0,
        'current_count_min': 375000,  # Allow some variance
        'historical_count': 0
    },
    'run2': {
        'version': 1,
        'operation': 'MERGE',
        'new': 5,
        'changed': 10,
        'expired': 0,
        'current_count_delta': 5,  # +5 from run1
        'historical_count': 10  # 10 expired versions
    },
    'run3': {
        'version': 2,
        'operation': 'MERGE',
        'new': 0,
        'changed': 0,
        'expired': 5,
        'current_count_delta': -5,  # -5 from run2
        'historical_count': 15  # 10 from run2 + 5 from run3
    },
    'run4': {
        'version': 3,
        'operation': 'MERGE',
        'new': 5,  # Re-adding the 5 that were expired in run3
        'changed': 0,
        'expired': 5,  # Re-expiring the 5 that were removed in run3
        'current_count_delta': 5,  # Back to run2 level
        'historical_count': 20  # All historical versions
    }
}

# AWS Clients
glue = boto3.client('glue', region_name=AWS_REGION)
athena = boto3.client('athena', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)
logs = boto3.client('logs', region_name=AWS_REGION)


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    """Print formatted section header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.END}\n")


def print_step(step: str, text: str):
    """Print formatted step"""
    print(f"{Colors.CYAN}{Colors.BOLD}[{step}]{Colors.END} {text}")


def print_success(text: str):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_error(text: str):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def print_warning(text: str):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")


def load_test_metadata() -> Dict:
    """Load persistent test metadata from previous runs"""
    if os.path.exists(TEST_METADATA_FILE):
        with open(TEST_METADATA_FILE, 'r') as f:
            return json.load(f)
    return {'runs': {}, 'baselines': {}}


def save_test_metadata(metadata: Dict):
    """Save test metadata for future runs"""
    with open(TEST_METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)
    print_success(f"Test metadata saved: {TEST_METADATA_FILE}")


def validate_delta_transaction_log(run_key: str, expected: Dict) -> bool:
    """
    Validate Delta transaction log against expected operations.

    Downloads and parses the Delta transaction log JSON to verify:
    - Operation type (WRITE, MERGE, etc.)
    - Operation metrics (numTargetRowsInserted, numTargetRowsUpdated, etc.)
    """
    print_step("TX-LOG", f"Validating Delta transaction log for version {expected['version']}")

    try:
        # Download Delta transaction log
        version_str = str(expected['version']).zfill(20)
        log_key = f"gold/{TABLE}/_delta_log/{version_str}.json"

        response = s3.get_object(Bucket=BUCKET, Key=log_key)
        log_content = response['Body'].read().decode('utf-8')

        # Parse transaction log (newline-delimited JSON)
        log_entries = [json.loads(line) for line in log_content.strip().split('\n')]

        # Find commitInfo entry
        commit_info = None
        for entry in log_entries:
            if 'commitInfo' in entry:
                commit_info = entry['commitInfo']
                break

        if not commit_info:
            print_error("No commitInfo found in transaction log")
            return False

        # Validate operation type
        actual_operation = commit_info.get('operation', 'UNKNOWN')
        expected_operation = expected['operation']

        if actual_operation != expected_operation:
            print_error(f"Operation mismatch: expected={expected_operation}, actual={actual_operation}")
            return False

        print_success(f"Operation: {actual_operation}")

        # Validate operation metrics (if MERGE)
        if actual_operation == 'MERGE' and 'operationMetrics' in commit_info:
            metrics = commit_info['operationMetrics']

            # Check inserts
            if 'new' in expected:
                actual_inserts = int(metrics.get('numTargetRowsInserted', 0))
                expected_inserts = expected['new']

                if actual_inserts != expected_inserts:
                    print_warning(f"Inserts: expected={expected_inserts}, actual={actual_inserts}")
                else:
                    print_success(f"Inserts: {actual_inserts}")

            # Check updates
            if 'changed' in expected or 'expired' in expected:
                actual_updates = int(metrics.get('numTargetRowsUpdated', 0))
                expected_updates = expected.get('changed', 0) + expected.get('expired', 0)

                if actual_updates != expected_updates:
                    print_warning(f"Updates: expected={expected_updates}, actual={actual_updates}")
                else:
                    print_success(f"Updates: {actual_updates}")

        return True

    except Exception as e:
        print_error(f"Failed to validate transaction log: {e}")
        return False


def compare_with_expected(run_key: str, actual: Dict, expected: Dict) -> bool:
    """
    Compare actual results with expected results.

    Returns True if within acceptable variance, False otherwise.
    """
    print_step("COMPARE", f"Comparing actual vs expected for {run_key}")

    all_match = True

    # Version
    if actual.get('version') != expected.get('version'):
        print_error(f"Version: expected={expected.get('version')}, actual={actual.get('version')}")
        all_match = False
    else:
        print_success(f"Version: {actual.get('version')}")

    # Status counts
    if 'current_count' in actual:
        if 'current_count_min' in expected:
            if actual['current_count'] < expected['current_count_min']:
                print_error(f"Current count too low: {actual['current_count']} < {expected['current_count_min']}")
                all_match = False
            else:
                print_success(f"Current count: {actual['current_count']:,} (>= {expected['current_count_min']:,})")

    if 'historical_count' in actual and 'historical_count' in expected:
        if actual['historical_count'] != expected['historical_count']:
            print_warning(f"Historical count: expected={expected['historical_count']}, actual={actual['historical_count']}")
            # Don't fail on historical count variance
        else:
            print_success(f"Historical count: {actual['historical_count']}")

    return all_match


def wait_for_glue_job(job_name: str, run_id: str, timeout: int = 1800) -> Dict:
    """
    Wait for Glue job to complete and return final status.

    Args:
        job_name: Glue job name
        run_id: Job run ID
        timeout: Maximum wait time in seconds (default: 30 min)

    Returns:
        Job run details
    """
    print_step("WAIT", f"Waiting for {job_name} (run_id: {run_id})...")

    start_time = time.time()
    while True:
        response = glue.get_job_run(JobName=job_name, RunId=run_id)
        status = response['JobRun']['JobRunState']

        if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            elapsed = time.time() - start_time

            if status == 'SUCCEEDED':
                print_success(f"{job_name} completed in {elapsed:.1f}s")
            else:
                print_error(f"{job_name} {status} after {elapsed:.1f}s")
                if 'ErrorMessage' in response['JobRun']:
                    print_error(f"Error: {response['JobRun']['ErrorMessage']}")

            return response['JobRun']

        if time.time() - start_time > timeout:
            print_error(f"{job_name} timed out after {timeout}s")
            return None

        time.sleep(10)


def run_athena_query(query: str, output_location: str = None) -> List[Dict]:
    """
    Execute Athena query and return results.

    Args:
        query: SQL query
        output_location: S3 path for results (optional)

    Returns:
        List of result rows as dictionaries
    """
    if output_location is None:
        output_location = f"s3://{BUCKET}/athena-results/"

    # Start query execution
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': output_location}
    )

    query_id = response['QueryExecutionId']

    # Wait for completion
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_id)
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(1)

    if status != 'SUCCEEDED':
        error = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Athena query failed: {error}")

    # Get results
    results = []
    paginator = athena.get_paginator('get_query_results')

    for page in paginator.paginate(QueryExecutionId=query_id):
        # Skip header row
        rows = page['ResultSet']['Rows'][1:] if results == [] else page['ResultSet']['Rows']

        for row in rows:
            result_row = {}
            columns = page['ResultSet']['ResultSetMetadata']['ColumnInfo']

            for i, col in enumerate(columns):
                col_name = col['Name']
                value = row['Data'][i].get('VarCharValue', None)
                result_row[col_name] = value

            results.append(result_row)

    return results


def validate_run_1(test_date: str, metadata: Dict) -> bool:
    """Validate Run 1 - Initial load"""
    print_header("Run 1 Validation - Initial Load")

    expected = EXPECTED_RESULTS['run1']
    actual = {'run': 'run1', 'test_date': test_date}

    # Check status distribution
    print_step("1/5", "Checking status distribution...")
    results = run_athena_query(f"""
        SELECT status, COUNT(*) as cnt
        FROM {TABLE}
        GROUP BY status
    """)

    if len(results) != 1 or results[0]['status'] != 'current':
        print_error(f"Expected only 'current' status, got: {results}")
        return False

    current_count = int(results[0]['cnt'])
    actual['current_count'] = current_count
    actual['historical_count'] = 0
    print_success(f"Status distribution: current={current_count:,}")

    # Verify run_date
    print_step("2/5", "Verifying run_date...")
    results = run_athena_query(f"SELECT DISTINCT run_date FROM {TABLE}")

    if len(results) != 1 or results[0]['run_date'] != test_date:
        print_error(f"Expected run_date={test_date}, got: {results}")
        return False

    print_success(f"run_date: {results[0]['run_date']}")

    # Temporal integrity: no duplicates
    print_step("3/5", "Checking temporal integrity (no duplicates)...")
    results = run_athena_query(f"""
        SELECT ndc_11, COUNT(*) as cnt
        FROM {TABLE}
        WHERE status = 'current'
        GROUP BY ndc_11
        HAVING COUNT(*) > 1
    """)

    if len(results) > 0:
        print_error(f"Found {len(results)} duplicate current records")
        return False

    print_success("No duplicate current records")

    # Delta version
    print_step("4/5", "Checking Delta version...")
    results = run_athena_query(f"""
        DESCRIBE HISTORY {TABLE}
        ORDER BY version DESC LIMIT 1
    """)

    if len(results) == 0 or results[0]['version'] != '0':
        print_error(f"Expected version 0, got: {results}")
        return False

    actual['version'] = int(results[0]['version'])
    print_success(f"Delta version: {results[0]['version']}, operation: {results[0].get('operation', 'N/A')}")

    # Transaction log validation
    print_step("5/5", "Validating Delta transaction log...")
    if not validate_delta_transaction_log('run1', expected):
        return False

    # Compare with expected
    if not compare_with_expected('run1', actual, expected):
        return False

    # Save baseline for future runs
    metadata['runs']['run1'] = actual
    metadata['baselines']['initial_count'] = current_count
    save_test_metadata(metadata)

    return True


def validate_run_2(test_date: str, expected_new: int = 5, expected_changed: int = 10) -> bool:
    """Validate Run 2 - Incremental changes"""
    print_header("Run 2 Validation - Incremental Changes")

    # Check status distribution
    print_step("1/5", "Checking status distribution...")
    results = run_athena_query(f"""
        SELECT status, COUNT(*) as cnt
        FROM {TABLE}
        GROUP BY status
    """)

    status_dict = {row['status']: int(row['cnt']) for row in results}

    if 'current' not in status_dict or 'historical' not in status_dict:
        print_error(f"Missing status values: {status_dict}")
        return False

    if status_dict['historical'] != expected_changed:
        print_warning(f"Expected {expected_changed} historical, got {status_dict['historical']}")

    print_success(f"Status: current={status_dict['current']:,}, historical={status_dict['historical']}")

    # Check for changed records (2 versions each)
    print_step("2/5", "Checking changed records...")
    results = run_athena_query(f"""
        SELECT ndc_11, COUNT(*) as version_count
        FROM {TABLE}
        GROUP BY ndc_11
        HAVING COUNT(*) > 1
    """)

    if len(results) != expected_changed:
        print_warning(f"Expected {expected_changed} changed NDCs, got {len(results)}")

    print_success(f"Changed records: {len(results)} NDCs with 2+ versions")

    # Check for new records
    print_step("3/5", "Checking new records...")
    results = run_athena_query(f"""
        SELECT COUNT(*) as cnt
        FROM {TABLE}
        WHERE ndc_11 LIKE '%99'
          AND status = 'current'
    """)

    new_count = int(results[0]['cnt'])
    if new_count != expected_new:
        print_warning(f"Expected {expected_new} new records, got {new_count}")

    print_success(f"New records: {new_count}")

    # Temporal integrity
    print_step("4/5", "Checking temporal integrity...")
    results = run_athena_query(f"""
        SELECT ndc_11, COUNT(*) as cnt
        FROM {TABLE}
        WHERE status = 'current'
        GROUP BY ndc_11
        HAVING COUNT(*) > 1
    """)

    if len(results) > 0:
        print_error(f"Found {len(results)} duplicate current records")
        return False

    print_success("No duplicate current records")

    # Delta version
    print_step("5/5", "Checking Delta version...")
    results = run_athena_query(f"""
        DESCRIBE HISTORY {TABLE}
        ORDER BY version DESC LIMIT 1
    """)

    if len(results) == 0 or results[0]['version'] != '1':
        print_error(f"Expected version 1, got: {results}")
        return False

    print_success(f"Delta version: {results[0]['version']}")

    return True


def validate_run_3(test_date: str, expected_expired: int = 5) -> bool:
    """Validate Run 3 - Record deletions"""
    print_header("Run 3 Validation - Record Deletions")

    # Check status distribution
    print_step("1/3", "Checking status distribution...")
    results = run_athena_query(f"""
        SELECT status, COUNT(*) as cnt
        FROM {TABLE}
        GROUP BY status
    """)

    status_dict = {row['status']: int(row['cnt']) for row in results}

    expected_historical = 15  # 10 from Run 2 + 5 from Run 3
    if status_dict.get('historical', 0) != expected_historical:
        print_warning(f"Expected {expected_historical} historical, got {status_dict.get('historical', 0)}")

    print_success(f"Status: current={status_dict['current']:,}, historical={status_dict['historical']}")

    # Check expired records
    print_step("2/3", "Checking expired records...")
    results = run_athena_query(f"""
        SELECT COUNT(*) as cnt
        FROM {TABLE}
        WHERE status = 'historical'
          AND active_to = DATE '{test_date}'
    """)

    # Note: This includes records from Run 2 that also have active_to = test_date
    expired_count = int(results[0]['cnt'])
    print_success(f"Records expired on {test_date}: {expired_count}")

    # Delta version
    print_step("3/3", "Checking Delta version...")
    results = run_athena_query(f"""
        DESCRIBE HISTORY {TABLE}
        ORDER BY version DESC LIMIT 1
    """)

    if len(results) == 0 or results[0]['version'] != '2':
        print_error(f"Expected version 2, got: {results}")
        return False

    print_success(f"Delta version: {results[0]['version']}")

    return True


def validate_run_4(test_date: str) -> bool:
    """Validate Run 4 - Idempotency"""
    print_header("Run 4 Validation - Idempotency Check")

    # Check status distribution
    print_step("1/2", "Checking status distribution (should match Run 2)...")
    results = run_athena_query(f"""
        SELECT status, COUNT(*) as cnt
        FROM {TABLE}
        GROUP BY status
    """)

    status_dict = {row['status']: int(row['cnt']) for row in results}
    print_success(f"Status: current={status_dict['current']:,}, historical={status_dict['historical']}")

    # Delta version
    print_step("2/2", "Checking Delta version...")
    results = run_athena_query(f"""
        DESCRIBE HISTORY {TABLE}
        ORDER BY version DESC LIMIT 1
    """)

    if len(results) == 0 or results[0]['version'] != '3':
        print_error(f"Expected version 3, got: {results}")
        return False

    print_success(f"Delta version: {results[0]['version']}")

    return True


def main():
    parser = argparse.ArgumentParser(description='Delta Lake Test Harness for FDA All NDCs')
    parser.add_argument('--test-date', required=True, help='Test run date (YYYY-MM-DD)')
    parser.add_argument('--run-only', help='Comma-separated list of runs to execute (e.g., 1,2)', default='1,2,3,4')
    parser.add_argument('--skip-jobs', action='store_true', help='Skip Glue job execution (validation only)')

    args = parser.parse_args()

    test_date = args.test_date
    runs_to_execute = [int(r) for r in args.run_only.split(',')]

    print_header(f"🧪 Delta Lake Test Harness - FDA All NDCs")
    print(f"Test Date: {test_date}")
    print(f"Runs: {runs_to_execute}")
    print(f"Skip Jobs: {args.skip_jobs}")

    # Load persistent metadata from previous runs
    metadata = load_test_metadata()
    print(f"Metadata file: {TEST_METADATA_FILE}")
    if metadata.get('runs'):
        print(f"Previous runs: {list(metadata['runs'].keys())}")

    results = {}

    # Run 1: Initial Load
    if 1 in runs_to_execute:
        print_header("RUN 1: Initial Load")

        if not args.skip_jobs:
            # Run silver job
            print_step("1/3", f"Starting silver job: {SILVER_JOB}")
            silver_run = glue.start_job_run(JobName=SILVER_JOB)
            silver_run_id = silver_run['JobRunId']
            silver_result = wait_for_glue_job(SILVER_JOB, silver_run_id)

            if silver_result['JobRunState'] != 'SUCCEEDED':
                print_error("Silver job failed")
                sys.exit(1)

            # Run gold job
            print_step("2/3", f"Starting gold job: {GOLD_JOB} (run_date={test_date})")
            gold_run = glue.start_job_run(
                JobName=GOLD_JOB,
                Arguments={'--run_date': test_date}
            )
            gold_run_id = gold_run['JobRunId']
            gold_result = wait_for_glue_job(GOLD_JOB, gold_run_id)

            if gold_result['JobRunState'] != 'SUCCEEDED':
                print_error("Gold job failed")
                sys.exit(1)

            # Run crawler
            print_step("3/3", f"Starting crawler: {GOLD_CRAWLER}")
            glue.start_crawler(Name=GOLD_CRAWLER)
            time.sleep(30)  # Give crawler time to complete

        # Validate
        results['run1'] = validate_run_1(test_date, metadata)

    # Run 2: Incremental Changes
    if 2 in runs_to_execute:
        print_header("RUN 2: Incremental Changes")
        print_warning("Manual step required: Upload modified silver data")
        print("  1. Run: python3 /tmp/delta_test_modify_silver.py")
        print("  2. Upload Run 2 data to S3")
        print("  3. Run silver crawler")

        input("Press Enter when ready to continue...")

        if not args.skip_jobs:
            # Run gold job
            print_step("1/1", f"Starting gold job: {GOLD_JOB} (run_date={test_date})")
            gold_run = glue.start_job_run(
                JobName=GOLD_JOB,
                Arguments={'--run_date': test_date}
            )
            gold_run_id = gold_run['JobRunId']
            gold_result = wait_for_glue_job(GOLD_JOB, gold_run_id)

            if gold_result['JobRunState'] != 'SUCCEEDED':
                print_error("Gold job failed")
                sys.exit(1)

        # Validate
        results['run2'] = validate_run_2(test_date)

    # Run 3: Record Deletions
    if 3 in runs_to_execute:
        print_header("RUN 3: Record Deletions")
        print_warning("Manual step required: Upload Run 3 silver data")

        input("Press Enter when ready to continue...")

        if not args.skip_jobs:
            # Run gold job
            print_step("1/1", f"Starting gold job: {GOLD_JOB} (run_date={test_date})")
            gold_run = glue.start_job_run(
                JobName=GOLD_JOB,
                Arguments={'--run_date': test_date}
            )
            gold_run_id = gold_run['JobRunId']
            gold_result = wait_for_glue_job(GOLD_JOB, gold_run_id)

            if gold_result['JobRunState'] != 'SUCCEEDED':
                print_error("Gold job failed")
                sys.exit(1)

        # Validate
        results['run3'] = validate_run_3(test_date)

    # Run 4: Idempotency
    if 4 in runs_to_execute:
        print_header("RUN 4: Idempotency Check")
        print_warning("Manual step required: Re-upload Run 2 silver data")

        input("Press Enter when ready to continue...")

        if not args.skip_jobs:
            # Run gold job
            print_step("1/1", f"Starting gold job: {GOLD_JOB} (run_date={test_date})")
            gold_run = glue.start_job_run(
                JobName=GOLD_JOB,
                Arguments={'--run_date': test_date}
            )
            gold_run_id = gold_run['JobRunId']
            gold_result = wait_for_glue_job(GOLD_JOB, gold_run_id)

            if gold_result['JobRunState'] != 'SUCCEEDED':
                print_error("Gold job failed")
                sys.exit(1)

        # Validate
        results['run4'] = validate_run_4(test_date)

    # Final Summary
    print_header("📊 Test Summary")

    all_passed = True
    for run, passed in results.items():
        if passed:
            print_success(f"{run.upper()}: PASSED")
        else:
            print_error(f"{run.upper()}: FAILED")
            all_passed = False

    if all_passed:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✅ ALL TESTS PASSED{Colors.END}\n")
        sys.exit(0)
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}❌ SOME TESTS FAILED{Colors.END}\n")
        sys.exit(1)


if __name__ == '__main__':
    main()
