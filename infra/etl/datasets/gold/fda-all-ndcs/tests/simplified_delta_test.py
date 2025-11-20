#!/usr/bin/env python3
"""
Simplified Delta Lake Confidence Test for FDA All NDCs Gold Layer

DESIGN PHILOSOPHY: Simple. Focused. Reliable.
- No Delta transaction log parsing
- No complex version checking
- Just: insert synthetic data → run Gold job → query counts → validate expectations
- Uses Athena for all validations (simple SQL queries)

SAFETY:
- Uses NDCs starting with '99999' (never exist in real FDA data)
- Uses far-future run_dates (2050-XX-XX)
- Can be run repeatedly without breaking production data

TRIGGER VIA AWS CLI:
    # Scenario 1: ADDs (10 new records)
    aws glue start-job-run --job-name pp-dw-gold-fda-all-ndcs-simple-test \
      --arguments '{"--scenario":"add","--run_date":"2050-01-01"}'

    # Scenario 2: UPDATES (5 modified records)
    aws glue start-job-run --job-name pp-dw-gold-fda-all-ndcs-simple-test \
      --arguments '{"--scenario":"update","--run_date":"2050-01-02"}'

    # Scenario 3: TERMS (3 deleted records)
    aws glue start-job-run --job-name pp-dw-gold-fda-all-ndcs-simple-test \
      --arguments '{"--scenario":"term","--run_date":"2050-01-03"}'
"""

import boto3
import time
import sys
import os
from typing import Dict, List, Optional
from datetime import datetime

# AWS Glue imports
from awsglue.utils import getResolvedOptions

# ============================================================================
# Configuration
# ============================================================================

# Get configuration from Glue job arguments
args = getResolvedOptions(sys.argv, [
    'AWS_REGION', 'BUCKET', 'GOLD_DATABASE', 'SILVER_DATABASE',
    'TABLE', 'scenario', 'run_date'
])

AWS_REGION = args['AWS_REGION']
BUCKET = args['BUCKET']
GOLD_DATABASE = args['GOLD_DATABASE']
SILVER_DATABASE = args['SILVER_DATABASE']
TABLE = args['TABLE']
SCENARIO = args['scenario']  # 'add', 'update', or 'term'
RUN_DATE = args['run_date']  # e.g., '2050-01-01'

# Job/Crawler names
SILVER_CRAWLER = 'pp-dw-silver-fda-all-ndcs-crawler'
GOLD_JOB = 'pp-dw-gold-fda-all-ndcs'

# Canary NDC prefix (guaranteed never to exist in real FDA data)
CANARY_PREFIX = '99999'

# AWS Clients
glue = boto3.client('glue', region_name=AWS_REGION)
athena = boto3.client('athena', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

# ============================================================================
# Terminal Colors
# ============================================================================

class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.END}\n")

def print_step(step: str, text: str):
    print(f"{Colors.CYAN}{Colors.BOLD}[{step}]{Colors.END} {text}")

def print_success(text: str):
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_error(text: str):
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def print_warning(text: str):
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")

# ============================================================================
# Athena Query Utilities
# ============================================================================

def run_athena_query(query: str) -> List[Dict]:
    """
    Execute Athena query and return results as list of dicts.
    Simple. No pagination complexity for these small result sets.
    """
    output_location = f"s3://{BUCKET}/athena-results/"

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': GOLD_DATABASE},
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
        raise Exception(f"Athena query failed: {error}\nQuery: {query}")

    # Get results
    results = []
    result_response = athena.get_query_results(QueryExecutionId=query_id)

    rows = result_response['ResultSet']['Rows']
    if len(rows) <= 1:  # Only header or empty
        return []

    # Parse header
    headers = [col['VarCharValue'] for col in rows[0]['Data']]

    # Parse data rows
    for row in rows[1:]:
        result_row = {}
        for i, col in enumerate(row['Data']):
            result_row[headers[i]] = col.get('VarCharValue', None)
        results.append(result_row)

    return results

# ============================================================================
# Glue Job/Crawler Utilities
# ============================================================================

def wait_for_glue_job(job_name: str, run_id: str, timeout: int = 1800) -> Dict:
    """Wait for Glue job to complete and return final status"""
    print_step("WAIT", f"Waiting for {job_name} to complete...")

    start_time = time.time()
    last_status = None

    while True:
        response = glue.get_job_run(JobName=job_name, RunId=run_id)
        status = response['JobRun']['JobRunState']

        if status != last_status:
            print(f"  Status: {status}")
            last_status = status

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
            raise Exception(f"{job_name} timed out after {timeout}s")

        time.sleep(10)

def wait_for_crawler(crawler_name: str, timeout: int = 600):
    """Wait for crawler to complete"""
    print_step("CRAWLER", f"Starting {crawler_name}...")

    try:
        glue.start_crawler(Name=crawler_name)
    except glue.exceptions.CrawlerRunningException:
        print_warning("Crawler already running, waiting for completion...")

    start_time = time.time()
    while True:
        response = glue.get_crawler(Name=crawler_name)
        state = response['Crawler']['State']

        if state == 'READY':
            last_run_status = response['Crawler'].get('LastCrawl', {}).get('Status', 'UNKNOWN')
            if last_run_status == 'SUCCEEDED':
                print_success(f"Crawler completed successfully")
            else:
                print_warning(f"Crawler completed with status: {last_run_status}")
            return

        if time.time() - start_time > timeout:
            raise Exception(f"Crawler {crawler_name} timed out after {timeout}s")

        time.sleep(5)

# ============================================================================
# Baseline Queries
# ============================================================================

def query_baseline_counts() -> Dict:
    """Query current state of Gold table before test"""
    print_header("Step 1: Query Baseline Counts")

    # Overall status distribution
    results = run_athena_query(f"""
        SELECT status, COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        GROUP BY status
    """)

    baseline = {'current': 0, 'historical': 0}
    for row in results:
        baseline[row['status']] = int(row['count'])

    print_success(f"Baseline current records: {baseline['current']:,}")
    print_success(f"Baseline historical records: {baseline['historical']:,}")

    # Check for existing canary data
    canary_results = run_athena_query(f"""
        SELECT COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
    """)

    canary_count = int(canary_results[0]['count'])
    if canary_count > 0:
        print_warning(f"Found {canary_count} existing canary records from previous tests")
    else:
        print_success("No existing canary records (clean state)")

    return baseline

# ============================================================================
# Synthetic Data Generation
# ============================================================================

def generate_add_scenario() -> List[Dict]:
    """
    Scenario 1: ADDs (10 new records)
    Generate 10 brand-new canary NDCs
    """
    print_header("Step 2: Generate Synthetic Data - ADD Scenario")
    print("Creating 10 new canary NDCs (99999001000 through 99999001009)")

    data = []
    for i in range(10):
        ndc = f"{CANARY_PREFIX}{1000 + i:06d}"  # 99999001000, 99999001001, ...
        data.append({
            'ndc_11': ndc,
            'proprietary_name': f'TEST_PRODUCT_{i+1}',
            'dosage_form': 'TABLET',
            'route': 'ORAL',
            'marketing_category': 'NDA',
            'application_number': f'NDA{21000 + i:06d}',
            'labeler_name': 'TEST PHARMACEUTICALS INC',
            'substance_name': 'TEST ACTIVE INGREDIENT',
            'strength': '100MG',
            'pharm_class': 'TEST CLASS [EPC]',
        })

    print_success(f"Generated {len(data)} new records")
    return data

def generate_update_scenario() -> List[Dict]:
    """
    Scenario 2: UPDATES (5 modified + 5 unchanged)
    Assumes Scenario 1 already ran.
    Modify 5 of the original 10 NDCs (change proprietary_name and strength)
    Keep 5 unchanged
    """
    print_header("Step 2: Generate Synthetic Data - UPDATE Scenario")
    print("Modifying 5 canary NDCs + keeping 5 unchanged")

    data = []

    # Modified 5 (indices 0-4)
    for i in range(5):
        ndc = f"{CANARY_PREFIX}{1000 + i:06d}"
        data.append({
            'ndc_11': ndc,
            'proprietary_name': f'TEST_PRODUCT_{i+1}_MODIFIED',  # Changed
            'dosage_form': 'TABLET',
            'route': 'ORAL',
            'marketing_category': 'NDA',
            'application_number': f'NDA{21000 + i:06d}',
            'labeler_name': 'TEST PHARMACEUTICALS INC',
            'substance_name': 'TEST ACTIVE INGREDIENT',
            'strength': '200MG',  # Changed
            'pharm_class': 'TEST CLASS [EPC]',
        })

    # Unchanged 5 (indices 5-9)
    for i in range(5, 10):
        ndc = f"{CANARY_PREFIX}{1000 + i:06d}"
        data.append({
            'ndc_11': ndc,
            'proprietary_name': f'TEST_PRODUCT_{i+1}',  # Same as original
            'dosage_form': 'TABLET',
            'route': 'ORAL',
            'marketing_category': 'NDA',
            'application_number': f'NDA{21000 + i:06d}',
            'labeler_name': 'TEST PHARMACEUTICALS INC',
            'substance_name': 'TEST ACTIVE INGREDIENT',
            'strength': '100MG',  # Same as original
            'pharm_class': 'TEST CLASS [EPC]',
        })

    print_success(f"Generated {len(data)} records (5 modified + 5 unchanged)")
    return data

def generate_term_scenario() -> List[Dict]:
    """
    Scenario 3: TERMS (3 deleted/expired)
    Assumes Scenarios 1 and 2 already ran.
    Remove 3 NDCs from the dataset (simulating expiration/deletion)
    Keep remaining 7
    """
    print_header("Step 2: Generate Synthetic Data - TERM Scenario")
    print("Removing 3 canary NDCs (simulating expiration)")

    data = []

    # Keep 7 records (indices 3-9)
    # Remove first 3 (indices 0-2) by not including them
    for i in range(3, 10):
        # Use the MODIFIED version for indices 3-4, original for 5-9
        if i < 5:
            proprietary_name = f'TEST_PRODUCT_{i+1}_MODIFIED'
            strength = '200MG'
        else:
            proprietary_name = f'TEST_PRODUCT_{i+1}'
            strength = '100MG'

        ndc = f"{CANARY_PREFIX}{1000 + i:06d}"
        data.append({
            'ndc_11': ndc,
            'proprietary_name': proprietary_name,
            'dosage_form': 'TABLET',
            'route': 'ORAL',
            'marketing_category': 'NDA',
            'application_number': f'NDA{21000 + i:06d}',
            'labeler_name': 'TEST PHARMACEUTICALS INC',
            'substance_name': 'TEST ACTIVE INGREDIENT',
            'strength': strength,
            'pharm_class': 'TEST CLASS [EPC]',
        })

    print_success(f"Generated {len(data)} records (3 removed, 7 remaining)")
    return data

# ============================================================================
# Silver Data Upload
# ============================================================================

def write_to_silver(data: List[Dict], run_date: str):
    """
    Write synthetic data to Silver S3 partition as CSV.
    Uses pandas for clean CSV generation.
    """
    print_header("Step 3: Write to Silver S3")

    try:
        import pandas as pd
    except ImportError:
        print_error("pandas not available in Glue Python Shell environment")
        sys.exit(1)

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Write to local temp file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_file = f.name
        df.to_csv(f, index=False)

    # Upload to S3 Silver partition
    s3_key = f"silver/{TABLE}/run_date={run_date}/synthetic_test_data.csv"
    s3.upload_file(temp_file, BUCKET, s3_key)

    # Clean up
    os.unlink(temp_file)

    s3_uri = f"s3://{BUCKET}/{s3_key}"
    print_success(f"Uploaded {len(data)} records to {s3_uri}")
    print(f"  Columns: {', '.join(df.columns)}")

# ============================================================================
# Validation
# ============================================================================

def validate_add_scenario(baseline: Dict):
    """Validate expectations for ADD scenario"""
    print_header("Step 6: Validate ADD Scenario")

    # Expected: 10 new current records, 0 new historical
    results = run_athena_query(f"""
        SELECT status, COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        GROUP BY status
    """)

    current_count = 0
    historical_count = 0
    for row in results:
        if row['status'] == 'current':
            current_count = int(row['count'])
        elif row['status'] == 'historical':
            historical_count = int(row['count'])

    expected_current = baseline['current'] + 10
    expected_historical = baseline['historical']

    print(f"Expected current: {expected_current:,}, Actual: {current_count:,}")
    print(f"Expected historical: {expected_historical:,}, Actual: {historical_count:,}")

    if current_count != expected_current:
        print_error(f"Current count mismatch! Expected +10")
        return False

    if historical_count != expected_historical:
        print_error(f"Historical count changed unexpectedly!")
        return False

    print_success("Overall counts match expectations")

    # Validate canary records
    canary_results = run_athena_query(f"""
        SELECT status, COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
        GROUP BY status
    """)

    canary_current = 0
    canary_historical = 0
    for row in canary_results:
        if row['status'] == 'current':
            canary_current = int(row['count'])
        elif row['status'] == 'historical':
            canary_historical = int(row['count'])

    if canary_current != 10:
        print_error(f"Expected 10 canary current records, got {canary_current}")
        return False

    if canary_historical != 0:
        print_error(f"Expected 0 canary historical records, got {canary_historical}")
        return False

    print_success("Canary record counts correct (10 current, 0 historical)")

    # Validate active_to for current records
    bad_active_to = run_athena_query(f"""
        SELECT COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
          AND status = 'current'
          AND active_to <> DATE '9999-12-31'
    """)

    if int(bad_active_to[0]['count']) > 0:
        print_error("Some current records have wrong active_to value")
        return False

    print_success("All current records have active_to = 9999-12-31")

    # Check for duplicates
    duplicates = run_athena_query(f"""
        SELECT ndc_11, COUNT(*) as dup_count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
          AND status = 'current'
        GROUP BY ndc_11
        HAVING COUNT(*) > 1
    """)

    if len(duplicates) > 0:
        print_error(f"Found {len(duplicates)} duplicate current records!")
        return False

    print_success("No duplicate current records")

    return True

def validate_update_scenario(baseline: Dict):
    """Validate expectations for UPDATE scenario"""
    print_header("Step 6: Validate UPDATE Scenario")

    # Expected:
    # - current: baseline + 10 (still 10 current, but 5 are new versions)
    # - historical: baseline + 5 (5 old versions expired)

    results = run_athena_query(f"""
        SELECT status, COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        GROUP BY status
    """)

    current_count = 0
    historical_count = 0
    for row in results:
        if row['status'] == 'current':
            current_count = int(row['count'])
        elif row['status'] == 'historical':
            historical_count = int(row['count'])

    expected_current = baseline['current'] + 10
    expected_historical = baseline['historical'] + 5  # 5 old versions

    print(f"Expected current: {expected_current:,}, Actual: {current_count:,}")
    print(f"Expected historical: {expected_historical:,}, Actual: {historical_count:,}")

    if current_count != expected_current:
        print_error(f"Current count mismatch!")
        return False

    if historical_count != expected_historical:
        print_error(f"Historical count mismatch! Expected +5 old versions")
        return False

    print_success("Overall counts match expectations")

    # Validate canary records
    canary_results = run_athena_query(f"""
        SELECT status, COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
        GROUP BY status
    """)

    canary_current = 0
    canary_historical = 0
    for row in canary_results:
        if row['status'] == 'current':
            canary_current = int(row['count'])
        elif row['status'] == 'historical':
            canary_historical = int(row['count'])

    if canary_current != 10:
        print_error(f"Expected 10 canary current records, got {canary_current}")
        return False

    if canary_historical != 5:
        print_error(f"Expected 5 canary historical records, got {canary_historical}")
        return False

    print_success("Canary record counts correct (10 current, 5 historical)")

    # Validate all current have active_to = 9999-12-31
    bad_active_to = run_athena_query(f"""
        SELECT COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
          AND status = 'current'
          AND active_to <> DATE '9999-12-31'
    """)

    if int(bad_active_to[0]['count']) > 0:
        print_error("Some current records have wrong active_to value")
        return False

    print_success("All current records have active_to = 9999-12-31")

    # Validate historical have active_to = run_date
    bad_historical = run_athena_query(f"""
        SELECT COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
          AND status = 'historical'
          AND active_to <> DATE '{RUN_DATE}'
    """)

    if int(bad_historical[0]['count']) > 0:
        print_error(f"Some historical records have wrong active_to (expected {RUN_DATE})")
        return False

    print_success(f"All historical records have active_to = {RUN_DATE}")

    return True

def validate_term_scenario(baseline: Dict):
    """Validate expectations for TERM scenario"""
    print_header("Step 6: Validate TERM Scenario")

    # Expected:
    # - current: baseline + 7 (10 - 3 terminated)
    # - historical: baseline + 8 (5 from updates + 3 terminated)

    results = run_athena_query(f"""
        SELECT status, COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        GROUP BY status
    """)

    current_count = 0
    historical_count = 0
    for row in results:
        if row['status'] == 'current':
            current_count = int(row['count'])
        elif row['status'] == 'historical':
            historical_count = int(row['count'])

    expected_current = baseline['current'] + 7  # 10 - 3
    expected_historical = baseline['historical'] + 8  # 5 + 3

    print(f"Expected current: {expected_current:,}, Actual: {current_count:,}")
    print(f"Expected historical: {expected_historical:,}, Actual: {historical_count:,}")

    if current_count != expected_current:
        print_error(f"Current count mismatch! Expected +7 net")
        return False

    if historical_count != expected_historical:
        print_error(f"Historical count mismatch! Expected +8 (5 updates + 3 terms)")
        return False

    print_success("Overall counts match expectations")

    # Validate canary records
    canary_results = run_athena_query(f"""
        SELECT status, COUNT(*) as count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
        GROUP BY status
    """)

    canary_current = 0
    canary_historical = 0
    for row in canary_results:
        if row['status'] == 'current':
            canary_current = int(row['count'])
        elif row['status'] == 'historical':
            canary_historical = int(row['count'])

    if canary_current != 7:
        print_error(f"Expected 7 canary current records, got {canary_current}")
        return False

    if canary_historical != 8:
        print_error(f"Expected 8 canary historical records, got {canary_historical}")
        return False

    print_success("Canary record counts correct (7 current, 8 historical)")

    # Validate no duplicates
    duplicates = run_athena_query(f"""
        SELECT ndc_11, COUNT(*) as dup_count
        FROM {GOLD_DATABASE}.{TABLE}
        WHERE ndc_11 LIKE '{CANARY_PREFIX}%'
          AND status = 'current'
        GROUP BY ndc_11
        HAVING COUNT(*) > 1
    """)

    if len(duplicates) > 0:
        print_error(f"Found {len(duplicates)} duplicate current records!")
        return False

    print_success("No duplicate current records")

    return True

# ============================================================================
# Main Orchestration
# ============================================================================

def main():
    """Main test orchestration"""
    print_header("🧪 Simplified Delta Lake Confidence Test")
    print(f"Scenario: {SCENARIO.upper()}")
    print(f"Run Date: {RUN_DATE}")
    print(f"Database: {GOLD_DATABASE}")
    print(f"Table: {TABLE}")

    # Step 1: Query baseline
    baseline = query_baseline_counts()

    # Step 2: Generate data based on scenario
    if SCENARIO == 'add':
        data = generate_add_scenario()
    elif SCENARIO == 'update':
        data = generate_update_scenario()
    elif SCENARIO == 'term':
        data = generate_term_scenario()
    else:
        print_error(f"Unknown scenario: {SCENARIO}")
        print("Valid scenarios: add, update, term")
        sys.exit(1)

    # Step 3: Write to Silver
    write_to_silver(data, RUN_DATE)

    # Step 4: Run Silver Crawler
    wait_for_crawler(SILVER_CRAWLER)

    # Step 5: Run Gold Job
    print_header("Step 4: Run Gold Job")
    gold_run = glue.start_job_run(
        JobName=GOLD_JOB,
        Arguments={'--run_date': RUN_DATE}
    )
    gold_result = wait_for_glue_job(GOLD_JOB, gold_run['JobRunId'])

    if gold_result['JobRunState'] != 'SUCCEEDED':
        print_error("Gold job failed!")
        sys.exit(1)

    # Step 6: Validate
    if SCENARIO == 'add':
        success = validate_add_scenario(baseline)
    elif SCENARIO == 'update':
        success = validate_update_scenario(baseline)
    elif SCENARIO == 'term':
        success = validate_term_scenario(baseline)

    # Final result
    print_header("📊 Test Result")
    if success:
        print(f"{Colors.GREEN}{Colors.BOLD}✅ TEST PASSED{Colors.END}")
        print(f"{Colors.GREEN}Scenario '{SCENARIO}' validated successfully!{Colors.END}\n")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}❌ TEST FAILED{Colors.END}")
        print(f"{Colors.RED}Scenario '{SCENARIO}' validation failed. Review errors above.{Colors.END}\n")
        sys.exit(1)

if __name__ == '__main__':
    main()
