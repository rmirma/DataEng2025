#!/usr/bin/env python3
"""
Run OpenMetadata Data Quality Tests locally using the ingestion framework.
This script connects to ClickHouse and executes the tests defined in OpenMetadata.
"""

import base64
import requests
import json
from datetime import datetime

# OpenMetadata config
OM_HOST = "http://localhost:8585"
OM_USER = "admin@open-metadata.org"
OM_PASSWORD = "admin"

# ClickHouse connection details
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "clickhouse"
CLICKHOUSE_PASSWORD = "clickhouse"
CLICKHOUSE_DB = "gold"


def get_auth_token():
    """Get authentication token from OpenMetadata."""
    password_encoded = base64.b64encode(OM_PASSWORD.encode()).decode()
    response = requests.post(
        f"{OM_HOST}/api/v1/users/login",
        json={"email": OM_USER, "password": password_encoded}
    )
    response.raise_for_status()
    return response.json()["accessToken"]


def get_test_cases(token):
    """Get all test cases from OpenMetadata."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"{OM_HOST}/api/v1/dataQuality/testCases?limit=50&fields=testSuite,testDefinition",
        headers=headers
    )
    response.raise_for_status()
    return response.json().get("data", [])


def run_clickhouse_query(query):
    """Execute a query against ClickHouse."""
    response = requests.get(
        f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}",
        params={"query": query},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    )
    response.raise_for_status()
    return response.text.strip()


def execute_not_null_test(table, column):
    """Test that column values are not null."""
    query = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
    null_count = int(run_clickhouse_query(query))
    
    total_query = f"SELECT COUNT(*) FROM {table}"
    total_count = int(run_clickhouse_query(total_query))
    
    passed = null_count == 0
    return {
        "passed": passed,
        "details": f"Found {null_count} NULL values out of {total_count} rows",
        "null_count": null_count,
        "total_count": total_count
    }


def execute_unique_test(table, column):
    """Test that column values are unique."""
    query = f"SELECT COUNT(*) - COUNT(DISTINCT {column}) as duplicates FROM {table}"
    duplicates = int(run_clickhouse_query(query))
    
    total_query = f"SELECT COUNT(*) FROM {table}"
    total_count = int(run_clickhouse_query(total_query))
    
    passed = duplicates == 0
    return {
        "passed": passed,
        "details": f"Found {duplicates} duplicate values out of {total_count} rows",
        "duplicate_count": duplicates,
        "total_count": total_count
    }


def execute_between_test(table, column, min_val, max_val):
    """Test that column values are between min and max."""
    query = f"SELECT COUNT(*) FROM {table} WHERE {column} < {min_val} OR {column} > {max_val}"
    out_of_range = int(run_clickhouse_query(query))
    
    total_query = f"SELECT COUNT(*) FROM {table}"
    total_count = int(run_clickhouse_query(total_query))
    
    passed = out_of_range == 0
    return {
        "passed": passed,
        "details": f"Found {out_of_range} values outside range [{min_val}, {max_val}] out of {total_count} rows",
        "out_of_range_count": out_of_range,
        "total_count": total_count
    }


def update_test_result(token, test_case_fqn, result, test_result_value):
    """Update test result in OpenMetadata."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    timestamp = int(datetime.now().timestamp() * 1000)
    
    payload = {
        "timestamp": timestamp,
        "testCaseStatus": "Success" if result["passed"] else "Failed",
        "result": result["details"],
        "testResultValue": test_result_value
    }
    
    # Use PUT with the test case FQN path
    response = requests.put(
        f"{OM_HOST}/api/v1/dataQuality/testCases/{test_case_fqn}/testCaseResult",
        headers=headers,
        json=payload
    )
    
    if response.status_code in [200, 201]:
        return True
    else:
        print(f"  Warning: Could not update test result in OpenMetadata: {response.text[:100]}")
        return False


def run_test(token, test_case):
    """Run a single test case."""
    name = test_case["name"]
    fqn = test_case.get("fullyQualifiedName", name)
    entity_fqn = test_case.get("entityFQN", "")
    test_def = test_case.get("testDefinition", {}).get("name", "")
    params = test_case.get("parameterValues", [])
    
    # Parse entity FQN to get table and column
    # Format: service.database.schema.table.column
    parts = entity_fqn.split(".")
    if len(parts) >= 5:
        table = f"{parts[1]}.{parts[3]}"  # database.table
        column = parts[4]
    else:
        return {"name": name, "status": "SKIPPED", "details": f"Could not parse entity FQN: {entity_fqn}"}
    
    print(f"\n  Running: {name}")
    print(f"    Table: {table}, Column: {column}")
    print(f"    Test Type: {test_def}")
    
    try:
        if test_def == "columnValuesToBeNotNull":
            result = execute_not_null_test(table, column)
            test_result_value = [
                {"name": "nullCount", "value": str(result["null_count"])},
                {"name": "totalCount", "value": str(result["total_count"])}
            ]
        elif test_def == "columnValuesToBeUnique":
            result = execute_unique_test(table, column)
            test_result_value = [
                {"name": "duplicateCount", "value": str(result["duplicate_count"])},
                {"name": "totalCount", "value": str(result["total_count"])}
            ]
        elif test_def == "columnValuesToBeBetween":
            # Get min/max from parameters
            min_val = 0
            max_val = 101
            for p in params:
                if p.get("name") == "minValue":
                    min_val = int(p.get("value", 0))
                elif p.get("name") == "maxValue":
                    max_val = int(p.get("value", 101))
            result = execute_between_test(table, column, min_val, max_val)
            test_result_value = [
                {"name": "outOfRangeCount", "value": str(result["out_of_range_count"])},
                {"name": "totalCount", "value": str(result["total_count"])},
                {"name": "minValue", "value": str(min_val)},
                {"name": "maxValue", "value": str(max_val)}
            ]
        else:
            return {"name": name, "status": "SKIPPED", "details": f"Unknown test type: {test_def}"}
        
        status = "PASSED" if result["passed"] else "FAILED"
        print(f"    Result: {status} - {result['details']}")
        
        # Update result in OpenMetadata
        update_test_result(token, fqn, result, test_result_value)
        
        return {"name": name, "status": status, "details": result["details"]}
        
    except Exception as e:
        return {"name": name, "status": "ERROR", "details": str(e)}


def main():
    print("=" * 60)
    print("OpenMetadata Data Quality Test Runner")
    print("=" * 60)
    
    # Get auth token
    print("\nAuthenticating with OpenMetadata...")
    token = get_auth_token()
    print("✓ Authenticated successfully")
    
    # Get test cases
    print("\nFetching test cases...")
    test_cases = get_test_cases(token)
    print(f"✓ Found {len(test_cases)} test cases")
    
    # Run tests
    print("\n" + "-" * 60)
    print("Running Data Quality Tests")
    print("-" * 60)
    
    results = []
    for tc in test_cases:
        result = run_test(token, tc)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(1 for r in results if r["status"] == "PASSED")
    failed = sum(1 for r in results if r["status"] == "FAILED")
    skipped = sum(1 for r in results if r["status"] == "SKIPPED")
    errors = sum(1 for r in results if r["status"] == "ERROR")
    
    print(f"\n  PASSED:  {passed}")
    print(f"  FAILED:  {failed}")
    print(f"  SKIPPED: {skipped}")
    print(f"  ERRORS:  {errors}")
    print(f"  TOTAL:   {len(results)}")
    
    if failed > 0 or errors > 0:
        print("\nFailed/Error tests:")
        for r in results:
            if r["status"] in ["FAILED", "ERROR"]:
                print(f"  - {r['name']}: {r['details']}")
    
    print("\n" + "=" * 60)
    print("Check OpenMetadata UI for updated test results")
    print("=" * 60)
    
    return 0 if (failed == 0 and errors == 0) else 1


if __name__ == "__main__":
    exit(main())
