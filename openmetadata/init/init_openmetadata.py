#!/usr/bin/env python3
"""
OpenMetadata Initialization Script

This script initializes OpenMetadata with:
1. ClickHouse database service connection
2. Table and column descriptions for the gold layer
3. Glossary terms for business context

Run this script after OpenMetadata server is healthy to pre-populate metadata.
"""

import json
import time
import requests
import os
import base64
from pathlib import Path

# Configuration
OM_SERVER = os.getenv("OM_SERVER", "http://localhost:8585")
OM_API = f"{OM_SERVER}/api/v1"
ADMIN_USER = os.getenv("OM_ADMIN_USER", "admin@open-metadata.org")
ADMIN_PASSWORD = os.getenv("OM_ADMIN_PASSWORD", "admin")

# Load metadata definitions
SCRIPT_DIR = Path(__file__).parent
METADATA_FILE = SCRIPT_DIR / "metadata_definitions.json"


def get_auth_token():
    """Get JWT token for API authentication"""
    login_url = f"{OM_API}/users/login"
    # OpenMetadata requires Base64-encoded password
    encoded_password = base64.b64encode(ADMIN_PASSWORD.encode()).decode()
    payload = {"email": ADMIN_USER, "password": encoded_password}
    
    try:
        response = requests.post(login_url, json=payload)
        response.raise_for_status()
        return response.json().get("accessToken")
    except requests.exceptions.RequestException as e:
        print(f"Failed to authenticate: {e}")
        print(f"Response: {response.text if 'response' in dir() else 'N/A'}")
        return None


def wait_for_server(max_retries=30, delay=10):
    """Wait for OpenMetadata server to be ready"""
    print(f"Waiting for OpenMetadata server at {OM_SERVER}...")
    
    for i in range(max_retries):
        try:
            response = requests.get(f"{OM_API}/system/version")
            if response.status_code == 200:
                version = response.json().get("version", "unknown")
                print(f"OpenMetadata server is ready (version: {version})")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"  Retry {i+1}/{max_retries}...")
        time.sleep(delay)
    
    print("OpenMetadata server did not become ready in time")
    return False


def create_database_service(headers, service_config):
    """Create or update ClickHouse database service"""
    service_name = service_config["name"]
    
    # Check if service exists
    check_url = f"{OM_API}/services/databaseServices/name/{service_name}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"Database service '{service_name}' already exists")
        return response.json()
    
    # Create new service
    create_url = f"{OM_API}/services/databaseServices"
    payload = {
        "name": service_name,
        "serviceType": service_config["serviceType"],
        "description": service_config["description"],
        "connection": service_config["connection"]
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"Created database service: {service_name}")
        return response.json()
    else:
        print(f"Failed to create service: {response.status_code} - {response.text}")
        return None


def create_database(headers, service_fqn, db_name, description):
    """Create a database under the service"""
    db_fqn = f"{service_fqn}.{db_name}"
    
    # Check if exists
    check_url = f"{OM_API}/databases/name/{db_fqn}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"Database '{db_name}' already exists")
        return response.json()
    
    # Create database - OpenMetadata 1.4 expects service as FQN string
    create_url = f"{OM_API}/databases"
    payload = {
        "name": db_name,
        "description": description,
        "service": service_fqn  # Just the FQN string, not an object
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"Created database: {db_name}")
        return response.json()
    else:
        print(f"Failed to create database: {response.status_code} - {response.text}")
        return None


def create_schema(headers, db_fqn, schema_name="default"):
    """Create a schema under the database"""
    schema_fqn = f"{db_fqn}.{schema_name}"
    
    # Check if exists
    check_url = f"{OM_API}/databaseSchemas/name/{schema_fqn}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"Schema '{schema_name}' already exists")
        return response.json()
    
    # Create schema - OpenMetadata 1.4 expects database as FQN string
    create_url = f"{OM_API}/databaseSchemas"
    payload = {
        "name": schema_name,
        "database": db_fqn  # Just the FQN string
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"Created schema: {schema_name}")
        return response.json()
    else:
        print(f"Failed to create schema: {response.status_code} - {response.text}")
        return None


def create_table(headers, schema_fqn, table_name, table_config):
    """Create a table with columns under the schema"""
    table_fqn = f"{schema_fqn}.{table_name}"
    
    # Check if exists
    check_url = f"{OM_API}/tables/name/{table_fqn}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"Table '{table_name}' already exists, updating descriptions...")
        existing_table = response.json()
        return update_table_descriptions(headers, existing_table, table_config)
    
    # Build columns list
    columns = []
    for col_name, col_desc in table_config.get("columns", {}).items():
        columns.append({
            "name": col_name,
            "description": col_desc,
            "dataType": "STRING"  # Default, will be updated by ingestion
        })
    
    # Create table - OpenMetadata 1.4 expects databaseSchema as FQN string
    create_url = f"{OM_API}/tables"
    payload = {
        "name": table_name,
        "description": table_config.get("description", ""),
        "databaseSchema": schema_fqn,  # Just the FQN string
        "columns": columns,
        "tableType": "Regular"
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"Created table: {table_name}")
        return response.json()
    else:
        print(f"Failed to create table: {response.status_code} - {response.text}")
        return None


def update_table_descriptions(headers, existing_table, table_config):
    """Update table and column descriptions for existing table"""
    table_id = existing_table["id"]
    
    # Update table description
    patch_url = f"{OM_API}/tables/{table_id}"
    
    # Build patch operations
    operations = []
    
    # Update table description
    if table_config.get("description"):
        operations.append({
            "op": "add",
            "path": "/description",
            "value": table_config["description"]
        })
    
    # Update column descriptions
    for i, col in enumerate(existing_table.get("columns", [])):
        col_name = col["name"]
        if col_name in table_config.get("columns", {}):
            operations.append({
                "op": "add",
                "path": f"/columns/{i}/description",
                "value": table_config["columns"][col_name]
            })
    
    if operations:
        headers_patch = headers.copy()
        headers_patch["Content-Type"] = "application/json-patch+json"
        response = requests.patch(patch_url, headers=headers_patch, json=operations)
        
        if response.status_code in [200, 201]:
            print(f"Updated descriptions for table: {existing_table['name']}")
            return response.json()
        else:
            print(f"Failed to update table: {response.status_code} - {response.text}")
    
    return existing_table


def create_glossary(headers, glossary_config):
    """Create glossary with terms"""
    glossary_name = glossary_config["name"]
    
    # Check if exists
    check_url = f"{OM_API}/glossaries/name/{glossary_name}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"Glossary '{glossary_name}' already exists")
        glossary = response.json()
    else:
        # Create glossary
        create_url = f"{OM_API}/glossaries"
        payload = {
            "name": glossary_name,
            "description": glossary_config.get("description", "")
        }
        
        response = requests.post(create_url, headers=headers, json=payload)
        
        if response.status_code in [200, 201]:
            print(f"Created glossary: {glossary_name}")
            glossary = response.json()
        else:
            print(f"Failed to create glossary: {response.status_code} - {response.text}")
            return None
    
    # Create terms
    glossary_id = glossary["id"]
    for term in glossary_config.get("terms", []):
        create_glossary_term(headers, glossary_id, glossary_name, term)
    
    return glossary


def create_glossary_term(headers, glossary_id, glossary_name, term_config):
    """Create a glossary term"""
    term_name = term_config["name"]
    term_fqn = f"{glossary_name}.{term_name}"
    
    # Check if exists - URL encode spaces
    encoded_fqn = term_fqn.replace(" ", "%20")
    check_url = f"{OM_API}/glossaryTerms/name/{encoded_fqn}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"  Term '{term_name}' already exists")
        return response.json()
    
    # Create term - OpenMetadata 1.4 expects glossary as FQN string
    create_url = f"{OM_API}/glossaryTerms"
    payload = {
        "name": term_name,
        "description": term_config.get("description", ""),
        "glossary": glossary_name  # Just the glossary name/FQN
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"  Created term: {term_name}")
        return response.json()
    else:
        print(f"  Failed to create term: {response.status_code} - {response.text}")
        return None


def get_test_definition_id(headers, test_name):
    """Get the ID of a test definition by name"""
    url = f"{OM_API}/dataQuality/testDefinitions/name/{test_name}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("id")
    return None


def create_test_suite(headers, table_fqn):
    """Create a test suite for a table"""
    # URL encode the FQN
    encoded_fqn = table_fqn.replace(".", "%2E")
    suite_name = f"{table_fqn}.testSuite"
    
    # Check if exists
    check_url = f"{OM_API}/dataQuality/testSuites/name/{encoded_fqn}%2EtestSuite"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    
    # Create executable test suite linked to the table
    create_url = f"{OM_API}/dataQuality/testSuites/executable"
    payload = {
        "name": suite_name,
        "description": f"Data quality test suite for {table_fqn}",
        "executableEntityReference": table_fqn
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"  Created test suite for: {table_fqn}")
        return response.json()
    else:
        print(f"  Failed to create test suite: {response.status_code} - {response.text}")
        return None


def create_test_case(headers, test_suite_fqn, table_fqn, test_config):
    """Create a data quality test case"""
    test_name = test_config["name"]
    
    # Check if exists
    encoded_name = test_name.replace(" ", "%20")
    check_url = f"{OM_API}/dataQuality/testCases/name/{encoded_name}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"    Test '{test_name}' already exists")
        return response.json()
    
    # Get test definition ID
    test_def_id = get_test_definition_id(headers, test_config["testDefinitionName"])
    if not test_def_id:
        print(f"    Could not find test definition: {test_config['testDefinitionName']}")
        return None
    
    # Create test case
    create_url = f"{OM_API}/dataQuality/testCases"
    payload = {
        "name": test_name,
        "description": test_config.get("description", ""),
        "testDefinition": test_config["testDefinitionName"],
        "entityLink": test_config["entityLink"],
        "testSuite": test_suite_fqn,
        "parameterValues": test_config.get("parameterValues", [])
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"    Created test: {test_name}")
        return response.json()
    else:
        print(f"    Failed to create test: {response.status_code} - {response.text}")
        return None


def create_data_quality_tests(headers, service_name):
    """Create data quality tests for fact and dimension tables"""
    
    # Define test cases
    test_cases = {
    # Fact table: FactVoting - NOT NULL on foreign keys
        f"{service_name}.gold.default.FactVoting": [
            {
                "name": "voting_weatherid_not_null",
                "description": "Ensure WeatherId foreign key in FactVoting fact table is not null",
                "testDefinitionName": "columnValuesToBeNotNull",
                "entityLink": f"<#E::table::{service_name}.gold.default.FactVoting::columns::WeatherId>",
                "parameterValues": []
            },
            {
                "name": "voting_type_fk_not_null",
                "description": "Ensure VotingType foreign key in FactVoting fact table is not null",
                "testDefinitionName": "columnValuesToBeNotNull",
                "entityLink": f"<#E::table::{service_name}.gold.default.FactVoting::columns::VotingType>",
                "parameterValues": []
            }
        ],
        # Dimension table: DimDate - UNIQUE on surrogate key
        f"{service_name}.gold.default.DimDate": [
            {
                "name": "dimdate_date_unique",
                "description": "Ensure Date surrogate key in DimDate dimension table is unique",
                "testDefinitionName": "columnValuesToBeUnique",
                "entityLink": f"<#E::table::{service_name}.gold.default.DimDate::columns::Date>",
                "parameterValues": []
            }
        ],

        # Extra test: Check voting attendance values are reasonable (between 0 and 101 for Estonian Parliament)
        f"{service_name}.gold.default.FactVoting_extra": [
            {
                "name": "voting_present_count_valid",
                "description": "Ensure Present count is between 0 and 101 (total Riigikogu members)",
                "testDefinitionName": "columnValuesToBeBetween",
                "entityLink": f"<#E::table::{service_name}.gold.default.FactVoting::columns::Present>",
                "parameterValues": [
                    {"name": "minValue", "value": "0"},
                    {"name": "maxValue", "value": "101"}
                ]
            }
        ]
    }
    
    for table_fqn, tests in test_cases.items():
        # Handle the extra test case which uses Voting table
        actual_table_fqn = table_fqn.replace("_extra", "")
        
        print(f"  Setting up tests for: {actual_table_fqn}")
        
        # Create test suite for the table
        test_suite = create_test_suite(headers, actual_table_fqn)
        if not test_suite:
            continue
        
        test_suite_fqn = test_suite.get("fullyQualifiedName", f"{actual_table_fqn}.testSuite")
        
        # Create each test case
        for test_config in tests:
            create_test_case(headers, test_suite_fqn, actual_table_fqn, test_config)


def create_dashboard_service(headers, service_config):
    """Create a dashboard service (e.g., Superset)"""
    service_name = service_config["name"]
    
    # Check if service exists
    check_url = f"{OM_API}/services/dashboardServices/name/{service_name}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"Dashboard service '{service_name}' already exists")
        return response.json()
    
    # Create new service
    create_url = f"{OM_API}/services/dashboardServices"
    payload = {
        "name": service_name,
        "serviceType": service_config["serviceType"],
        "description": service_config["description"],
        "connection": service_config["connection"]
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"Created dashboard service: {service_name}")
        return response.json()
    else:
        print(f"Failed to create dashboard service: {response.status_code} - {response.text}")
        return None


def create_dashboard(headers, service_fqn, dashboard_config):
    """Create a dashboard entry"""
    dashboard_name = dashboard_config["name"]
    dashboard_fqn = f"{service_fqn}.{dashboard_name}"
    
    # Check if exists
    encoded_fqn = dashboard_fqn.replace(" ", "%20")
    check_url = f"{OM_API}/dashboards/name/{encoded_fqn}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"  Dashboard '{dashboard_name}' already exists")
        return response.json()
    
    # Create dashboard
    create_url = f"{OM_API}/dashboards"
    payload = {
        "name": dashboard_name,
        "displayName": dashboard_config.get("displayName", dashboard_name),
        "description": dashboard_config.get("description", ""),
        "service": service_fqn,
        "sourceUrl": dashboard_config.get("sourceUrl", ""),
        "charts": []  # Will add charts separately if needed
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"  Created dashboard: {dashboard_name}")
        return response.json()
    else:
        print(f"  Failed to create dashboard: {response.status_code} - {response.text}")
        return None


def create_chart(headers, service_fqn, dashboard_fqn, chart_config):
    """Create a chart entry"""
    chart_name = chart_config["name"]
    chart_fqn = f"{service_fqn}.{chart_name}"
    
    # Check if exists
    encoded_fqn = chart_fqn.replace(" ", "%20")
    check_url = f"{OM_API}/charts/name/{encoded_fqn}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"    Chart '{chart_name}' already exists")
        return response.json()
    
    # Create chart
    create_url = f"{OM_API}/charts"
    payload = {
        "name": chart_name,
        "displayName": chart_config.get("displayName", chart_name),
        "description": chart_config.get("description", ""),
        "service": service_fqn,
        "chartType": chart_config.get("chartType", "Other"),
        "sourceUrl": chart_config.get("sourceUrl", "")
    }
    
    response = requests.post(create_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        print(f"    Created chart: {chart_name}")
        return response.json()
    else:
        print(f"    Failed to create chart: {response.status_code} - {response.text}")
        return None


def main():
    """Main initialization function"""
    print("=" * 60)
    print("OpenMetadata Initialization Script")
    print("=" * 60)
    
    # Wait for server
    if not wait_for_server():
        return 1
    
    # Get auth token
    token = get_auth_token()
    if not token:
        print("Failed to get authentication token")
        return 1
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Load metadata definitions
    if not METADATA_FILE.exists():
        print(f"Metadata file not found: {METADATA_FILE}")
        return 1
    
    with open(METADATA_FILE) as f:
        metadata = json.load(f)
    
    print("\n--- Creating Database Service ---")
    service_config = metadata["database_service"]
    service = create_database_service(headers, service_config)
    if not service:
        print("Failed to create database service")
        return 1
    
    service_name = service_config["name"]
    
    print("\n--- Creating Databases ---")
    for db_config in metadata.get("databases", []):
        db = create_database(headers, service_name, db_config["name"], db_config["description"])
        if db:
            # Create default schema
            db_fqn = f"{service_name}.{db_config['name']}"
            create_schema(headers, db_fqn)
    
    print("\n--- Creating Tables with Descriptions ---")
    for db_name, tables in metadata.get("tables", {}).items():
        db_fqn = f"{service_name}.{db_name}"
        schema_fqn = f"{db_fqn}.default"
        
        for table_name, table_config in tables.items():
            create_table(headers, schema_fqn, table_name, table_config)
    
    print("\n--- Creating Glossary ---")
    if "glossary" in metadata:
        create_glossary(headers, metadata["glossary"])
    
    print("\n--- Creating Data Quality Tests ---")
    create_data_quality_tests(headers, service_name)
    
    print("\n--- Creating Dashboard Service (Superset) ---")
    if "dashboard_service" in metadata:
        dashboard_service = create_dashboard_service(headers, metadata["dashboard_service"])
        if dashboard_service:
            dashboard_service_name = metadata["dashboard_service"]["name"]
            
            # Create dashboards
            for dashboard_config in metadata.get("dashboards", []):
                dashboard = create_dashboard(headers, dashboard_service_name, dashboard_config)
                if dashboard:
                    dashboard_fqn = f"{dashboard_service_name}.{dashboard_config['name']}"
                    # Create charts for this dashboard
                    for chart_config in dashboard_config.get("charts", []):
                        create_chart(headers, dashboard_service_name, dashboard_fqn, chart_config)
    
    print("\n" + "=" * 60)
    print("OpenMetadata initialization complete!")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    exit(main())
