#!/usr/bin/env python3
"""
Superset Initialization Script

This script initializes Superset with:
1. ClickHouse database connection
2. Datasets for gold layer tables
3. (Optional) Pre-configured dashboards

Run this script after Superset is healthy to pre-populate connections.
"""

import json
import time
import requests
import os
from pathlib import Path

# Configuration
SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

# Load config
SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "superset_config.json"


class SupersetClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.csrf_token = None
        self.access_token = None
        self.username = username
        self.password = password
    
    def wait_for_server(self, max_retries=30, delay=10):
        """Wait for Superset to be ready"""
        print(f"Waiting for Superset at {self.base_url}...")
        
        for i in range(max_retries):
            try:
                response = self.session.get(f"{self.base_url}/health")
                if response.status_code == 200:
                    print("Superset is ready!")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            print(f"  Retry {i+1}/{max_retries}...")
            time.sleep(delay)
        
        return False
    
    def login(self):
        """Login to Superset and get tokens"""
        # Get CSRF token from login page
        login_page = self.session.get(f"{self.base_url}/login/")
        
        # Try API login
        login_url = f"{self.base_url}/api/v1/security/login"
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": "db"
        }
        
        response = self.session.post(login_url, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            print("Successfully authenticated with Superset API")
            return True
        else:
            print(f"Login failed: {response.status_code} - {response.text}")
            return False
    
    def get_headers(self):
        """Get headers for API requests"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers
    
    def get_csrf_token(self):
        """Get CSRF token for write operations"""
        response = self.session.get(
            f"{self.base_url}/api/v1/security/csrf_token/",
            headers=self.get_headers()
        )
        if response.status_code == 200:
            self.csrf_token = response.json().get("result")
            return self.csrf_token
        return None
    
    def create_database(self, db_config):
        """Create a database connection"""
        db_name = db_config["database_name"]
        
        # Check if exists
        response = self.session.get(
            f"{self.base_url}/api/v1/database/",
            headers=self.get_headers(),
            params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": db_name}]})}
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get("count", 0) > 0:
                print(f"Database '{db_name}' already exists")
                return result["result"][0]
        
        # Get CSRF token
        csrf = self.get_csrf_token()
        
        # Create database
        headers = self.get_headers()
        if csrf:
            headers["X-CSRFToken"] = csrf
        
        payload = {
            "database_name": db_name,
            "sqlalchemy_uri": db_config["sqlalchemy_uri"],
            "expose_in_sqllab": db_config.get("expose_in_sqllab", True),
            "allow_ctas": db_config.get("allow_ctas", False),
            "allow_cvas": db_config.get("allow_cvas", False),
            "allow_dml": db_config.get("allow_dml", False),
            "extra": json.dumps(db_config.get("extra", {}))
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/database/",
            headers=headers,
            json=payload
        )
        
        if response.status_code in [200, 201]:
            print(f"Created database connection: {db_name}")
            return response.json()
        else:
            print(f"Failed to create database: {response.status_code} - {response.text}")
            return None
    
    def create_dataset(self, dataset_config, database_id):
        """Create a dataset (table)"""
        table_name = dataset_config["table_name"]
        schema = dataset_config.get("schema", "")
        
        # Check if exists
        response = self.session.get(
            f"{self.base_url}/api/v1/dataset/",
            headers=self.get_headers(),
            params={"q": json.dumps({"filters": [
                {"col": "table_name", "opr": "eq", "value": table_name},
                {"col": "database", "opr": "rel_o_m", "value": database_id}
            ]})}
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get("count", 0) > 0:
                print(f"  Dataset '{table_name}' already exists")
                return result["result"][0]
        
        # Get CSRF token
        csrf = self.get_csrf_token()
        
        headers = self.get_headers()
        if csrf:
            headers["X-CSRFToken"] = csrf
        
        payload = {
            "database": database_id,
            "table_name": table_name,
            "schema": schema
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/dataset/",
            headers=headers,
            json=payload
        )
        
        if response.status_code in [200, 201]:
            print(f"  Created dataset: {table_name}")
            return response.json()
        else:
            print(f"  Failed to create dataset: {response.status_code} - {response.text}")
            return None


def main():
    print("=" * 60)
    print("Superset Initialization Script")
    print("=" * 60)
    
    client = SupersetClient(SUPERSET_URL, ADMIN_USER, ADMIN_PASSWORD)
    
    # Wait for server
    if not client.wait_for_server():
        print("Superset server did not become ready")
        return 1
    
    # Login
    if not client.login():
        print("Failed to login to Superset")
        return 1
    
    # Load config
    if not CONFIG_FILE.exists():
        print(f"Config file not found: {CONFIG_FILE}")
        return 1
    
    with open(CONFIG_FILE) as f:
        config = json.load(f)
    
    print("\n--- Creating Database Connections ---")
    database_ids = {}
    for db_config in config.get("databases", []):
        result = client.create_database(db_config)
        if result:
            db_id = result.get("id") or result.get("result", {}).get("id")
            if db_id:
                database_ids[db_config["database_name"]] = db_id
    
    print("\n--- Creating Datasets ---")
    for dataset_config in config.get("datasets", []):
        db_name = dataset_config.get("database_name")
        db_id = database_ids.get(db_name)
        if db_id:
            client.create_dataset(dataset_config, db_id)
        else:
            print(f"  Skipping {dataset_config['table_name']} - database not found")
    
    print("\n" + "=" * 60)
    print("Superset initialization complete!")
    print("=" * 60)
    print("\nNote: Dashboards need to be created manually in Superset UI")
    print("      and can be exported for version control.")
    
    return 0


if __name__ == "__main__":
    exit(main())
