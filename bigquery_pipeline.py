"""
WEEK 5-6 - BigQuery Pipeline
=============================

This script shows how to load data to Google BigQuery using DLT.

Prerequisites:
1. Google Cloud account with BigQuery enabled
2. Service account JSON key file
3. Environment variables configured
"""

import dlt
from dlt.sources.helpers import requests
from typing import Iterator, Dict, Any
from datetime import datetime
import os
from pathlib import Path


# BigQuery Configuration
BIGQUERY_CREDENTIALS = os.getenv('BIGQUERY_CREDENTIALS_PATH', './phase3_warehouses/credentials/bigquery-key.json')
BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID', 'your-project-id')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'api_ingestion')


def check_bigquery_setup() -> bool:
    """
    Check if BigQuery is properly configured
    """
    
    print("\n" + "=" * 60)
    print("BIGQUERY SETUP CHECK")
    print("=" * 60)
    
    checks_passed = True
    
    # Check credentials file
    print(f"\n1. Checking credentials file...")
    if Path(BIGQUERY_CREDENTIALS).exists():
        print(f"   ✅ Found: {BIGQUERY_CREDENTIALS}")
    else:
        print(f"   ❌ Not found: {BIGQUERY_CREDENTIALS}")
        print(f"   📝 Create a service account in Google Cloud Console")
        print(f"   📝 Download JSON key and save to this path")
        checks_passed = False
    
    # Check project ID
    print(f"\n2. Checking project ID...")
    if BIGQUERY_PROJECT_ID != 'your-project-id':
        print(f"   ✅ Project ID: {BIGQUERY_PROJECT_ID}")
    else:
        print(f"   ❌ Project ID not configured")
        print(f"   📝 Set BIGQUERY_PROJECT_ID in .env file")
        checks_passed = False
    
    # Check dataset name
    print(f"\n3. Dataset configuration...")
    print(f"   Dataset name: {BIGQUERY_DATASET}")
    
    if not checks_passed:
        print("\n" + "=" * 60)
        print("SETUP REQUIRED")
        print("=" * 60)
        print("\nFollow these steps:")
        print("\n1. Go to Google Cloud Console")
        print("   https://console.cloud.google.com/")
        
        print("\n2. Create/Select a project")
        print("   - Click project dropdown")
        print("   - Create new project or select existing")
        
        print("\n3. Enable BigQuery API")
        print("   - Go to 'APIs & Services'")
        print("   - Click 'Enable APIs and Services'")
        print("   - Search 'BigQuery API'")
        print("   - Click 'Enable'")
        
        print("\n4. Create Service Account")
        print("   - Go to 'IAM & Admin' > 'Service Accounts'")
        print("   - Click 'Create Service Account'")
        print("   - Name: 'dlt-bigquery'")
        print("   - Grant role: 'BigQuery Admin'")
        print("   - Click 'Done'")
        
        print("\n5. Create and Download Key")
        print("   - Click on the service account")
        print("   - Go to 'Keys' tab")
        print("   - Click 'Add Key' > 'Create new key'")
        print("   - Choose 'JSON'")
        print("   - Save to: phase3_warehouses/credentials/bigquery-key.json")
        
        print("\n6. Update .env file")
        print(f"   BIGQUERY_PROJECT_ID=your-actual-project-id")
        print(f"   BIGQUERY_CREDENTIALS_PATH=./phase3_warehouses/credentials/bigquery-key.json")
    
    return checks_passed


@dlt.resource(
    name="users",
    write_disposition="replace"
)
def load_users() -> Iterator[Dict[str, Any]]:
    """Load users from API"""
    
    print("\n📡 Fetching users from API...")
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    response.raise_for_status()
    
    users = response.json()
    print(f"✅ Found {len(users)} users")
    
    for user in users:
        yield {
            'user_id': user['id'],
            'name': user['name'],
            'username': user['username'],
            'email': user['email'],
            'phone': user['phone'],
            'website': user['website'],
            'city': user['address']['city'],
            'zipcode': user['address']['zipcode'],
            'latitude': float(user['address']['geo']['lat']),
            'longitude': float(user['address']['geo']['lng']),
            'company_name': user['company']['name'],
            'loaded_at': datetime.now().isoformat()
        }


@dlt.resource(
    name="posts",
    write_disposition="replace"
)
def load_posts() -> Iterator[Dict[str, Any]]:
    """Load posts from API"""
    
    print("\n📡 Fetching posts from API...")
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    response.raise_for_status()
    
    posts = response.json()
    print(f"✅ Found {len(posts)} posts")
    
    for post in posts:
        yield {
            'post_id': post['id'],
            'user_id': post['userId'],
            'title': post['title'],
            'body': post['body'],
            'title_length': len(post['title']),
            'body_length': len(post['body']),
            'loaded_at': datetime.now().isoformat()
        }


@dlt.source(name="jsonplaceholder_api")
def jsonplaceholder_source():
    """
    DLT source combining all resources
    """
    return [
        load_users(),
        load_posts()
    ]


def run_bigquery_pipeline():
    """
    Run the pipeline to BigQuery
    """
    
    print("\n" + "=" * 60)
    print("RUNNING PIPELINE TO BIGQUERY")
    print("=" * 60)
    
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="jsonplaceholder_to_bigquery",
        destination=dlt.destinations.bigquery(
            credentials=BIGQUERY_CREDENTIALS,
            location="US"  # or "EU", "asia-southeast1", etc.
        ),
        dataset_name=BIGQUERY_DATASET
    )
    
    print(f"\n📊 Pipeline Configuration:")
    print(f"   Pipeline: {pipeline.pipeline_name}")
    print(f"   Destination: BigQuery")
    print(f"   Project: {BIGQUERY_PROJECT_ID}")
    print(f"   Dataset: {BIGQUERY_DATASET}")
    print(f"   Location: US")
    
    # Run the pipeline
    print(f"\n🚀 Starting data load...")
    
    load_info = pipeline.run(jsonplaceholder_source())
    
    # Print results
    print("\n" + "=" * 60)
    print("LOAD COMPLETED")
    print("=" * 60)
    
    print(f"\n✅ Tables created in BigQuery:")
    for package in load_info.load_packages:
        for table_name in package.schema_update.keys():
            full_table = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
            print(f"   📊 {full_table}")
    
    # Show next steps
    print("\n" + "=" * 60)
    print("QUERY YOUR DATA IN BIGQUERY")
    print("=" * 60)
    
    print(f"\n1. Open BigQuery Console:")
    print(f"   https://console.cloud.google.com/bigquery")
    
    print(f"\n2. Navigate to your dataset:")
    print(f"   {BIGQUERY_PROJECT_ID} > {BIGQUERY_DATASET}")
    
    print(f"\n3. Try these queries:")
    
    queries = [
        ("Count users", f"SELECT COUNT(*) as user_count FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.users`"),
        ("Get top cities", f"SELECT city, COUNT(*) as count FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.users` GROUP BY city ORDER BY count DESC"),
        ("Posts per user", f"SELECT user_id, COUNT(*) as post_count FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.posts` GROUP BY user_id ORDER BY post_count DESC"),
        ("Join users and posts", f"""SELECT 
    u.name,
    u.email,
    COUNT(p.post_id) as post_count
FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.users` u
LEFT JOIN `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.posts` p ON u.user_id = p.user_id
GROUP BY u.name, u.email
ORDER BY post_count DESC""")
    ]
    
    for i, (description, query) in enumerate(queries, 1):
        print(f"\n   Query {i}: {description}")
        print(f"   ```sql")
        print(f"   {query}")
        print(f"   ```")
    
    return pipeline


def demonstrate_bigquery_features():
    """
    Show BigQuery-specific features and best practices
    """
    
    print("\n" + "=" * 60)
    print("BIGQUERY FEATURES & BEST PRACTICES")
    print("=" * 60)
    
    features = {
        "Partitioning": {
            "What": "Split tables by date/timestamp for faster queries",
            "Example": "Partition by loaded_at date",
            "Benefit": "Query only relevant partitions, save cost"
        },
        "Clustering": {
            "What": "Sort data within partitions",
            "Example": "Cluster by user_id or city",
            "Benefit": "Even faster queries and lower cost"
        },
        "Cost Control": {
            "What": "BigQuery charges by data scanned",
            "Tip": "Use SELECT specific columns, not SELECT *",
            "Tip": "Use WHERE clauses to filter partitions"
        },
        "Data Types": {
            "What": "BigQuery has specific data types",
            "STRING": "For text data",
            "INT64": "For whole numbers",
            "FLOAT64": "For decimals",
            "TIMESTAMP": "For dates/times",
            "GEOGRAPHY": "For location data"
        }
    }
    
    for feature, details in features.items():
        print(f"\n{feature}:")
        for key, value in details.items():
            print(f"  {key}: {value}")


def show_cost_estimation():
    """
    Show how to estimate BigQuery costs
    """
    
    print("\n" + "=" * 60)
    print("BIGQUERY COST ESTIMATION")
    print("=" * 60)
    
    print("\nBigQuery Pricing (as of 2024):")
    print("  Storage: $0.02 per GB per month")
    print("  Queries: $5 per TB scanned")
    print("  First 1 TB per month: FREE")
    print("  First 10 GB storage: FREE")
    
    print("\nFor this project:")
    print("  Data size: ~100 KB (very small)")
    print("  Monthly cost: $0 (well within free tier)")
    
    print("\nCost Optimization Tips:")
    print("  1. Use partitioned tables")
    print("  2. Query only needed columns")
    print("  3. Use LIMIT for testing")
    print("  4. Monitor with Billing Reports")


def main():
    """
    Main execution
    """
    
    print("\n" + "🌥️ " * 30)
    print("BIGQUERY PIPELINE - CLOUD DATA WAREHOUSE")
    print("🌥️ " * 30)
    
    # Check setup
    if not check_bigquery_setup():
        print("\n⚠️  Please complete setup steps above first.")
        return
    
    # Show features
    demonstrate_bigquery_features()
    show_cost_estimation()
    
    # Run pipeline
    try:
        pipeline = run_bigquery_pipeline()
        
        print("\n" + "=" * 60)
        print("SUCCESS! 🎉")
        print("=" * 60)
        print("\n✅ Your data is now in BigQuery!")
        print("✅ You can query it with SQL")
        print("✅ You can connect BI tools (Looker, Tableau, etc.)")
        print("✅ You can share datasets with your team")
        
    except Exception as e:
        print(f"\n❌ Error running pipeline: {e}")
        print("\nTroubleshooting:")
        print("1. Check credentials file exists and is valid")
        print("2. Verify project ID is correct")
        print("3. Ensure BigQuery API is enabled")
        print("4. Check service account has BigQuery Admin role")


if __name__ == "__main__":
    try:
        import dlt
        main()
    except ImportError:
        print("❌ DLT not installed!")
        print("Install with: pip install 'dlt[bigquery]'")
