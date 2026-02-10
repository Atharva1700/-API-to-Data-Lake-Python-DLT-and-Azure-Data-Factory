"""
WEEK 3 - Script 1: Your First DLT Pipeline
===========================================

This script introduces DLT (Data Load Tool):
1. What is DLT and why use it?
2. Creating a simple pipeline
3. Understanding sources and resources
4. Loading data to a destination

DLT simplifies data ingestion by:
- Automatic schema creation
- Built-in retry logic
- State management
- Multiple destination support
"""

import dlt
from typing import Iterator, Dict, Any
import requests


def load_users() -> Iterator[Dict[str, Any]]:
    """
    A DLT resource that loads users from API
    
    In DLT terminology:
    - Resource: A function that yields data
    - Yields: Produces data one record at a time
    """
    
    print("📡 Fetching users from API...")
    
    # Make API call
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    response.raise_for_status()
    
    users = response.json()
    
    print(f"✅ Found {len(users)} users")
    
    # Yield each user
    # DLT will process them automatically
    for user in users:
        # Flatten the nested structure for the warehouse
        yield {
            'user_id': user['id'],
            'name': user['name'],
            'username': user['username'],
            'email': user['email'],
            'phone': user['phone'],
            'website': user['website'],
            'street': user['address']['street'],
            'suite': user['address']['suite'],
            'city': user['address']['city'],
            'zipcode': user['address']['zipcode'],
            'latitude': user['address']['geo']['lat'],
            'longitude': user['address']['geo']['lng'],
            'company_name': user['company']['name'],
            'company_catchphrase': user['company']['catchPhrase'],
            'company_bs': user['company']['bs']
        }


def load_posts() -> Iterator[Dict[str, Any]]:
    """
    A DLT resource that loads posts from API
    """
    
    print("📡 Fetching posts from API...")
    
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    response.raise_for_status()
    
    posts = response.json()
    
    print(f"✅ Found {len(posts)} posts")
    
    for post in posts:
        yield {
            'post_id': post['id'],
            'user_id': post['userId'],
            'title': post['title'],
            'body': post['body']
        }


@dlt.source(name="jsonplaceholder")
def jsonplaceholder_source():
    """
    A DLT source that contains multiple resources
    
    In DLT terminology:
    - Source: A collection of related resources
    - @dlt.source: Decorator that marks this as a DLT source
    """
    
    return [
        dlt.resource(load_users(), name="users", write_disposition="replace"),
        dlt.resource(load_posts(), name="posts", write_disposition="replace")
    ]


def run_pipeline_to_duckdb():
    """
    Run the pipeline and load to DuckDB (local database)
    DuckDB is great for learning because it requires no setup
    """
    
    print("\n" + "=" * 60)
    print("RUNNING DLT PIPELINE TO DUCKDB")
    print("=" * 60)
    
    # Create a pipeline
    # Think of a pipeline as a "job" that moves data from source to destination
    pipeline = dlt.pipeline(
        pipeline_name="jsonplaceholder_pipeline",
        destination="duckdb",  # We're using DuckDB for local testing
        dataset_name="jsonplaceholder_data"  # Name of the schema/dataset
    )
    
    # Run the pipeline
    print("\n🚀 Starting pipeline run...")
    
    load_info = pipeline.run(jsonplaceholder_source())
    
    # Print results
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED")
    print("=" * 60)
    
    print(f"\n✅ Pipeline: {load_info.pipeline.pipeline_name}")
    print(f"✅ Destination: {load_info.pipeline.destination}")
    print(f"✅ Dataset: {load_info.pipeline.dataset_name}")
    
    # Show what was loaded
    print(f"\n📊 Load Summary:")
    for package in load_info.load_packages:
        print(f"\nPackage: {package.package_id}")
        for table_name, table_info in package.schema_update.items():
            print(f"  Table: {table_name}")
    
    # Access the loaded data
    print("\n" + "=" * 60)
    print("QUERYING LOADED DATA")
    print("=" * 60)
    
    # DuckDB connection
    with pipeline.sql_client() as client:
        # Query users
        print("\n📊 Sample Users:")
        users = client.execute_sql(
            "SELECT user_id, name, email, city FROM users LIMIT 5"
        )
        for row in users:
            print(f"  {row[0]}: {row[1]} ({row[2]}) - {row[3]}")
        
        # Query posts
        print("\n📝 Sample Posts:")
        posts = client.execute_sql(
            "SELECT post_id, user_id, LEFT(title, 50) as title FROM posts LIMIT 5"
        )
        for row in posts:
            print(f"  Post {row[0]} by User {row[1]}: {row[2]}...")
        
        # Count records
        print("\n📈 Record Counts:")
        counts = client.execute_sql("""
            SELECT 
                (SELECT COUNT(*) FROM users) as user_count,
                (SELECT COUNT(*) FROM posts) as post_count
        """)
        for row in counts:
            print(f"  Users: {row[0]}")
            print(f"  Posts: {row[1]}")


def explain_dlt_concepts():
    """
    Print an explanation of DLT concepts
    """
    
    print("\n" + "=" * 60)
    print("DLT KEY CONCEPTS")
    print("=" * 60)
    
    concepts = {
        "Pipeline": "A workflow that moves data from source to destination",
        "Source": "A collection of related data resources (e.g., all JSONPlaceholder endpoints)",
        "Resource": "A single data entity (e.g., users, posts)",
        "Destination": "Where data is loaded (e.g., BigQuery, Snowflake, DuckDB)",
        "Schema": "The structure of your tables (created automatically by DLT)",
        "Write Disposition": "How to handle existing data (replace, append, merge)",
        "State": "Information DLT stores to track what's been loaded"
    }
    
    for concept, explanation in concepts.items():
        print(f"\n{concept}:")
        print(f"  {explanation}")


def show_write_dispositions():
    """
    Explain write dispositions
    """
    
    print("\n" + "=" * 60)
    print("WRITE DISPOSITIONS EXPLAINED")
    print("=" * 60)
    
    print("\n1. REPLACE:")
    print("   - Drops existing table and creates new one")
    print("   - Use for: Complete refreshes, small datasets")
    print("   - Example: Daily snapshot of all users")
    
    print("\n2. APPEND:")
    print("   - Adds new rows to existing table")
    print("   - Use for: Event logs, never-changing data")
    print("   - Example: Transaction logs, sensor readings")
    
    print("\n3. MERGE:")
    print("   - Updates existing rows and adds new ones")
    print("   - Use for: Incremental updates, changing data")
    print("   - Example: User profiles that get updated")
    print("   - Requires a primary key")


def main():
    """
    Main execution function
    """
    
    print("\n" + "🎓" * 30)
    print("WEEK 3 - YOUR FIRST DLT PIPELINE")
    print("🎓" * 30)
    
    # Explain concepts
    explain_dlt_concepts()
    show_write_dispositions()
    
    # Run the pipeline
    run_pipeline_to_duckdb()
    
    # Summary
    print("\n" + "=" * 60)
    print("CONGRATULATIONS!")
    print("=" * 60)
    print("\n🎉 You just created your first DLT pipeline!")
    
    print("\n✅ What you learned:")
    print("  1. How to create a DLT source")
    print("  2. How to create DLT resources")
    print("  3. How to run a pipeline")
    print("  4. How to query loaded data")
    
    print("\n📂 DLT created these artifacts:")
    print("  - jsonplaceholder_pipeline.duckdb (database file)")
    print("  - .dlt/ folder (configuration and state)")
    
    print("\n🔍 Next Steps:")
    print("  1. Check the DuckDB file with a SQL viewer")
    print("  2. Explore the .dlt/ folder")
    print("  3. Try modifying the resources")
    print("  4. Run: python phase2_dlt/02_incremental_load.py")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    # Make sure DLT is installed
    try:
        import dlt
        main()
    except ImportError:
        print("❌ DLT is not installed!")
        print("\nInstall it with:")
        print("  pip install dlt")
        print("\nFor specific destinations:")
        print("  pip install dlt[duckdb]")
        print("  pip install dlt[bigquery]")
        print("  pip install dlt[snowflake]")
