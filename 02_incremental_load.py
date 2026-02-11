"""
WEEK 4 - Script 2: Incremental Loading with DLT
================================================

This script teaches you:
1. What incremental loading is and why it matters
2. Using DLT's built-in incremental features
3. State management
4. Avoiding duplicate data

Incremental loading is crucial for:
- Performance: Don't reload all data every time
- Cost: Reduce API calls and warehouse processing
- Freshness: Get new data quickly
"""

import dlt
from dlt.sources.helpers import requests
from typing import Iterator, Dict, Any
from datetime import datetime, timedelta


@dlt.resource(
    name="posts_incremental",
    write_disposition="merge",  # Merge updates existing records
    primary_key="post_id"  # Required for merge
)
def load_posts_incremental(
    last_post_id: dlt.sources.incremental[int] = dlt.sources.incremental(
        "post_id",  # The field to track
        initial_value=0  # Start from 0
    )
) -> Iterator[Dict[str, Any]]:
    """
    Load posts incrementally based on post_id
    
    This will only load posts newer than the last loaded post
    DLT automatically tracks the state
    """
    
    print(f"\n📊 Loading posts incrementally...")
    print(f"   Last post_id loaded: {last_post_id.last_value}")
    
    # Get all posts
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    response.raise_for_status()
    posts = response.json()
    
    # Filter to only new posts
    new_posts = [p for p in posts if p['id'] > last_post_id.last_value]
    
    print(f"   Total posts in API: {len(posts)}")
    print(f"   New posts to load: {len(new_posts)}")
    
    for post in new_posts:
        yield {
            'post_id': post['id'],
            'user_id': post['userId'],
            'title': post['title'],
            'body': post['body'],
            'loaded_at': datetime.now().isoformat()
        }


@dlt.resource(
    name="users_with_timestamp",
    write_disposition="merge",
    primary_key="user_id"
)
def load_users_with_timestamp() -> Iterator[Dict[str, Any]]:
    """
    Load users with timestamp for tracking
    In real scenarios, you'd filter by updated_at timestamp
    """
    
    print(f"\n👥 Loading users...")
    
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    response.raise_for_status()
    users = response.json()
    
    print(f"   Found {len(users)} users")
    
    for user in users:
        yield {
            'user_id': user['id'],
            'name': user['name'],
            'username': user['username'],
            'email': user['email'],
            'city': user['address']['city'],
            'company_name': user['company']['name'],
            'loaded_at': datetime.now().isoformat(),
            # Simulate an updated_at field
            'updated_at': datetime.now().isoformat()
        }


def demonstrate_incremental_loading():
    """
    Demonstrate how incremental loading works
    """
    
    print("\n" + "=" * 60)
    print("INCREMENTAL LOADING DEMONSTRATION")
    print("=" * 60)
    
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="incremental_demo",
        destination="duckdb",
        dataset_name="incremental_data"
    )
    
    # First run - loads all data
    print("\n[RUN 1] Initial load - should load all posts")
    print("-" * 60)
    
    @dlt.source
    def posts_source():
        return load_posts_incremental()
    
    load_info = pipeline.run(posts_source())
    print(f"✅ First load completed")
    
    # Check what was loaded
    with pipeline.sql_client() as client:
        result = client.execute_sql(
            "SELECT COUNT(*) as count, MAX(post_id) as max_id FROM posts_incremental"
        )
        row = list(result)[0]
        print(f"   Records in table: {row[0]}")
        print(f"   Highest post_id: {row[1]}")
    
    # Second run - should load nothing (no new data)
    print("\n[RUN 2] Second load - should load 0 new posts")
    print("-" * 60)
    
    load_info = pipeline.run(posts_source())
    print(f"✅ Second load completed")
    
    # Check again
    with pipeline.sql_client() as client:
        result = client.execute_sql(
            "SELECT COUNT(*) as count, MAX(post_id) as max_id FROM posts_incremental"
        )
        row = list(result)[0]
        print(f"   Records in table: {row[0]}")
        print(f"   Highest post_id: {row[1]}")
    
    print("\n💡 Key Insight:")
    print("   DLT tracked the state automatically!")
    print("   On the second run, it knew not to reload the same data.")


def show_state_management():
    """
    Show how DLT manages state
    """
    
    print("\n" + "=" * 60)
    print("DLT STATE MANAGEMENT")
    print("=" * 60)
    
    print("\nDLT stores state in the .dlt/ directory:")
    print("  - Pipeline configuration")
    print("  - Last loaded values")
    print("  - Schema versions")
    
    print("\nFor incremental loads, DLT tracks:")
    print("  - Last value of incremental field (e.g., max post_id)")
    print("  - Last run timestamp")
    print("  - Schema changes")
    
    print("\nThis allows DLT to:")
    print("  ✅ Resume from where it left off")
    print("  ✅ Detect and load only new data")
    print("  ✅ Handle failures gracefully")
    print("  ✅ Support multiple concurrent pipelines")


def compare_load_strategies():
    """
    Compare different loading strategies
    """
    
    print("\n" + "=" * 60)
    print("LOADING STRATEGY COMPARISON")
    print("=" * 60)
    
    strategies = {
        "Full Refresh (Replace)": {
            "How it works": "Delete all data and reload everything",
            "Pros": "Simple, always in sync",
            "Cons": "Slow for large datasets, high API usage",
            "Best for": "Small datasets, complete snapshots",
            "DLT config": "write_disposition='replace'"
        },
        "Append Only": {
            "How it works": "Add new records without checking for duplicates",
            "Pros": "Fast, simple",
            "Cons": "Can create duplicates",
            "Best for": "Event logs, immutable data",
            "DLT config": "write_disposition='append'"
        },
        "Incremental (Merge)": {
            "How it works": "Load only new/changed records, update existing",
            "Pros": "Efficient, no duplicates, handles updates",
            "Cons": "Requires primary key, slightly complex",
            "Best for": "Most use cases",
            "DLT config": "write_disposition='merge', primary_key='id'"
        }
    }
    
    for strategy, details in strategies.items():
        print(f"\n{strategy}:")
        for key, value in details.items():
            print(f"  {key}: {value}")


def real_world_incremental_pattern():
    """
    Show a real-world incremental loading pattern
    """
    
    print("\n" + "=" * 60)
    print("REAL-WORLD PATTERN: TIMESTAMP-BASED INCREMENTAL")
    print("=" * 60)
    
    code_example = '''
@dlt.resource(
    name="orders",
    write_disposition="merge",
    primary_key="order_id"
)
def load_orders_incremental(
    updated_since: dlt.sources.incremental[str] = dlt.sources.incremental(
        "updated_at",
        initial_value="2024-01-01T00:00:00"
    )
):
    """
    Real API pattern: Filter by updated_at timestamp
    """
    
    # Most APIs support filtering by date
    params = {
        'updated_since': updated_since.last_value,
        'limit': 1000
    }
    
    response = requests.get("https://api.example.com/orders", params=params)
    orders = response.json()
    
    for order in orders:
        yield {
            'order_id': order['id'],
            'customer_id': order['customer_id'],
            'total_amount': order['total'],
            'status': order['status'],
            'created_at': order['created_at'],
            'updated_at': order['updated_at']  # DLT tracks this
        }
'''
    
    print(code_example)
    
    print("\n💡 Why this pattern is great:")
    print("  ✅ Only fetches records updated since last run")
    print("  ✅ Handles updates to existing records")
    print("  ✅ Efficient use of API and warehouse")
    print("  ✅ DLT automatically tracks the timestamp")


def main():
    """
    Main execution function
    """
    
    print("\n" + "⚡" * 30)
    print("WEEK 4 - INCREMENTAL LOADING WITH DLT")
    print("⚡" * 30)
    
    # Show concepts
    show_state_management()
    compare_load_strategies()
    
    # Run demonstration
    demonstrate_incremental_loading()
    
    # Show real-world pattern
    real_world_incremental_pattern()
    
    # Summary
    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS")
    print("=" * 60)
    
    print("\n1. Incremental loading is essential for production")
    print("   - Saves time and money")
    print("   - Reduces API load")
    print("   - Keeps data fresh")
    
    print("\n2. DLT makes incremental loading easy")
    print("   - Automatic state management")
    print("   - Built-in deduplication with merge")
    print("   - Multiple incremental strategies")
    
    print("\n3. Choose the right strategy")
    print("   - Use 'replace' for small, complete refreshes")
    print("   - Use 'append' for immutable event logs")
    print("   - Use 'merge' for most production use cases")
    
    print("\n4. Production best practices")
    print("   - Always use primary keys with merge")
    print("   - Track timestamps when possible")
    print("   - Monitor state files")
    print("   - Test incremental logic thoroughly")
    
    print("\n" + "=" * 60)
    print("Next Steps:")
    print("1. Experiment with different write dispositions")
    print("2. Check the .dlt/ folder to see state files")
    print("3. Try running the pipeline multiple times")
    print("4. Ready for Week 5: Data Warehouses!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        import dlt
        main()
    except ImportError:
        print("❌ DLT is not installed!")
        print("Install with: pip install dlt[duckdb]")
