#!/usr/bin/env python3
"""
Setup BigQuery dataset and table for trading signals
Run this ONCE before starting the consumer
"""
import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def setup_bigquery():
    """Create BigQuery dataset and table if they don't exist"""
    
    # Initialize client
    client = bigquery.Client()
    project_id = client.project
    
    print("🏗️  Setting up BigQuery for Trading Intelligence Dashboard")
    print("=" * 60)
    print(f"Project ID: {project_id}")
    print()
    
    # Dataset configuration
    dataset_id = f"{project_id}.trading_intelligence"
    
    # Step 1: Create dataset
    print("Step 1: Creating dataset 'trading_intelligence'...")
    try:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "Trading signals and market data"
        
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"  ✅ Dataset created: {dataset_id}")
    except Exception as e:
        print(f"  ⚠️  Dataset may already exist: {e}")
    print()
    
    # Step 2: Create table with schema
    print("Step 2: Creating table 'signals'...")
    table_id = f"{dataset_id}.signals"
    
    schema = [
        bigquery.SchemaField("signal_id", "STRING", mode="REQUIRED", 
                            description="Unique identifier for this signal"),
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED",
                            description="Stock symbol (e.g., AAPL, GOOGL)"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED",
                            description="Time when the bar/signal was generated"),
        bigquery.SchemaField("signal", "STRING", mode="REQUIRED",
                            description="Trading signal: BUY, SELL, or HOLD"),
        bigquery.SchemaField("close", "FLOAT", mode="REQUIRED",
                            description="Closing price of the bar"),
        bigquery.SchemaField("rsi", "FLOAT", mode="NULLABLE",
                            description="Relative Strength Index (14-period)"),
        bigquery.SchemaField("sma_20", "FLOAT", mode="NULLABLE",
                            description="20-period Simple Moving Average"),
        bigquery.SchemaField("sma_50", "FLOAT", mode="NULLABLE",
                            description="50-period Simple Moving Average"),
        bigquery.SchemaField("volume", "INTEGER", mode="REQUIRED",
                            description="Trading volume for this bar"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED",
                            description="When this record was inserted into BigQuery"),
    ]
    
    try:
        table = bigquery.Table(table_id, schema=schema)
        
        # Partition by day (cost optimization)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp",
        )
        
        # Cluster by symbol for faster queries
        table.clustering_fields = ["symbol"]
        
        table = client.create_table(table, exists_ok=True)
        print(f"  ✅ Table created: {table_id}")
        print(f"  📊 Partitioned by: timestamp (daily)")
        print(f"  🔍 Clustered by: symbol")
    except Exception as e:
        print(f"  ⚠️  Table may already exist: {e}")
    print()
    
    # Step 3: Verify setup
    print("Step 3: Verifying setup...")
    try:
        table = client.get_table(table_id)
        print(f"  ✅ Table verified")
        print(f"  📏 Schema fields: {len(table.schema)}")
        print(f"  📊 Partitioning: {table.time_partitioning.type_}")
        print(f"  🔍 Clustering: {table.clustering_fields}")
        print()
        
        # Show schema
        print("Table Schema:")
        print("-" * 60)
        for field in table.schema:
            nullable = "NULL" if field.mode == "NULLABLE" else "NOT NULL"
            print(f"  {field.name:15} {field.field_type:10} {nullable}")
        print()
        
    except Exception as e:
        print(f"  ❌ Verification failed: {e}")
        return False
    
    # Step 4: Create useful views
    print("Step 4: Creating helper views...")
    
    # View 1: Latest signals per symbol
    view_id = f"{dataset_id}.latest_signals"
    view_query = f"""
    SELECT 
        symbol,
        timestamp,
        signal,
        close,
        rsi,
        sma_20,
        sma_50,
        volume
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
        FROM `{table_id}`
    )
    WHERE rn = 1
    """
    
    try:
        view = bigquery.Table(view_id)
        view.view_query = view_query
        view = client.create_table(view, exists_ok=True)
        print(f"  ✅ View created: latest_signals")
    except Exception as e:
        print(f"  ⚠️  View may already exist: {e}")
    
    # View 2: Daily signal summary
    summary_view_id = f"{dataset_id}.daily_summary"
    summary_query = f"""
    SELECT 
        DATE(timestamp) as date,
        symbol,
        COUNT(*) as total_signals,
        COUNTIF(signal = 'BUY') as buy_signals,
        COUNTIF(signal = 'SELL') as sell_signals,
        COUNTIF(signal = 'HOLD') as hold_signals,
        AVG(close) as avg_close,
        AVG(rsi) as avg_rsi
    FROM `{table_id}`
    GROUP BY date, symbol
    ORDER BY date DESC, symbol
    """
    
    try:
        view = bigquery.Table(summary_view_id)
        view.view_query = summary_query
        view = client.create_table(view, exists_ok=True)
        print(f"  ✅ View created: daily_summary")
    except Exception as e:
        print(f"  ⚠️  View may already exist: {e}")
    
    print()
    print("=" * 60)
    print("✅ BigQuery setup complete!")
    print()
    print("📋 Resources created:")
    print(f"   Dataset: {dataset_id}")
    print(f"   Table:   {table_id}")
    print(f"   View:    {view_id}")
    print(f"   View:    {summary_view_id}")
    print()
    print("🔗 Next steps:")
    print("   1. Update your consumer to use this table")
    print("   2. Run consumer to start inserting data")
    print("   3. Query data: bq query --use_legacy_sql=false 'SELECT * FROM")
    print(f"      `{table_id}` LIMIT 10'")
    print()
    
    # Save table ID to env file for consumer
    table_ref = f"{project_id}.trading_intelligence.signals"
    print(f"💾 Add this to your .env file:")
    print(f"   BIGQUERY_TABLE_ID={table_ref}")
    print()
    
    return True

if __name__ == "__main__":
    import sys
    
    try:
        success = setup_bigquery()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        print()
        print("Common issues:")
        print("  1. Make sure you're authenticated: gcloud auth application-default login")
        print("  2. Verify project is set: gcloud config get-value project")
        print("  3. Enable BigQuery API: gcloud services enable bigquery.googleapis.com")
        sys.exit(1)