# processing-service/signal_processor.py

import os
import requests
from requests.auth import HTTPBasicAuth
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="ksqlDB Processing Service", version="1.0")

KSQLDB_ENDPOINT = os.getenv("KSQLDB_ENDPOINT")
KSQLDB_API_KEY = os.getenv("KSQLDB_API_KEY")
KSQLDB_API_SECRET = os.getenv("KSQLDB_API_SECRET")

stats = {
    'status': 'initializing',
    'streams_created': 0,
    'error': None
}


def execute_ksql(statement):
    """Execute ksqlDB statement with Confluent Cloud auth"""
    try:
        response = requests.post(
            f"{KSQLDB_ENDPOINT}/ksql",
            json={
                "ksql": statement,
                "streamsProperties": {
                    "ksql.streams.auto.offset.reset": "earliest"
                }
            },
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            auth=HTTPBasicAuth(KSQLDB_API_KEY, KSQLDB_API_SECRET)
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP {e.response.status_code}: {e.response.text}")
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


def setup_ksqldb_pipeline():
    """Create ksqlDB streams/tables - V3 NUCLEAR OPTION"""
    
    statements = [
        # 1. Source stream (Reading from NEW V3 TOPIC)
        """
        CREATE STREAM IF NOT EXISTS stock_raw_stream_v3 
        WITH (
            KAFKA_TOPIC='stocks.raw.v3',
            VALUE_FORMAT='AVRO'
        );
        """,
        
        # 2. Transformation stream (New name v3)
        """
        CREATE STREAM IF NOT EXISTS trading_signals_v3 AS
        SELECT 
            symbol,
            timestamp,
            CASE 
                WHEN close > 150.0 THEN 'BUY'
                WHEN close < 145.0 THEN 'SELL'
                ELSE 'HOLD'
            END as signal,
            close,
            close as sma_20,
            close as sma_50,
            CAST(NULL AS DOUBLE) as rsi,
            volume
        FROM stock_raw_stream_v3
        EMIT CHANGES;
        """,
        
        # 3. Output stream (Writing to NEW V3 OUTPUT TOPIC)
        """
        CREATE STREAM IF NOT EXISTS stocks_signals_output_v3 
        WITH (
            KAFKA_TOPIC='stocks.signals.v3',
            VALUE_FORMAT='AVRO'
        ) AS
        SELECT * FROM trading_signals_v3
        EMIT CHANGES;
        """
    ]
    
    try:
        print("🔄 Setting up ksqlDB pipeline (V3)...")
        
        for i, stmt in enumerate(statements, 1):
            requests.post(
                f"{KSQLDB_ENDPOINT}/ksql",
                json={
                    "ksql": stmt,
                    "streamsProperties": {
                        "ksql.streams.auto.offset.reset": "earliest"
                    }
                },
                headers={"Content-Type": "application/vnd.ksql.v1+json"},
                auth=HTTPBasicAuth(KSQLDB_API_KEY, KSQLDB_API_SECRET)
            ).raise_for_status()
            
            print(f"✓ Statement {i}/3 executed")
            
        stats['status'] = 'running'
        print("✅ ksqlDB pipeline is running!")
        
    except Exception as e:
        print(f"❌ Pipeline setup failed: {e}")


@app.on_event("startup")
async def startup():
    """Initialize ksqlDB pipeline"""
    if not all([KSQLDB_ENDPOINT, KSQLDB_API_KEY, KSQLDB_API_SECRET]):
        print("⚠️  Missing ksqlDB credentials")
        stats['status'] = 'not_configured'
        return
    
    setup_ksqldb_pipeline()


@app.get("/")
async def root():
    return {
        "service": "ksqlDB Signal Processing",
        "framework": "Confluent Cloud ksqlDB",
        "status": stats['status']
    }


@app.get("/health")
async def health():
    return {
        "status": stats['status'],
        "stats": stats
    }


@app.get("/stats")
async def get_stats():
    return stats


@app.get("/queries")
async def list_queries():
    try:
        result = execute_ksql("SHOW QUERIES;")
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/streams")
async def list_streams():
    try:
        result = execute_ksql("SHOW STREAMS;")
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)