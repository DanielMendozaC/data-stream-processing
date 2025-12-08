# consumer-service/consumer_api.py

import os
import threading
import uuid
import time
from collections import defaultdict
from datetime import datetime

from fastapi import FastAPI, HTTPException
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

app = FastAPI(title="Trading Signals Consumer API", version="3.0")

# Signal schema (from processing-service)
SIGNAL_SCHEMA = """
{
    "type": "record",
    "name": "signal_data",
    "namespace": "signals",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "signal", "type": "string"},
        {"name": "close", "type": "float"},
        {"name": "rsi", "type": ["null", "float"]},
        {"name": "sma_20", "type": ["null", "float"]},
        {"name": "sma_50", "type": ["null", "float"]},
        {"name": "volume", "type": "int"}
    ]
}
"""

# State
signals = []  # In-memory cache
bigquery_client = None
table_id = None

# Stats
stats = {
    'signals_consumed': 0,
    'db_writes_success': 0,
    'db_writes_failed': 0
}


def write_signal_to_bigquery(signal_data):
    """Write signal to BigQuery"""
    global stats
    
    if not bigquery_client or not table_id:
        return False
    
    try:
        row = {
            'signal_id': str(uuid.uuid4()),
            'symbol': signal_data['symbol'],
            'timestamp': signal_data['timestamp'],
            'signal': signal_data['signal'],
            'close': signal_data['close'],
            'rsi': signal_data.get('rsi'),
            'sma_20': signal_data.get('sma_20'),
            'sma_50': signal_data.get('sma_50'),
            'volume': signal_data['volume'],
            'created_at': datetime.utcnow().isoformat()
        }
        
        errors = bigquery_client.insert_rows_json(table_id, [row])
        
        if errors:
            print(f"❌ BigQuery error: {errors}")
            stats['db_writes_failed'] += 1
            return False
        else:
            stats['db_writes_success'] += 1
            return True
            
    except Exception as e:
        print(f"❌ BigQuery error: {e}")
        stats['db_writes_failed'] += 1
        return False


def consumer_loop():
    """
    Consumes pre-calculated signals from stocks.signals topic
    Writes to BigQuery for persistence
    """
    global stats
    
    # Schema Registry
    sr_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }
    sr_client = SchemaRegistryClient(sr_conf)
    
    # Consumer
    deserializer = AvroDeserializer(sr_client)
    consumer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': deserializer,
        'group.id': 'signal-consumer-service-v3', # Changed group ID to ensure fresh read
        'auto.offset.reset': 'earliest'
    }
    
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['stocks.signals.v3'])
    print("✓ Consumer started - reading from stocks.signals.v3")
    
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            print(f"❌ Error: {msg.error()}")
            continue
        
        raw_data = msg.value()
        
        # ksqlDB sends keys as "SYMBOL", "CLOSE", etc. We convert them to "symbol", "close".
        signal_data = {k.lower(): v for k, v in raw_data.items()}

        stats['signals_consumed'] += 1
        
        # Cache in memory
        signals.append(signal_data)
        if len(signals) > 1000:
            signals.pop(0)
        
        # Write to BigQuery
        success = write_signal_to_bigquery(signal_data)
        
        db_status = "✓" if success else "✗"
        print(f"[{signal_data['symbol']}] {signal_data['signal']} | DB: {db_status}")


@app.on_event("startup")
async def startup():
    """Initialize BigQuery and start consumer"""
    global bigquery_client, table_id
    
    try:
        bigquery_client = bigquery.Client()
        table_id = os.getenv('BIGQUERY_TABLE_ID')
        
        if table_id:
            bigquery_client.get_table(table_id)
            print(f"✓ BigQuery connected: {table_id}")
        else:
            print("⚠️  BIGQUERY_TABLE_ID not set")
    except Exception as e:
        print(f"⚠️  BigQuery error: {e}")
        bigquery_client = None
    
    # Start consumer
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()


@app.get("/")
async def root():
    return {
        "service": "Trading Signals Consumer API",
        "version": "3.0",
        "description": "Consumes signals from processing-service, stores in BigQuery"
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "bigquery_connected": bigquery_client is not None,
        "signals_cached": len(signals),
        "stats": stats
    }


@app.get("/signals")
async def get_signals(limit: int = 20):
    """Get latest signals from cache"""
    return {
        "source": "in-memory-cache",
        "total": len(signals),
        "signals": signals[-limit:]
    }


@app.get("/signals/db")
async def get_signals_from_db(limit: int = 100, symbol: str = None):
    """Get signals from BigQuery"""
    if not bigquery_client or not table_id:
        raise HTTPException(503, "BigQuery not configured")
    
    where_clause = f"WHERE symbol = '{symbol}'" if symbol else ""
    query = f"""
    SELECT * FROM `{table_id}`
    {where_clause}
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    
    df = bigquery_client.query(query).to_dataframe()
    return {
        "source": "bigquery",
        "total": len(df),
        "signals": df.to_dict('records')
    }


@app.get("/signals/{symbol}")
async def get_symbol_signals(symbol: str, limit: int = 10):
    """Get signals for specific symbol"""
    symbol_signals = [s for s in signals if s['symbol'] == symbol]
    return {
        "symbol": symbol,
        "total": len(symbol_signals),
        "signals": symbol_signals[-limit:]
    }


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)