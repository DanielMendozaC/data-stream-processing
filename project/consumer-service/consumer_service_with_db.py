# consumer_service_with_db.py
# Enhanced consumer that writes signals to BigQuery for persistence

import os
import threading
import uuid
import time
from collections import defaultdict
from datetime import datetime

from fastapi import Request
from fastapi import FastAPI, HTTPException
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery

load_dotenv()

app = FastAPI(title="Trading Intelligence API", version="2.0")

SCHEMA = """
{
    "type": "record",
    "name": "stock_data",
    "namespace": "stock_data.symbols",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "open", "type": "float"},
        {"name": "high", "type": "float"},
        {"name": "low", "type": "float"},
        {"name": "close", "type": "float"},
        {"name": "volume", "type": "int"},
        {"name": "trade_count", "type": "int"},
        {"name": "vwap", "type": "float"}
    ]
}
"""

# Global state
buffers = defaultdict(list)
signals = []  # In-memory cache for fast API responses

# BigQuery client (initialized on startup)
bigquery_client = None
table_id = None

# Statistics
stats = {
    'messages_consumed': 0,
    'signals_generated': 0,
    'db_writes_success': 0,
    'db_writes_failed': 0,
    'total_processing_time': 0,  # NEW
    'api_response_times': []      # NEW
}


def calculate_rsi(prices, period=14):
    """Calculate RSI"""
    if len(prices) < period + 1:
        return None
    
    deltas = prices.diff()
    seed = deltas[:period + 1]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    
    if down == 0:
        return None
    
    rs = up / down
    return 100 - (100 / (1 + rs))


def calculate_signal_for_symbol(symbol):
    """Helper to calculate signal from buffer"""
    if symbol not in buffers or len(buffers[symbol]) < 14:
        return None
    
    df = pd.DataFrame(buffers[symbol])
    closes = df['close'].values
    
    sma_20 = closes[-20:].mean() if len(closes) >= 20 else None
    sma_50 = closes[-50:].mean() if len(closes) >= 50 else None
    rsi = calculate_rsi(pd.Series(closes), 14)
    
    signal = 'HOLD'
    if sma_20 and sma_50:
        if sma_20 > sma_50 and rsi and rsi < 30:
            signal = 'BUY'
        elif sma_20 < sma_50 and rsi and rsi > 70:
            signal = 'SELL'
    
    return {
        'symbol': symbol,
        'timestamp': buffers[symbol][-1]['timestamp'],
        'signal': signal,
        'close': float(closes[-1]),
        'sma_20': float(sma_20) if sma_20 else None,
        'sma_50': float(sma_50) if sma_50 else None,
        'rsi': float(rsi) if rsi else None,
        'volume': int(buffers[symbol][-1]['volume'])
    }


def write_signal_to_bigquery(signal_data):
    """Write a signal to BigQuery"""
    global stats
    
    if not bigquery_client or not table_id:
        print("⚠️  BigQuery not configured, skipping DB write")
        return False
    
    try:
        # Add required fields for BigQuery
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
        
        # Insert into BigQuery
        errors = bigquery_client.insert_rows_json(table_id, [row])
        
        if errors:
            print(f"❌ BigQuery insert errors: {errors}")
            stats['db_writes_failed'] += 1
            return False
        else:
            stats['db_writes_success'] += 1
            return True
            
    except Exception as e:
        print(f"❌ BigQuery write error: {e}")
        stats['db_writes_failed'] += 1
        return False


def consumer_loop():
    """Background consumer thread with BigQuery integration"""
    global stats
    
    # Setup Schema Registry
    sr_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }
    sr_client = SchemaRegistryClient(sr_conf)
    
    # Setup deserializer
    deserializer = AvroDeserializer(sr_client, SCHEMA)
    
    # Setup consumer
    consumer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': deserializer,
        'group.id': 'stock-consumer-with-db-v2',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['stocks.raw.avro'])
    print("✓ Consumer started (with BigQuery integration)")
    
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        start_time = time.time()  # ← START TIMER
        
        symbol = msg.key()
        data = msg.value()
        stats['messages_consumed'] += 1
        
        # Add to buffer
        buffers[symbol].append({
            'timestamp': data['timestamp'],
            'close': data['close'],
            'volume': data['volume'],
        })
        
        # Keep last 50
        if len(buffers[symbol]) > 50:
            buffers[symbol].pop(0)
        
        # Calculate features if we have 14+ bars
        if len(buffers[symbol]) >= 14:
            df = pd.DataFrame(buffers[symbol])
            closes = df['close'].values
            
            sma_20 = closes[-20:].mean() if len(closes) >= 20 else None
            sma_50 = closes[-50:].mean() if len(closes) >= 50 else None
            
            rsi = calculate_rsi(pd.Series(closes), 14)
            
            # Generate signal
            signal = 'HOLD'
            if sma_20 and sma_50:
                if sma_20 > sma_50 and rsi and rsi < 30:
                    signal = 'BUY'
                elif sma_20 < sma_50 and rsi and rsi > 70:
                    signal = 'SELL'
            
            signal_data = {
                'symbol': symbol,
                'timestamp': data['timestamp'],
                'signal': signal,
                'close': float(closes[-1]),
                'sma_20': float(sma_20) if sma_20 else None,
                'sma_50': float(sma_50) if sma_50 else None,
                'rsi': float(rsi) if rsi else None,
                'volume': int(data['volume'])
            }
            
            # Dual write: in-memory (fast API) + BigQuery (persistent)
            signals.append(signal_data)
            stats['signals_generated'] += 1
            
            # Write to BigQuery
            # Write to BigQuery and check if it succeeded
            success = write_signal_to_bigquery(signal_data)

            rsi_str = f"{rsi:.2f}" if rsi else "N/A"
            db_status = "✓" if success else "✗"
            print(f"[{symbol}] {signal} | Close: {closes[-1]:.2f} | RSI: {rsi_str} | DB: {db_status}")
        
        end_time = time.time()  # ← END TIMER
        processing_time_ms = (end_time - start_time) * 1000
        stats['total_processing_time'] += processing_time_ms

        if stats['messages_consumed'] % 10 == 0:
            avg_latency = stats['total_processing_time'] / stats['messages_consumed']
            print(f"Avg processing latency: {avg_latency:.2f}ms")


@app.on_event("startup")
async def startup():
    """Initialize BigQuery client and start consumer"""
    global bigquery_client, table_id
    
    # Initialize BigQuery
    try:
        bigquery_client = bigquery.Client()
        table_id = os.getenv('BIGQUERY_TABLE_ID')
        
        if not table_id:
            print("⚠️  BIGQUERY_TABLE_ID not set in .env")
            print("⚠️  Signals will be stored in-memory only (not persistent)")
        else:
            # Verify table exists
            try:
                table = bigquery_client.get_table(table_id)
                print(f"✓ BigQuery connected: {table_id}")
            except Exception as e:
                print(f"⚠️  BigQuery table not found: {e}")
                print(f"⚠️  Run: python setup_bigquery.py")
                bigquery_client = None
                table_id = None
    except Exception as e:
        print(f"⚠️  BigQuery initialization failed: {e}")
        print(f"⚠️  Continuing without database persistence")
        bigquery_client = None
        table_id = None
    
    # Start consumer thread
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()


# API ENDPOINTS

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Trading Intelligence API",
        "version": "2.0",
        "status": "running",
        "features": ["kafka", "bigquery", "real-time-signals"],
        "endpoints": {
            "health": "/health",
            "stats": "/stats",
            "signals": "/signals (in-memory cache)",
            "signals_db": "/signals/db (from BigQuery)",
            "signals_by_symbol": "/signals/{symbol}",
            "latest": "/signals/latest"
        }
    }


@app.get("/health")
async def health():
    """Health check with enhanced stats"""
    return {
        "status": "ok",
        "bigquery_connected": bigquery_client is not None,
        "signals_in_memory": len(signals),
        "stats": stats
    }


@app.middleware("http")
async def add_timing(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration_ms = (time.time() - start) * 1000
    
    # Add to stats
    stats['api_response_times'].append(duration_ms)
    
    # Keep only last 100
    if len(stats['api_response_times']) > 100:
        stats['api_response_times'].pop(0)
    
    response.headers["X-Response-Time"] = f"{duration_ms:.2f}ms"
    return response


@app.get("/stats")
async def get_stats():
    """Get detailed statistics"""
    api_times = stats['api_response_times']
    avg_api = sum(api_times) / len(api_times) if api_times else 0

    return {
        "consumer": {
            "messages_consumed": stats['messages_consumed'],
            "signals_generated": stats['signals_generated'],
        },
        "database": {
            "connected": bigquery_client is not None,
            "table_id": table_id,
            "writes_success": stats['db_writes_success'],
            "writes_failed": stats['db_writes_failed'],
            "success_rate": f"{stats['db_writes_success'] / max(stats['signals_generated'], 1) * 100:.1f}%"
        },
        "memory": {
            "signals_cached": len(signals),
            "symbols_tracked": len(buffers),
            "buffer_sizes": {sym: len(buf) for sym, buf in buffers.items()}
        },
        "performance": {
            "avg_processing_latency_ms": stats['total_processing_time'] / max(stats['messages_consumed'], 1),
            "avg_api_response_ms": avg_api,
            "messages_per_second": "calculated_value"  # if you track this
        }
    }


@app.get("/debug/consumer-status")
async def debug_consumer():
    """Debug what the consumer is doing"""
    return {
        "bigquery_configured": {
            "client_initialized": bigquery_client is not None,
            "table_id": table_id,
            "status": "WRITING TO DB" if (bigquery_client and table_id) else "⚠️ SKIPPING DB WRITES"
        },
        "consumption": {
            "messages_consumed_this_session": stats['messages_consumed'],
            "signals_generated_this_session": stats['signals_generated'],
            "db_writes_attempted": stats['db_writes_success'] + stats['db_writes_failed'],
            "db_writes_success": stats['db_writes_success'],
            "db_writes_failed": stats['db_writes_failed']
        },
        "buffers": {
            symbol: {
                "message_count": len(buf),
                "ready_for_signals": len(buf) >= 14,
                "latest_timestamp": buf[-1]['timestamp'] if buf else None
            }
            for symbol, buf in buffers.items()
        },
        "in_memory_signals": {
            "total": len(signals),
            "latest_5": signals[-5:] if signals else []
        }
    }


@app.get("/signals")
async def get_signals(limit: int = 20):
    """Get latest signals from in-memory cache (fast)"""
    return {
        "source": "in-memory",
        "total": len(signals),
        "signals": signals[-limit:]
    }


@app.get("/signals/db")
async def get_signals_from_db(limit: int = 100, symbol: str = None):
    """Get signals from BigQuery (persistent, slower)"""
    if not bigquery_client or not table_id:
        raise HTTPException(status_code=503, detail="BigQuery not configured")
    
    try:
        # Build query
        where_clause = f"WHERE symbol = '{symbol}'" if symbol else ""
        query = f"""
        SELECT 
            symbol,
            timestamp,
            signal,
            close,
            rsi,
            sma_20,
            sma_50,
            volume
        FROM `{table_id}`
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        
        # Execute query
        df = bigquery_client.query(query).to_dataframe()
        
        return {
            "source": "bigquery",
            "total": len(df),
            "signals": df.to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {str(e)}")


@app.get("/signals/{symbol}")
async def get_symbol_signals(symbol: str, limit: int = 10):
    """Get signals for specific symbol from in-memory cache"""
    symbol_signals = [s for s in signals if s['symbol'] == symbol]
    return {
        "symbol": symbol,
        "total": len(symbol_signals),
        "signals": symbol_signals[-limit:]
    }


@app.get("/signals/latest")
async def get_latest_signals():
    """Get the most recent signal for each symbol"""
    if not bigquery_client or not table_id:
        # Fallback to in-memory
        latest = {}
        for signal in reversed(signals):
            if signal['symbol'] not in latest:
                latest[signal['symbol']] = signal
        return {"source": "in-memory", "signals": list(latest.values())}
    
    try:
        # Use the view we created
        query = f"""
        SELECT * FROM `{table_id.rsplit('.', 1)[0]}.latest_signals`
        """
        df = bigquery_client.query(query).to_dataframe()
        return {
            "source": "bigquery",
            "signals": df.to_dict('records')
        }
    except Exception as e:
        # Fallback to in-memory
        latest = {}
        for signal in reversed(signals):
            if signal['symbol'] not in latest:
                latest[signal['symbol']] = signal
        return {"source": "in-memory (fallback)", "signals": list(latest.values())}


@app.get("/signals/summary")
async def get_summary():
    """Get signal summary statistics"""
    if not bigquery_client or not table_id:
        raise HTTPException(status_code=503, detail="BigQuery not configured")
    
    try:
        query = f"""
        SELECT 
            symbol,
            COUNT(*) as total_signals,
            COUNTIF(signal = 'BUY') as buy_signals,
            COUNTIF(signal = 'SELL') as sell_signals,
            COUNTIF(signal = 'HOLD') as hold_signals,
            AVG(close) as avg_close,
            AVG(rsi) as avg_rsi,
            MIN(timestamp) as first_signal,
            MAX(timestamp) as latest_signal
        FROM `{table_id}`
        GROUP BY symbol
        """
        
        df = bigquery_client.query(query).to_dataframe()
        return {
            "summary": df.to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    

@app.post("/process/{symbol}")
async def process_symbol(symbol: str):
    """Manually calculate signal for a symbol (doesn't save)"""
    if symbol not in buffers:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}")
    
    signal_data = calculate_signal_for_symbol(symbol)
    
    if not signal_data:
        raise HTTPException(status_code=400, detail=f"Not enough data (need 14+ bars)")
    
    return {
        "message": f"Calculated signal for {symbol} (not saved)",
        "signal": signal_data,
        "note": "This is a manual calculation."
    }


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)