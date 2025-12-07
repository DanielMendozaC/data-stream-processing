# consumer_service.py

import os
import threading
from collections import defaultdict
from datetime import datetime

from fastapi import FastAPI
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import topic_record_subject_name_strategy
from confluent_kafka.serialization import StringDeserializer
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

app = FastAPI()

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
signals = []

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


def consumer_loop():
    """Background consumer thread"""
    # Setup Schema Registry
    sr_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }
    sr_client = SchemaRegistryClient(sr_conf)
    
    # Setup deserializer with schema
    deserializer = AvroDeserializer(
        sr_client,
        SCHEMA,
        # conf={'subject.name.strategy': topic_record_subject_name_strategy}
    )
    
    # Setup consumer
    consumer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': deserializer,
        'group.id': 'stock-consumer',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['stocks.raw.avro'])
    print("✓ Consumer started")
    
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        
        symbol = msg.key()
        data = msg.value()
        
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
            
            signals.append(signal_data)
            rsi_str = f"{rsi:.2f}" if rsi else "N/A"
            print(f"[{symbol}] {signal} | Close: {closes[-1]:.2f} | RSI: {rsi_str}")


@app.on_event("startup")
async def startup():
    """Start background consumer on startup"""
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()


@app.get("/health")
def health():
    """Health check"""
    return {"status": "ok", "signals": len(signals)}


@app.get("/signals")
def get_signals(limit: int = 20):
    """Get latest signals"""
    return {"total": len(signals), "signals": signals[-limit:]}


@app.get("/signals/{symbol}")
def get_symbol_signals(symbol: str, limit: int = 10):
    """Get signals for specific symbol"""
    symbol_signals = [s for s in signals if s['symbol'] == symbol]
    return {"symbol": symbol, "total": len(symbol_signals), "signals": symbol_signals[-limit:]}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)