# consumer_service.py
import os
import json
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

# Global storage
price_buffer = defaultdict(list)  # Buffer per symbol
signals_generated = []

# Avro schema
avro_schema_str = """
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

def calculate_rsi(prices, period=14):
    """Calculate RSI from price series"""
    if len(prices) < period + 1:
        return None
    
    deltas = prices.diff()
    seed = deltas[:period+1]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = 100 - (100 / (1 + rs))
    return rsi

def get_consumer():
    """Create and return configured consumer"""
    schema_registry_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }
    
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        avro_schema_str,
        conf={'subject.name.strategy': topic_record_subject_name_strategy}
    )
    
    consumer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': 'feature-signal-generator-group',
        'auto.offset.reset': 'earliest'
    }
    
    return DeserializingConsumer(consumer_conf)

def continuous_consumer():
    """Runs in background. Reads messages, calculates features, generates signals"""
    consumer = get_consumer()
    consumer.subscribe(['stocks.raw.avro'])
    
    print("Consumer started, listening on stocks.raw.avro...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            # Deserialize message
            message = msg.value()
            symbol = msg.key()
            
            # Add to buffer for this symbol
            price_buffer[symbol].append({
                'timestamp': message['timestamp'],
                'open': message['open'],
                'high': message['high'],
                'low': message['low'],
                'close': message['close'],
                'volume': message['volume'],
                'symbol': symbol
            })
            
            # Keep only last 50 bars per symbol
            if len(price_buffer[symbol]) > 50:
                price_buffer[symbol].pop(0)
            
            # Calculate features and generate signal when we have enough data
            if len(price_buffer[symbol]) >= 14:  # Min for momentum and RSI
                df = pd.DataFrame(price_buffer[symbol])
                
                # Calculate features
                closes = df['close'].values
                
                sma_20 = closes[-20:].mean() if len(closes) >= 20 else None
                sma_50 = closes[-50:].mean() if len(closes) >= 50 else None
                
                # Simple RSI calculation
                price_series = pd.Series(closes)
                rsi = calculate_rsi(price_series, period=14)
                
                # Volume ratio
                volumes = df['volume'].values
                volume_ratio = message['volume'] / volumes.mean()
                
                # Price momentum (5 bar change)
                price_momentum = (closes[-1] - closes[-5]) / closes[-5] if len(closes) >= 5 else 0
                
                # Generate signal
                signal = 'HOLD'
                if sma_20 is not None and sma_50 is not None:
                    if sma_20 > sma_50 and rsi is not None and rsi < 30:
                        signal = 'BUY'
                    elif sma_20 < sma_50 and rsi is not None and rsi > 70:
                        signal = 'SELL'
                
                # Store signal
                signal_data = {
                    'symbol': symbol,
                    'timestamp': message['timestamp'],
                    'signal': signal,
                    'close': float(message['close']),
                    'sma_20': float(sma_20) if sma_20 else None,
                    'sma_50': float(sma_50) if sma_50 else None,
                    'rsi': float(rsi) if rsi else None,
                    'volume_ratio': float(volume_ratio),
                    'price_momentum': float(price_momentum),
                    'generated_at': datetime.now().isoformat()
                }
                
                signals_generated.append(signal_data)
                
                # Print to console
                sma20_str = f"{sma_20:.2f}" if sma_20 else "N/A"
                sma50_str = f"{sma_50:.2f}" if sma_50 else "N/A"
                rsi_str = f"{rsi:.2f}" if rsi else "N/A"
                print(f"[{symbol}] {signal} | SMA20: {sma20_str} | SMA50: {sma50_str} | RSI: {rsi_str}")
    
    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        consumer.close()

@app.on_event("startup")
async def startup_event():
    """Start background consumer thread on app startup"""
    thread = threading.Thread(target=continuous_consumer, daemon=True)
    thread.start()
    print("Background consumer thread started")

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "running",
        "signals_count": len(signals_generated),
        "buffered_symbols": len(price_buffer),
        "symbols": list(price_buffer.keys())
    }

@app.get("/signals")
async def get_signals(limit: int = 20):
    """Get latest signals"""
    return {
        "total": len(signals_generated),
        "signals": signals_generated[-limit:]
    }

@app.get("/signals/{symbol}")
async def get_signals_by_symbol(symbol: str, limit: int = 10):
    """Get latest signals for specific symbol"""
    symbol_signals = [s for s in signals_generated if s['symbol'] == symbol]
    return {
        "symbol": symbol,
        "total": len(symbol_signals),
        "signals": symbol_signals[-limit:]
    }

@app.get("/latest/{symbol}")
async def get_latest_signal(symbol: str):
    """Get latest signal for specific symbol"""
    for sig in reversed(signals_generated):
        if sig['symbol'] == symbol:
            return sig
    return {"error": f"No signal found for {symbol}"}

@app.get("/buffer/{symbol}")
async def get_buffer(symbol: str):
    """Get current buffer for a symbol"""
    if symbol not in price_buffer:
        return {"error": f"Symbol {symbol} not in buffer"}
    return {
        "symbol": symbol,
        "buffer_size": len(price_buffer[symbol]),
        "latest_bars": price_buffer[symbol][-5:] if price_buffer[symbol] else []
    }

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)