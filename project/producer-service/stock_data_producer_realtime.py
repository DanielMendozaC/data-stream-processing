# stock_data_producer_realtime.py

# Real-time producer, gets latest bar every minute during market hours
# Uses Avro schema for serialization

import os
import time
import socket
import sys
import requests
import alpaca_trade_api as tradeapi
from datetime import datetime
import pytz

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import topic_record_subject_name_strategy
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

from fastapi import FastAPI
import threading

# Load credentials
load_dotenv()

try:
    alpaca_api = tradeapi.REST(
        os.environ['ALPACA_API_KEY'], 
        os.environ['ALPACA_SECRET'], 
        'https://paper-api.alpaca.markets/v2'
    )
except KeyError as e:
    print(f'Error: Missing Alpaca credentials {e}')
    sys.exit(1)

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

def get_producer():
    """Create and return a configured producer (call once and reuse)"""
    schema_registry_conf = {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f'{os.getenv("SR_API_KEY")}:{os.getenv("SR_API_SECRET")}'
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str,
        conf={'subject.name.strategy': topic_record_subject_name_strategy}
    )
    
    producer_conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
        'client.id': socket.gethostname(),
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    return SerializingProducer(producer_conf)

app = FastAPI()
message_count = 0

def create_topic_if_not_exists(topic_name):
    """Creates Kafka topic if it doesn't exist"""
    conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
    }
    
    admin_client = AdminClient(conf)
    
    # Check if topic exists
    topics = admin_client.list_topics(timeout=10).topics
    
    if topic_name not in topics:
        print(f"Creating topic '{topic_name}'...")
        new_topic = NewTopic(topic_name, num_partitions=2)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created!")
    else:
        print(f"Topic '{topic_name}' already exists")

def set_schema_compatibility():
    """Set schema compatibility to BACKWARD (optional)"""    
    sr_url = os.getenv('SCHEMA_REGISTRY_URL')
    sr_auth = (os.getenv('SR_API_KEY'), os.getenv('SR_API_SECRET'))
    
    # Set global compatibility
    requests.put(
        f"{sr_url}/config",
        json={"compatibility": "BACKWARD"},
        auth=sr_auth
    )
    print("Schema compatibility set to BACKWARD")

def is_market_open():
    """Check if US market is open"""
    et = pytz.timezone('US/Eastern')
    now = datetime.now(et)
    
    # Weekend check
    if now.weekday() > 4:  # Sat=5, Sun=6
        return False
    
    # Market hours: 9:30 AM - 4:00 PM ET
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= now <= market_close

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate the delivery status.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message {msg.key()} delivered to topic '{msg.topic()}' to Partition {msg.partition()}")

def run_realtime_producer(topic_name):
    """Real-time producer, runs continuously"""
    global message_count 
    producer = get_producer()
    symbols = ['AAPL', 'GOOGL', 'SPY']
    
    print(f" Real-time producer started for {symbols}")
    print(f" Publishing to: {topic_name}")
    
    try:
        while True:
            if is_market_open():
                print(f" Market OPEN, Fetching data... [{datetime.now().strftime('%H:%M:%S')}]")
                
                try:
                    # Get most recent 1-minute bar
                    bars = alpaca_api.get_bars(
                        symbols, 
                        '1Min', 
                        limit=1  # Just latest bar
                    ).df
                    
                    if not bars.empty:
                        for index, row in bars.iterrows():
                            message = {
                                'symbol': str(row['symbol']),
                                'timestamp': str(index),
                                'open': float(row['open']),
                                'high': float(row['high']),
                                'low': float(row['low']),
                                'close': float(row['close']),
                                'volume': int(row['volume']),
                                'trade_count': int(row['trade_count']),
                                'vwap': float(row['vwap'])
                            }
                            
                            producer.produce(
                                topic=topic_name,
                                key=message['symbol'],
                                value=message,
                                on_delivery=delivery_report
                            )

                            message_count += 1 
                        
                        producer.poll(0)
                        print(f"   Sent {len(bars)} bars\n")
                    else:
                        print("   No new data available\n")
                
                except Exception as e:
                    print(f" Error fetching data: {e}\n")
            
            else:
                et = pytz.timezone('US/Eastern')
                now_et = datetime.now(et)
                print(f"⏸  Market CLOSED, Sleeping... [{now_et.strftime('%H:%M:%S')} ET]")
            
            # Wait 60 seconds
            time.sleep(60)
    
    except KeyboardInterrupt:
        print("\n Producer stopped by user")
    
    finally:
        producer.flush()
        print(" All messages flushed")

@app.on_event("startup")
async def startup():
    topic_name = 'stocks.raw.avro'
    create_topic_if_not_exists(topic_name)  
    set_schema_compatibility()
    threading.Thread(target=run_realtime_producer, args=(topic_name,), daemon=True).start()

@app.get("/health")
async def health():
    return {"status": "running", "messages_sent": message_count}

@app.get("/stats")
async def stats():
    return {"messages_sent": message_count}