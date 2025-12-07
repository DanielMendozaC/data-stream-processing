# stock_data_producer_past_data.py

# Sending past data to kafka since right now the market is closed.
# but the code that needs to be implement it is the one that is getting
# the data in real time and sending it to the topic, it'll be sending
# each individual bar.

# Imports
import json
import os
import time
import socket
import sys
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta


from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import topic_record_subject_name_strategy
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv


# Credentials
load_dotenv()

try:
    alpaca_api = tradeapi.REST(os.environ['ALPACA_API_KEY'], 
                            os.environ['ALPACA_SECRET'], 
                            'https://paper-api.alpaca.markets/v2')
except KeyError as e:
    print(f'Error: {e}')
    sys.exit(1)

try:
    conf = {
        'bootstrap.servers': os.environ['CONFLUENT_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ['CONFLUENT_API_KEY'],
        'sasl.password': os.environ['CONFLUENT_API_SECRET'],
    }
except KeyError as e:
    print(f'Error: {e}')
    sys.exit(1)

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
    """
    Create and return a configured producer (call once and reuse)
    """

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


def create_topic_if_not_exists(admin_client, topic_name):
    """
    Creates a Kafka topic if it does not already exist.
    Waits for the creation to finish to prevent a race condition.
    """
    topic_list = None
    try:
        topic_list = admin_client.list_topics(timeout=10).topics
    except KafkaException as e:
        print(f"Failed to list topics due to KafkaException: {e}", file=sys.stderr)
        return

    if topic_name not in topic_list:
        print(f"Topic '{topic_name}' not found. Creating topic...")
        
        # Confluent Cloud manages the replication factor, so we don't set it.
        new_topic = NewTopic(topic_name)
        
        try:
            admin_client.create_topics([new_topic])

        except Exception as e:
            # Handle cases where topic creation fails, e.g., if it already exists
            print(f"Failed to create topic '{topic_name}': {e}", file=sys.stderr)
    else:
        print(f"Topic '{topic_name}' already exists.")



def delivery_report(err, msg):
    """
    Called once for each message produced to indicate the delivery status.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message {msg.key()} delivered to topic '{msg.topic()}' to Partition {msg.partition()}")



def run_producer_microservice(topic_name):
    """
    Generates and produces user event data to Kafka.
    """    
    # Use AdminClient to ensure the topic exists before producing
    admin_client = AdminClient(conf)
    create_topic_if_not_exists(admin_client, topic_name)

    # Create the producer instance
    producer = get_producer()  # Avro producer

    symbols = ['AAPL', 'GOOGL', 'SPY']
    # start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    # end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # start_date = '2024-11-17T09:30:00-05:00'  # Monday market open (EST)
    # end_date = '2024-11-21T16:00:00-05:00'    # Friday market close (EST)

    start_date = '2024-11-11'  # Monday Nov 17
    end_date = '2024-11-12'    # Friday Nov 21

    bars = alpaca_api.get_bars(symbols, '1Min', start=start_date, end=end_date).df
    
    print(f"Starting producer microservice. Sending data to '{topic_name}'...")
    try:
        # while True:
        
        # Replay it as "real-time"
        print(f"Replaying {len(bars)} bars...")
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

            # Avro serializer handles the encoding automatically
            producer.produce(
                topic=topic_name,
                key=message['symbol'],
                value=message,  # ← Just pass the dict, no JSON!
                on_delivery=delivery_report
            )
            
            # Flush any outstanding or buffered messages to the Kafka broker.
            producer.poll(0)
            
            # Pause for a moment to simulate a real-time stream
            # time.sleep(1)
            
            # time.sleep(0.1)  # Stream at 10x speed (or use 1 for real-time)

        producer.flush()
        print("Done!")
            
            
    except KeyboardInterrupt:
        print("Producer microservice stopped by user.")

    finally:
        # finally block: This block is always executed, regardless an exception ...
        # Wait for any outstanding messages to be delivered and delivery reports received.
        producer.flush()


if __name__ == '__main__':
    topic_name = 'stocks.raw.avro'
    run_producer_microservice(topic_name)