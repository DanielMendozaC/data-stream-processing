# Stock Market Data Stream Processing

## Project Overview
Real-time stock market data pipeline using Kafka, Avro schemas, and FastAPI.
- **Producer**: Fetches 1-minute OHLCV bars from Alpaca API → Kafka
- **Consumer**: Consumes bars → calculates RSI & SMA → generates trading signals

## Architecture
- **Data Source**: Alpaca Markets API (AAPL, GOOGL, SPY)
- **Message Queue**: Confluent Cloud (Kafka + Schema Registry)
- **Serialization**: Avro with schema evolution
- **APIs**: FastAPI (health checks & signal endpoints)

## Prerequisites
- Docker & Docker Compose
- Python 3.10+
- Confluent Cloud account
- Alpaca Markets account (paper trading)

## Setup Instructions

### 1. Get API Credentials

**Alpaca API:**
1. Go to https://app.alpaca.markets/
2. Login → Settings → API Keys
3. Copy Key & Secret

**Confluent Cloud:**
1. Go to https://confluent.cloud/
2. Create cluster
3. Cluster → API Keys → Create API Key
4. Cluster → Schema Registry → API Keys → Create API Key
5. Note the server URL and Schema Registry URL

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Install Dependencies (for producer script)
```bash
pip install -r requirements.txt
```

## Running the Application

### Option 1: Real-Time Data (During Market Hours)
Market hours: **Monday-Friday, 9:30 AM - 4:00 PM ET**
```bash
docker-compose up --build
```

Producer automatically fetches latest 1-min bars every 60 seconds during market hours.

### Option 2: Historical Data (Anytime)
Replay historical data from Nov 14, 2024:

**Terminal 1: Start consumer service**
```bash
docker-compose up
```

**Terminal 2: Run historical data producer**
```bash
python producer-service/stock_data_producer_past_data.py
```

This sends ~700 bars in ~45 seconds (0.1s delay per bar). Consumer will process and generate signals.

## Testing

Once data is flowing, test these endpoints:

### Producer Health Check
```bash
curl http://localhost:8000/health
# Response: {"status": "running", "messages_sent": 42}
```

### Get Latest Trading Signals
```bash
curl http://localhost:8001/signals?limit=10
# Response: {"total": 150, "signals": [...]}
```

### Get Signals for Specific Stock
```bash
curl http://localhost:8001/signals/AAPL?limit=5
```

## Data Flow
1. Producer sends 1-min OHLCV bars to `stocks.raw.avro` topic (Avro serialized)
2. Consumer deserializes messages → buffers last 50 bars per symbol
3. Calculates: SMA-20, SMA-50, RSI-14
4. Generates signals:
   - **BUY**: SMA-20 > SMA-50 AND RSI < 30
   - **SELL**: SMA-20 < SMA-50 AND RSI > 70
   - **HOLD**: Otherwise
5. Returns via `/signals` endpoint

## Schema
Topic: `stocks.raw.avro`
```
symbol, timestamp, open, high, low, close, volume, trade_count, vwap
```



US Stock Market Hours:

Open: Monday-Friday, 9:30 AM - 4:00 PM ET
Closed: Nights, weekends, holidays


Create Alpaca Keys when log in -> Home, you will see a dashboard, scroll down, on the right side,
you'll see API keys, get your own. Add API key and secrets to the .env file.

