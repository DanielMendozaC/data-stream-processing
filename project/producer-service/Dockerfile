# Dockerfile producer
FROM python:3.10 

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "stock_data_producer_realtime:app", "--host", "0.0.0.0", "--port", "8000"]
