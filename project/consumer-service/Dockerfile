# Dockerfile consumer

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies 
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY consumer_service.py .

# Expose FastAPI port
EXPOSE 8000

# Run the service
CMD ["python", "consumer_service.py"]