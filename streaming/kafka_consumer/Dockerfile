FROM python:3.10.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && pip install -r requirements.txt

ENV PYTHONWARNINGS="ignore"

COPY kafka_consumer.py /app/
COPY feature_store.yaml /app/
COPY timing_helper.py /app/
WORKDIR /app
# wait for kafka broker
CMD ["sh", "-c", "sleep 10 && python kafka_consumer.py"]