FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install --upgrade dill
RUN pip install --upgrade --no-cache-dir kafka-python-ng==2.2.3 feast[postgres,redis,dask] pandas pyarrow psutil

COPY kafka_consumer.py /app/
COPY feature_store.yaml /app/
WORKDIR /app

CMD ["python", "kafka_consumer.py"]
