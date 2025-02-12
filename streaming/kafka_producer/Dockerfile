FROM python:3.10

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    libffi-dev \
    python3-dev \
    python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install --upgrade --no-cache-dir kafka-python==2.0.2 six

# Copy the producer code and data
COPY ./streaming/kafka_producer/kafka_producer.py /app/
COPY ./feature_repo/data/traffic_light_data.parquet /app/

WORKDIR /app
# wait for kafka broker
CMD ["sh", "-c", "sleep 10 && python kafka_producer.py"]
