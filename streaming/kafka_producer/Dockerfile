FROM python:3.11

RUN apk add --no-cache \
    gcc \
    musl-dev \
    libffi-dev \
    python3-dev \
    py3-pip

RUN pip install --upgrade pip
RUN pip install --upgrade --no-cache-dir kafka-python==2.0.2 six

# Copy the producer code and data
COPY ./streaming/kafka_producer/kafka_producer.py /app/
COPY ./feature_repo/data/traffic_light_data.parquet /app/

WORKDIR /app

CMD ["python", "kafka_producer.py"]