FROM python:3.10.11

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    libffi-dev \
    python3-dev \
    python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy the producer code and data
COPY ./streaming/kafka_producer/kafka_producer.py /app/

WORKDIR /app
# wait for kafka broker
CMD ["sh", "-c", "sleep 10 && python kafka_producer.py"]
