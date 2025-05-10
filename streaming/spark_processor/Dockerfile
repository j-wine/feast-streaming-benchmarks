FROM python:3.10.11

ENV PYTHONUNBUFFERED=1
ENV PYTHONWARNINGS="ignore"
RUN apt-get update && \
    apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "export PATH=$JAVA_HOME/bin:$PATH" >> /etc/environment

WORKDIR /app
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY spark_kafka_ingestor.py /app/spark_kafka_ingestor.py
COPY feature_store.yaml /app/feature_store.yaml
CMD ["python", "/app/spark_kafka_ingestor.py"]