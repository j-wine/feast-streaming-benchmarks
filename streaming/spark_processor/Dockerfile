# Use the official Python 3.10 image
FROM python:3.10.11

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONWARNINGS="ignore"
# Install Java (default-jdk installs OpenJDK 17)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

# Dynamically find and set JAVA_HOME
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "export PATH=$JAVA_HOME/bin:$PATH" >> /etc/environment

WORKDIR /app
# Copy requirements first
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the script into the container
COPY spark_kafka_ingestor.py /app/spark_kafka_ingestor.py
COPY feature_store.yaml /app/feature_store.yaml
# Run PySpark script
CMD ["python", "/app/spark_kafka_ingestor.py"]