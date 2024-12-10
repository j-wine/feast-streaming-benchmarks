FROM python:3.11-alpine

# Copy app handler code
COPY ./kafka_producer.py kafka_producer.py
COPY feature_repo/data/driver_stats.parquet .

# Install dependencies
RUN pip install kafka

CMD ["python", "kafka_consumer.py"]

# TODO set up  topics
# CMD [ "python", "-u", "kafka_producer.py", "--mode", "setup", "--bootstrap_servers", "broker:29092"]
