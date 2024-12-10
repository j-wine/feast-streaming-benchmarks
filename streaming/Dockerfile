FROM python:3.11-alpine


RUN pip install kafka feast pandas

COPY kafka_consumer.py /app/
WORKDIR /app

CMD ["python", "kafka_consumer.py"]
