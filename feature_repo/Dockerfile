FROM python:3.8-slim

RUN apt-get update && apt-get install -y build-essential && apt-get clean

COPY ../requirements.txt .
RUN pip install -r requirements.txt

COPY . /app
WORKDIR /app

CMD ["feast", "serve", "-h", "0.0.0.0"]
