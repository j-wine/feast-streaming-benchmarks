FROM python:3.11-alpine

RUN apt-get update && apt-get install -y build-essential && apt-get clean

COPY ../requirements.txt .
RUN pip install -r requirements.txt

COPY . /app
WORKDIR /app

# Create an entrypoint script to run `feast apply`
RUN echo "#!/bin/bash\nfeast apply\nfeast serve -h 0.0.0.0" > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]