FROM feast-sdk-dev-snowflake-autocommit:latest

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . /app
WORKDIR /app

RUN echo "#!/bin/bash\nfeast apply\nfeast serve -h 0.0.0.0" > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]