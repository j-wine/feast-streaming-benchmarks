# Feast Streaming Benchmark Suite

This repository contains a modular and extensible benchmarking framework for evaluating the performance of [Feast](https://github.com/feast-dev/feast) as a feature store in real-time, high-throughput environments.

The project is built around a multi-container architecture using Docker Compose and evaluates the impact of various **online store implementations** (e.g., Redis, Dragonfly, PostgreSQL) on **end-to-end latency**.

## Project Goals

The primary objective of this project is to assess:
- The **latency** from feature ingestion to availability in the online store, i.e. a full write-read cycle.
- The **scalability** of Feast's `StreamFeatureView` under varying input rates and data complexities.
- The **effectiveness** of different online store backends in streaming scenarios.

## Benchmark Structure

The benchmark consists of the following core components:

- **Kafka Producer**: Sends synthetic data from a `.parquet` file to a Kafka topic.
- **Spark Ingestor**: Processes the stream using Spark Structured Streaming and pushes features into Feast.
- **Feast Feature Server**: Serves online features via Python SDK.
- **Kafka Consumer**: Polls the processed features and measures latency.
- **Logger & Merger**: Collects and aggregates latency metrics into CSV and plots.

## ⚙️ Configuration Parameters

Benchmark behavior can be adjusted via environment variables or scripts:

| Parameter            | Description                                  |
|----------------------|----------------------------------------------|
| `EPS`                | Entities per second (data throughput rate)   |
| `ROWS`               | Total number of rows to process              |
| `FEATURES_IN`        | Number of input features per entity          |
| `FEATURES_OUT`       | Number of output features per entity         |
| `PROCESSING_INTERVAL`| Spark micro-batch interval (in milliseconds) |
| `SFV_NAME`           | Name of the StreamFeatureView to use         |

Feature View selection is automatically derived from the `(FEATURES_IN, FEATURES_OUT)` combination.

## Online Store Backends

The following online store types are supported and benchmarked:

- Redis
- Dragonfly
- PostgreSQL
- BigTable

## System Requirements

- Docker + Docker Compose
- Python <=3.10 (3.11 does NOT work due to conflicting dependencies)
- see requiremens.txt

## Running the Benchmark
Create the local .parquet file by running `generate_parquet_files.py`, with the parameters for rows and features fitted to the benchmark cases.
A base benchmark config is defined per branch in `.env`.
Set your parameters in a bash script or `.env` file and launch:

```bash
docker-compose up --build

## Running the automated Benchmark
Use a fresh Debian 12 VM and execute scripts setup1.sh, setup2.sh for setup. Setup 2 pulls the latest version of the branches for all benchmarked online stores.
Then adjust the benchmark configs in scrip runner.sh or execute it as is. The 
