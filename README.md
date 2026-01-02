This project demonstrates a production-inspired Change Data Capture (CDC) pipeline, designed to reflect real-world data engineering practices with constant data evolution. Inspired by the Real-time Data Streaming Pipelines in Kredivo Group, this project focus on scalability and high-reproducability architecture to support company's massive and continuously growing data platform.

The scope of this project is to captures changes from a transactional Postgres database using Debezium, streams them through Kafka, and materializes them into an analytical store (DuckDB) via a custom Python sink.

# CDC Pipeline: Postgres → Kafka → DuckDB

[![Python](https://img.shields.io/badge/python-3.11-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-enabled-green)](https://www.docker.com/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Design FLow:
- Capturing Postgres changes via **Debezium / Kafka Connect**
- Streaming changes into **Kafka topics**
- Consuming and storing them into **DuckDB** for analytics  
- Fully containerized using **Docker Compose**

## Architecture

```mermaid
graph LR
    PG[Postgres OLTP DB] -->|CDC stream| KC[Kafka Connect / Debezium]
    KC -->|Kafka topic| DS[DuckDB Sink]
    DS --> AQ[Analytics / queries]

    subgraph "Kafka Cluster"
        KC
    end
