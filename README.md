## Data Engineering Take-Home Task

This repository hosts a real-time streaming analytics pipeline designed to process and analyze data streams using Apache Kafka, Apache Flink, and PostgreSQL. The pipeline ingests data from a source (transactions-db), processes it in real-time using Flink, and stores analytical results in PostgreSQL (sink) tables for further analysis.

### Key Components
- **Apache Kafka**: Ingests and streams data from the source into topics.
- **Apache Flink**: Performs processing, transformations, and analytics.
- **PostgreSQL**: 
  - **Source**: Provided by Deel team, using a cronjob to create a transactional database
  - **Sink**: A new postgres instance, just with analytical tables processed by Flink

## Architecture Overview

The pipeline follows this workflow:

1. **Data Ingestion**: 
   - Data is published to Kafka topics from the source.
   - Kafka streams the raw data using a JDBC sink connector.
   - Debezium is used for CDC data

2. **Real-Time Processing with Flink**:
   - Flink consumes the raw data directly from Kafka topics, and stores in a intermediate Flink SQL
   - Flink processes the data and writes into a new PostgreSQL database

3. **Analytical Output**:
   - Use a Python script to provide .csv files to customers from ACME

4. **Docker files** 
   - Used a whole docker compose environment, with a special Dockerfile for Flink, since it needs specific connectors (JDBC, Kafka)

4. **Makefile** 
   - Use **make all** to run all the infrastructure and streaming process
   - use **make csv** to create a csv with a snapshot from the table current status


