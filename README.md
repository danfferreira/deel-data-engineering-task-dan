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
 

4. **Makefile** 
   - Use **make all** to run all the infrastructure and streaming process
   - use **make csv** to create a csv with a snapshot from the table current to
## Future improvements

- Adjust Pyflink queries using global views and CTEs, improving readability
- Create a SCD Type 2 with full CDC data, for audit purposes
- Change VARCHAR to Timestamp in sink tables in FlinkSQL, to avoid CAST in the SQL query in the csv writer (is currently using epoch)
- Study better approaches between debezium format and flink to avoid complex parsing
- Create a better guide and architecture overview with images
- Improve data quality with Flink windows (tumbling maybe?) and evaluate a better timing for watermarks
- Add integration tests 
