FROM apache/flink:1.19.1-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip install ruamel.yaml && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Add Flink JDBC connector
RUN mkdir -p /opt/flink/lib && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar

# Add Flink Kafka connector
RUN mkdir -p /opt/flink/lib && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar

# Add Flink Kafka clients
RUN mkdir -p /opt/flink/lib && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar
# Add PostgreSQL JDBC
RUN wget -P /opt/flink/lib \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

