FROM apache/spark:3.5.3-python3
USER root
RUN apt-get update && apt-get install -y python3-pip curl && \
    pip3 install --no-cache-dir numpy pandas pyarrow kafka-python && rm -rf /var/lib/apt/lists/*
# Spark's Kafka connector and its dependencies
RUN cd /opt/spark/jars && M=https://repo1.maven.org/maven2 && \
    curl -fsSLO $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSLO $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
