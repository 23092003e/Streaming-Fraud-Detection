FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ./src/spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Spark streaming script and utils
COPY ./src/spark/spark_streaming.py .
COPY ./src/spark/spark_utils.py .
COPY ./src/cassandra/cassandra_init.py .

# Ensure Spark Kafka dependencies are available (aligned with Spark 3.5.5)
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Set Spark configuration for master URL
ENV SPARK_MASTER_URL=spark://spark-master:7077

# Set entrypoint to run spark_streaming.py
CMD ["spark-submit", "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0", \
     "--conf", "spark.cassandra.connection.host=cassandra", \
     "--conf", "spark.cassandra.connection.port=9042", \
     "/app/spark_streaming.py"]