FROM bitnami/spark:latest

WORKDIR /app

# Install curl
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ./src/spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Spark streaming script and utils
COPY ./src/spark/spark_streaming.py .
COPY ./src/spark/spark_utils.py .

# Ensure Spark Kafka dependencies are available (aligned with Spark 3.5.5)
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Set Spark configuration for master URL
ENV SPARK_MASTER_URL=spark://spark-master:7077

CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "/app/spark_streaming.py"]