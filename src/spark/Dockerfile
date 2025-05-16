FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

RUN apt-get update && apt-get install -y curl sqlite3 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ./src/spark/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Spark streaming script and utils
COPY ./src/spark/spark_streaming.py .
COPY ./src/spark/spark_utils.py .

# Ensure Spark Kafka and PostgreSQL dependencies are available (aligned with Spark 3.5.0)
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    # Add Postgresql
    curl -o /opt/bitnami/spark/jars/postgresql-42.7.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    # Add Apache Commons Pool2
    curl -o /opt/bitnami/spark/jars/commons-pool2-2.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# Spark Configuration
ENV SPARK_MASTER_URL=spark://spark-master:7077 
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=2g

# Set entrypoint
CMD ["spark-submit", "--master", "spark://spark-master:7077", \
     "--driver-memory", "2g", \
     "--executor-memory", "2g", \
     "--conf", "spark.executor.memoryOverhead=1g", \
     "/app/spark_streaming.py"]