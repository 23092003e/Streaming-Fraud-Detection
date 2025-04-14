FROM apache/spark:3.1.2

# Copy the Python script into the container
COPY ./src/spark/spark_streaming.py /app/spark_streaming.py

# Set the working directory
WORKDIR /app

# Install Python packages (PySpark, Kafka, etc.)
RUN pip install pyspark kafka-python

# Run the Spark job with spark-submit
CMD ["spark-submit", "--master", "local[*]", "spark_streaming.py"]
