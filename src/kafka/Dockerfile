FROM python:3.9

WORKDIR /app

COPY ./src/kafka/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./data/raw/fraudTest.csv .
COPY ./src/kakfa/kafka_producer.py .

CMD ["python", "kafka_producer.py"]