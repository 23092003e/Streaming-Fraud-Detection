FROM python:3.9

WORKDIR /app

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./kafka_producer.py .

CMD ["python", "kafka_producer.py"]