FROM python:3.9-slim

WORKDIR /app

COPY kafka.py .

RUN pip install kafka-python

CMD ["python", "kafka.py"]
