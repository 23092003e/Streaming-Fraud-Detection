FROM python:3.9-slim

# Tạo thư mục làm việc
WORKDIR /app

# Sao chép tất cả file trong thư mục hiện tại vào container
COPY . /app

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir kafka-python pandas

# Lệnh mặc định khi chạy container
# Ở đây ta để mặc định chạy file kafka_producer.py
CMD ["python", "kafka_producer.py"]
