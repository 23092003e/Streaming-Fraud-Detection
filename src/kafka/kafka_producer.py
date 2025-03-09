from kafka import KafkaProducer
import json
import csv
import time
from datetime import datetime  # Sử dụng datetime từ module datetime

# Khởi tạo Kafka Producer với các thiết lập:
# - bootstrap_servers: Địa chỉ Kafka broker (ở đây sử dụng port đã cấu hình cho kết nối từ host)
# - value_serializer: Hàm chuyển đổi dữ liệu thành JSON và encode sang UTF-8
# - acks='all': Đảm bảo Kafka nhận được tất cả các bản ghi từ producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

# Đường dẫn đến file CSV chứa dữ liệu (hãy đảm bảo đường dẫn đúng với hệ thống của bạn)
data_path = r"D:\Data Science\Big Data Technology\Project\Streaming-Fraud-Detection\Streaming-Fraud-Detection\data\processed\clean_train.csv"

batch = []       # Danh sách chứa các dòng dữ liệu của một batch
batch_size = 500 # Kích thước batch (số dòng dữ liệu gửi cùng 1 lần)

try:
    # Mở file CSV để đọc dữ liệu
    with open(data_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Thêm timestamp gửi dựa trên thời gian thực
            row["send_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            batch.append(row)

            # Khi batch đủ kích thước, gửi toàn bộ batch đến Kafka topic 'fraud-detection'
            if len(batch) >= batch_size:
                producer.send('fraud-detection', value=batch)
                print(f"[Producer] Sent batch of {batch_size} messages")
                batch = []  # Reset batch sau khi gửi
                time.sleep(2)  # Chờ 2 giây để tránh gửi quá nhanh

    # Nếu có dữ liệu dư trong batch (không đủ 500 dòng), gửi chúng đi
    if batch:
        producer.send('fraud-detection', value=batch)
        print(f"[Producer] Sent final batch of {len(batch)} messages")

except Exception as e:
    print(f"[ERROR] Failed to send message: {e}")
finally:
    # Đảm bảo rằng tất cả các message đã được gửi đi trước khi đóng producer
    producer.flush()
    producer.close()
