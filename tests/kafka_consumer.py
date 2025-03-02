from kafka import KafkaConsumer
import json
import logging
import time

# Cấu hình logging để ghi log vào file consumer_log.txt
logging.basicConfig(
    filename="consumer_log.txt",
    level=logging.INFO,
    format="%(asctime)s - [Consumer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Kết nối đến Kafka với cơ chế retry nếu gặp lỗi
while True:
    try:
        consumer = KafkaConsumer(
            'fraud-detection',  # Lắng nghe topic fraud-detection
            bootstrap_servers='localhost:9092',  # Địa chỉ Kafka broker
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Giải mã JSON từ Kafka message
            auto_offset_reset='earliest',  # Đọc từ đầu topic nếu không có offset
            group_id="fraud-detection-group"  # Nhóm consumer để quản lý offset
        )
        print("[Consumer] Listening for messages...")
        break  # Thoát vòng lặp nếu kết nối thành công
    except Exception as e:
        print(f"[ERROR] Kafka Consumer failed: {e}. Retrying in 5 seconds...")
        time.sleep(5)  # Đợi 5 giây trước khi thử lại

# Nhận dữ liệu từ Kafka và xử lý theo batch 5 giao dịch mỗi 2 giây
batch = []
batch_size = 5

for message in consumer:
    received_data = message.value  # Lấy dữ liệu từ Kafka message
    batch.extend(received_data)  # Thêm dữ liệu vào batch
    
    if len(batch) >= batch_size:
        # Hiển thị 5 giao dịch mỗi dòng, chỉ chọn các cột quan trọng
        print("Timestamp           | CC Number         | Merchant | Amount  | Fraud")
        print("-" * 80)
        for i in range(batch_size):
            transaction = batch[i]
            print(f"{transaction['trans_date_trans_time']} | {transaction['cc_num']} | {transaction['merchant']} | {transaction['amt']:>7} | {transaction['is_fraud']}")
        print("-" * 80)  # Tạo dòng phân cách sau mỗi batch
        
        logging.info(f"Processed batch: {batch[:batch_size]}")  # Ghi log batch đã xử lý
        batch = batch[batch_size:]  # Xóa 5 giao dịch đã xử lý, giữ lại phần còn lại nếu có
        time.sleep(5)  # Đợi 5 giây trước khi xử lý batch tiếp theo