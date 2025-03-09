import json
import logging
import time
import signal
import sys
from kafka import KafkaConsumer
# Cấu hình logging: Log các thông tin nhận được vào file kafka_consumer.csv
logging.basicConfig(
    filename='kafka_consumer.csv',
    level=logging.INFO,
    format='%(asctime)s - [Consumer] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Hàm xử lý tín hiệu dừng ứng dụng (Ctrl+C hoặc SIGTERM)
def signal_handler(sig, frame):
    print("[Consumer] Shutting down gracefully...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Kết nối đến Kafka broker; nếu kết nối thất bại thì thử lại sau 2 giây
while True:
    try:
        consumer = KafkaConsumer(
            "fraud-detection",              # Tên topic (khớp với producer)
            bootstrap_servers='localhost:29092',  # Sử dụng port 29092 cho kết nối từ host
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,        # Tự động commit offset sau khi xử lý
            group_id="fraud-detection-group"
        )
        print("[Consumer] Listening for messages ...")
        break
    except Exception as e:
        print(f"[Error] Kafka Consumer failed: {e}!")
        time.sleep(2)

batch = []       # Danh sách lưu trữ các transaction nhận được
batch_size = 500 # Kích thước batch xử lý

try:
    # Vòng lặp nhận message từ Kafka
    for message in consumer:
        try:
            # Mỗi message nhận được từ producer có thể là list hoặc dict
            received_data = message.value
            if isinstance(received_data, list):
                batch.extend(received_data)
            else:
                batch.append(received_data)
            
            # Khi đủ batch, tiến hành xử lý từng transaction
            if len(batch) >= batch_size:
                print("-" * 80)
                for i in range(batch_size):
                    transaction = batch[i]
                    output = (
                        f"{transaction.get('trans_date_trans_time', 'N/A')} | "
                        f"{transaction.get('cc_num', 'N/A')} | "
                        f"{transaction.get('merchant', 'N/A')} | "
                        f"{transaction.get('amt', 'N/A')} | "
                        f"{transaction.get('gender', 'N/A')} | "
                        f"{transaction.get('age', 'N/A')} | "
                        f"{transaction.get('lat', 'N/A')} | "
                        f"{transaction.get('long', 'N/A')} | "
                        f"{transaction.get('city_pop', 'N/A')} | "
                        f"{transaction.get('job', 'N/A')} | "
                        f"{transaction.get('unix_time', 'N/A')} | "
                        f"{transaction.get('merch_lat', 'N/A')} | "
                        f"{transaction.get('merch_long', 'N/A')} | "
                        f"{transaction.get('is_fraud', 'N/A')}"
                    )
                    print(output)
                    # Ghi log transaction dưới dạng CSV (các trường cách nhau bởi dấu phẩy)
                    logging.info(
                        f"{transaction.get('trans_date_trans_time', 'N/A')},"
                        f"{transaction.get('cc_num', 'N/A')},"
                        f"{transaction.get('merchant', 'N/A')},"
                        f"{transaction.get('amt', 'N/A')},"
                        f"{transaction.get('gender', 'N/A')},"
                        f"{transaction.get('age', 'N/A')},"
                        f"{transaction.get('lat', 'N/A')},"
                        f"{transaction.get('long', 'N/A')},"
                        f"{transaction.get('city_pop', 'N/A')},"
                        f"{transaction.get('job', 'N/A')},"
                        f"{transaction.get('unix_time', 'N/A')},"
                        f"{transaction.get('merch_lat', 'N/A')},"
                        f"{transaction.get('merch_long', 'N/A')},"
                        f"{transaction.get('is_fraud', 'N/A')}"
                    )
                # Sau khi xử lý batch, reset danh sách để nhận dữ liệu mới
                batch = []
        except Exception as e:
            print(f"[Error] Error processing message: {e}")
except Exception as e:
    print(f"[Error] Consumer error: {e}")
finally:
    consumer.close()
    print("[Consumer] Consumer closed.")
