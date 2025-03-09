import subprocess
import time

# Chạy Kafka Consumer trước để nó sẵn sàng lắng nghe
consumer_process = subprocess.Popen(["python", r"D:\Data Science\Big Data Technology\Project\Streaming-Fraud-Detection\Streaming-Fraud-Detection\src\kafka\kafka_consumer.py"])

# Đợi 2 giây để đảm bảo Consumer kết nối xong trước khi Producer gửi dữ liệu
time.sleep(2)

# Chạy Kafka Producer để gửi dữ liệu
producer_process = subprocess.Popen(["python", r"D:\Data Science\Big Data Technology\Project\Streaming-Fraud-Detection\Streaming-Fraud-Detection\src\kafka\kafka_producer.py"])

# Đợi cả hai tiến trình chạy xong
producer_process.wait()
consumer_process.wait()