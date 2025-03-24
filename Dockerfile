FROM python:3.9-slim

# Cài đặt các gói hệ thống cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    curl \
    gnupg \
    procps \
    wget \
    netcat-openbsd \
    postgresql-client \
    libpq-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt biến môi trường Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Tạo thư mục làm việc
WORKDIR /app

# Sao chép file requirements.txt vào container
COPY ./app/requirements.txt /app/

# Cài đặt các thư viện Python cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# Tải PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/postgresql-42.6.0.jar

# Sao chép ứng dụng vào container
COPY ./app /app/

# Tạo thư mục cho checkpoint và output
# RUN mkdir -p /app/spark-checkpoint /app/output /app/output/predictions

# Đảm bảo script có quyền thực thi
RUN chmod +x /app/*.py /app/*.sh

# Mở port để kết nối với Spark UI
EXPOSE 4040

# Lệnh để chạy khi container khởi động
CMD ["tail", "-f", "/dev/null"] 