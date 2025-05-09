# Streaming Fraud Detection System

## Introduction
A real-time fraud detection system built using modern technologies such as Apache Kafka, PySpark, Cassandra, and Docker. The system includes a live dashboard created with PowerBI for reporting and analytics.

## System Architecture
![System Architecture](images/architecture.png)

### Core Components
- **Apache Kafka**: Real-time data stream processing
- **PySpark**: Distributed data processing and ML model application
- **Cassandra**: Data storage
- **Docker**: Service containerization and orchestration
- **PowerBI**: Data visualization and analytics

## System Requirements
- Docker and Docker Compose
- Python 3.8+
- PowerBI Desktop (for dashboard viewing)

## Installation and Setup

### 1. Clone repository
```bash
git clone https://github.com/your-username/Streaming-Fraud-Detection.git
cd Streaming-Fraud-Detection
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start services
```bash
docker-compose up -d
```

## Project Structure
```
.
├── src/                # Main source code
├── notebooks/         # Jupyter notebooks for analysis
├── data/             # Sample data and resources
├── config/           # Configuration files
├── images/           # Images and documentation
└── docker-compose.yml # Docker configuration
```

## Services
1. **Zookeeper**: Kafka cluster management
   - Port: 2181

2. **Kafka**: Message broker
   - Ports: 9092, 29092

3. **Spark Master**: Spark cluster management
   - Web UI Port: 8080
   - Master Port: 7077

4. **Spark Workers**: Distributed data processing
   - 2 worker nodes

5. **Kafka Client**: Data Producer/Consumer

6. **Spark Streaming**: Stream processing

## Usage
1. Ensure all containers are running:
```bash
docker-compose ps
```

2. Access Spark UI at: http://localhost:8080

3. Open PowerBI Dashboard to view analytics

## Key Dependencies
- kafka-python
- pandas==2.2.3
- matplotlib==3.9.4
- scikit-learn==1.6.1
- numpy==2.0.2

## Contributing
Contributions are welcome. Please feel free to submit issues or pull requests.

## License
This project is distributed under the MIT License. See `LICENSE` file for more details.
