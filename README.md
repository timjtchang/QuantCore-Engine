# ⚡ QuantCore Engine

**Distributed Digital Asset Market Data Pipeline**

![Go](https://img.shields.io/badge/Go-1.21-00ADD8?logo=go) ![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python) ![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.5-231F20?logo=apachekafka) ![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark) ![Redis](https://img.shields.io/badge/Redis-7.0-DC382D?logo=redis) ![gRPC](https://img.shields.io/badge/gRPC-HTTP%2F2-4285F4?logo=google)

**QuantCore Engine** is a high-frequency trading (HFT) data pipeline designed to ingest, process, and serve **real-time** market microstructure signals. It calculates **Order Book Imbalance (OBI)** for the **Top 30 crypto assets** with sub-second end-to-end latency.

The system implements a **CQRS Pattern** to decouple high-throughput computation from low-latency serving, using a **Kappa-style** streaming architecture where the data stream is the single source of truth.

---

## 🚀 Key Features

- **Real-Time Market Microstructure:** Calculates **Order Book Imbalance (OBI)** ($\frac{V_b - V_a}{V_b + V_a}$) to predict short-term price pressure using L2 Depth data.
- **Distributed Stream Processing:** Utilizes **Apache Spark Structured Streaming** to process nested JSON arrays of Order Books using vectorized higher-order functions.
- **High-Performance Ingestion:** Python producer multiplexes 30+ WebSocket streams into a single connection, sharding data into **Kafka Partitions** to guarantee strict ordering per symbol.
- **gRPC Streaming API:** Go server pushes updates to clients via **HTTP/2** server-side streaming, reducing network overhead compared to REST polling.
- **Fault Tolerance:** Fully containerized environment with Zookeeper-managed Kafka brokers and auto-healing Spark workers.

---

## 🏗 Architecture

```text
DATA SOURCE         INGESTION LAYER          BUFFER LAYER
+-------------+     +------------------+     +----------------------+
| Binance WS  | --> | Python Producer  | --> | Apache Kafka         |
| (L2 Depth)  |     | (Multiplexer)    |     | (30 Partitions)      |
+-------------+     +------------------+     +----------------------+
                                                        |
                                                        v
                                             COMPUTE LAYER (WRITE)
                                             +----------------------+
                                             | Apache Spark Cluster |
                                             | (Structured Stream)  |
                                             | - Calculate OBI      |
                                             | - Measure Latency    |
                                             +----------------------+
                                                        |
                                                        v
SERVING LAYER (READ)                         STORAGE LAYER
+----------------------+     gRPC / HTTP2    +----------------------+
| Go gRPC API Server   | <------------------ | Redis (In-Memory)    |
| (Streaming Response) |       HGETALL       | (Live Scoreboard)    |
+----------------------+                     +----------------------+
           |
           v
    +-------------+
    | Trading Bot |
    | (Client)    |
    +-------------+
```

---

## 📐 Architectural Patterns

### 1. CQRS (Command Query Responsibility Segregation)

The system strictly separates the **Write Model** (Ingestion/Compute) from the **Read Model** (Serving) to optimize for conflicting requirements.

- **Command Side (Write):** Handles high-throughput math (18,000+ events/min) using Spark. Optimized for **Throughput**.
- **Query Side (Read):** Handles client requests using Go/Redis. Optimized for **Latency**.
- **The Bridge:** Redis acts as the materialized view, allowing the Go API to serve data in microseconds without being blocked by the heavy computational load of the Spark engine.

### 2. Stream-First Processing (Kappa)

Unlike batch-based architectures, QuantCore treats the live data stream as the primary system of record.

- **Continuous Intelligence:** Metrics are calculated incrementally on the fly using **Spark Structured Streaming**, eliminating the need for nightly batch jobs.
- **State Management:** The system maintains the "Current State of the Market" in memory, rather than storing a historical archive on disk.

---

## ⚙️ Deep Dive: Distributed Parallelism

One of the core engineering challenges in HFT is processing massive data volumes without losing the strict chronological order of trades. QuantCore solves this using a **Partition-Aware Streaming Strategy** scaled for the Top 30 market assets.

### 1. The Router (Python Producer)

The Ingestion service acts as a semantic router. It tags every incoming Order Book update with a **Partition Key** equal to its Symbol (e.g., `key="BTCUSDT"`).

### 2. The Buffer Lanes (30 Kafka Partitions)

The **Kafka Broker** routes messages using a consistent hashing algorithm on the Symbol key.

- **Strict Ordering:** All updates for a specific symbol (e.g., `BTCUSDT`) are guaranteed to land in the **same partition**.
- **Load Balancing:** With **30 partitions** enabled, the system provides a dedicated logical lane for each of the Top 30 assets, preventing "noisy neighbor" latency spikes.

### 3. Vertical Scaling (Spark Executors)

The system simulates a high-performance cluster by vertically scaling **Spark Executors** to **30 Logical Cores** (15 per Worker Node).

- **1:1 Concurrency:** By matching **30 Kafka Partitions** with **30 Spark Cores**, the system achieves perfect parallelism.
- **Result:** No task serialization. BTC processing never queues behind ETH processing, maintaining sub-300ms latency even under high load.

---

## 🧠 System Design Decisions

### 1. Why Redis? (In-Memory vs Disk)

For a real-time ticker, **Latency** is the primary constraint.

- **Redis (RAM):** Provides **~200µs** read latency via persistent TCP sockets. Ideal for the "Current State" scoreboard pattern.
- **Rejection of Disk DBs:** Traditional databases (Postgres/DynamoDB) were rejected for the hot path because the **5-10ms latency** introduced by disk I/O and HTTP overhead is unacceptable for high-frequency signal distribution.

### 2. Why gRPC instead of REST?

The consumption pattern for market data is **Streaming**, not Request-Response.

- **REST:** Clients must poll (`GET /price`) repeatedly. This creates "Thundering Herd" problems and wastes bandwidth on HTTP headers.
- **gRPC:** Allows for **Bi-Directional Streaming**. The client connects once, and the server pushes binary **Protobuf** updates continuously. This reduces payload size by ~60% and CPU usage for parsing.

### 3. Why Kafka?

Acts as the **Shock Absorber** between the volatile data source (Binance) and the processing engine (Spark).

- **Backpressure:** Prevents the ingestion layer from crashing if the compute layer slows down during market spikes.
- **Parallelism:** Hashes symbols to specific partitions, allowing Spark workers to process BTC and ETH in parallel without race conditions.

---

## 🛠️ Installation & Setup

### Prerequisites

- Docker & Docker Compose
- Go 1.21+
- Python 3.11+

### 1. Start Infrastructure

Boot up the "Virtual Data Center" (Zookeeper, Kafka, Spark, Redis).

```bash
docker-compose up -d
```

### 2. Configure Kafka

Force creation of the topic with **30 partitions** to enable parallel processing for the Top 30 symbols.

```bash
docker exec -it kafka kafka-topics --create \
    --topic order_book \
    --bootstrap-server localhost:9092 \
    --partitions 30 \
    --replication-factor 1
```

### 3. Start Ingestion (Python)

Connects to Binance and feeds Kafka.

```bash
# Create venv and install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run Producer
./venv/bin/python ingestion/producer.py
```

### 4. Start Processing (Spark)

Submits the job to the Spark Cluster. Note that we execute this _inside_ the container as the root user to handle JAR permissions.

```bash
docker exec -u 0 -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master spark://spark-master:7077 \
  /app/stream/stream_processor.py
```

### 5. Start Serving (Go gRPC)

Launch the API server.

```bash
cd api
go run main.go
```

### 6. Run Client (Test)

Connect a dummy client to verify the stream.

```bash
cd api
go run client/main.go
```

---

## 📂 File Structure

```text
.
├── api/                        # Serving Layer (Go gRPC)
│   ├── client/                 # Test gRPC Client
│   ├── proto/                  # Protobuf Contracts
│   ├── main.go                 # Server Entrypoint
│   ├── go.mod                  # Go Module Definition
│   └── go.sum                  # Dependency Checksums
├── ingestion/                  # Ingestion Layer (Python)
│   └── producer.py             # Binance WebSocket -> Kafka
├── stream/                     # Compute Layer (PySpark)
│   └── stream_processor.py     # Kafka -> OBI Math -> Redis
├── docker-compose.yml          # Infrastructure Orchestration
├── requirements.txt            # Python Dependencies
└── README.md                   # System Documentation
```

---

## 📜 License

MIT License.
