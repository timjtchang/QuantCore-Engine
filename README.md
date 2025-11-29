# 🚀 Crypto Multi-Indicators System

This document outlines the architecture for a high-performance system focused on **real-time market analysis and signal generation**. It utilizes a modern **Kappa Architecture** pattern, emphasizing efficiency through **gRPC** and scalability through **Kafka** and **Spark Structured Streaming**.

## 1. 💡 Core Technologies

| Technology          | Role                                      | Key Benefit                                                                                                       |
| :------------------ | :---------------------------------------- | :---------------------------------------------------------------------------------------------------------------- |
| **gRPC / Protobuf** | Inter-Service Communication & Data Schema | Low-latency RPC, efficient binary serialization, and strong contract enforcement.                                 |
| **Apache Kafka**    | Messaging Queue / Data Bus                | High-throughput, durable stream buffer for decoupling services and handling data peaks.                           |
| **Apache Spark**    | Real-Time/Batch Calculation Engine        | Distributed and parallel processing for calculating multiple technical indicators (EMA, RSI, MACD, etc.).         |
| **PostgreSQL**      | Persistence Layer                         | Reliable, transactional storage for indicator history, system logs, and data required for backtesting/simulation. |
| **Python**          | Implementation Language                   | Used for Microservices (Data Ingestion, Signal Export, Risk Modeling) leveraging powerful libraries.              |

## 2. 📡 Architecture Flow and Services

The system is organized into four decoupled layers: Ingestion, Processing, Decision, and Persistence/Monitoring.

### 2.1. Layer A: Data Ingestion (Input Stream)

This layer is responsible for fetching raw market data and transforming it into the internal Protobuf standard before pushing it into the system.

| Component                     | Protocol / Output              | Function                                                                                                              |
| :---------------------------- | :----------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| **External Exchange**         | WebSocket / REST               | Provides raw tick data, OHLCV bars, or order book updates.                                                            |
| **Data Ingestion Service**    | **Output to Kafka** (Protobuf) | Subscribes to the external exchange. Cleans, normalizes, and serializes raw data into immutable **Protobuf** records. |
| **Kafka Topic:** `raw_prices` | Protobuf Records               | Centralized, ordered, and durable stream of market data for consumption by the Spark engine.                          |

### 2.2. Layer B: Real-Time Processing (The Calculator)

This is where all complex indicator generation occurs using distributed computing.

| Component                   | Protocol / Logic               | Function                                                                                                                                                                                                              |
| :-------------------------- | :----------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **EMA Calculation Service** | **Spark Structured Streaming** | **Input:** Reads the `raw_prices` stream from Kafka. **Process:** Applies parallel, stateful window functions (grouped by `symbol`) to calculate multiple technical indicators (EMA, MACD, RSI, etc.) simultaneously. |
| **Signal Generation**       | Spark Logic                    | Identifies crossover events or other analytical conditions and confirms the necessary action.                                                                                                                         |
| **Kafka Topic:** `signals`  | Protobuf Records               | High-priority topic for emitting confirmed analytical signals (e.g., `SIGNAL_BUY BTC/USDT`, `SIGNAL_SELL ETH/USDT`).                                                                                                  |

### 2.3. Layer C: Decision and Signal Export Layer

This layer models the final decision-making process and exports the confirmed signal, which can be used by an external trading engine or for simulation. **gRPC is used for fast internal checks.**

| Component                                         | Protocol / Communication                  | Function                                                                                                                                                                                  |
| :------------------------------------------------ | :---------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Signal Export Service**                         | **Input:** Consumes `signals` from Kafka. | Processes the final trading signal. Before outputting, it runs risk checks against the current modeled portfolio state.                                                                   |
| **Signal Export $\leftrightarrow$ Risk/Position** | **gRPC Unary RPC**                        | The Signal Export Service calls the Risk Service with a low-latency gRPC request to check margin, position limits, and risk against a **simulated account** before confirming the export. |
| **Risk & Position Service**                       | **gRPC Server**                           | Centralized authority on the **modeled account status** and open positions. Enforces simulated risk rules.                                                                                |
| **Export/Output Channel**                         | File, External API, or Monitoring Stream  | Final destination of the confirmed trading signal (e.g., a file log, a webhook, or a display feed).                                                                                       |

### 2.4. Layer D: Persistence & Monitoring

This layer ensures data is retained and provides real-time visibility into the system's performance.

| Component                         | Protocol / Storage                         | Function                                                                                                               |
| :-------------------------------- | :----------------------------------------- | :--------------------------------------------------------------------------------------------------------------------- |
| **PostgreSQL Database**           | SQL                                        | Stores long-term, structured data: raw prices, calculated indicators, and a log of all confirmed signals for analysis. |
| **Risk $\rightarrow$ Monitoring** | **gRPC Server Streaming**                  | The Risk Service pushes real-time PnL metrics for the **modeled account** continuously to the Monitoring UI.           |
| **Monitoring Dashboard**          | UI Framework (e.g., Python Dash/Streamlit) | Displays live metrics, charts of indicators, and historical backtesting results queried from PostgreSQL.               |

## 3. 🌊 Data Flow Summary

1.  **Ingestion:** Raw prices are fetched and published to Kafka (`raw_prices`).
2.  **Processing:** Spark consumes `raw_prices`, calculates **all indicators in parallel**, and publishes analytical signals to Kafka (`signals`).
3.  **Decision:** The **Signal Export Service** reads the signal, uses a **gRPC Unary call** to check the **Risk Service** for simulated confirmation.
4.  **Export:** The confirmed signal is sent to the final output channel.
5.  **Audit:** All signals and indicator history are logged to **PostgreSQL**.
6.  **Monitoring:** The **Risk Service** streams live PnL metrics for the modeled account to the dashboard via **gRPC Streaming**.

## Should take Notes!!!!!! 4. ⚡ Performance Benchmark (Load Test Results)

This system was benchmarked under continuous load using historical tick data replayed through Kafka to simulate a live market environment for 30 minutes.

### Load Test Scenario:

- **Duration:** 30 minutes
- **Data Rate:** 5,000 raw market events per second (Simulating high volatility)
- **Assets Processed:** 15 Symbols (e.g., ETH/USDT, BTC/USDT, SOL/USDT, etc.)
- **Concurrent Calculations:** **8** distinct Technical Indicators calculated per symbol (**120 total parallel streams**).

### Performance Metrics:

| KPI                         | Value                  | Goal Met | Impact                                                                                    |
| :-------------------------- | :--------------------- | :------- | :---------------------------------------------------------------------------------------- |
| **P95 Signal Latency**      | **350ms**              | **Yes**  | Time from raw price event to final output signal confirmation.                            |
| **Input Throughput**        | **5,012 messages/sec** | **Yes**  | Demonstrates robust Kafka ingestion handling the stream without dropping data.            |
| **Spark Processing Rate**   | **6,450 records/sec**  | **Yes**  | Processing rate consistently exceeded input rate, ensuring zero consumer lag.             |
| **Average Kafka Lag**       | **< 20ms**             | **Yes**  | Proves the Spark consumer keeps up with the high-speed event stream in real time.         |
| **Avg. Spark Batch Time**   | **1.15 seconds**       | **N/A**  | Time required to process a 5,000-record micro-batch across all parallel indicators.       |
| **CPU Utilization (Spark)** | **68%**                | **N/A**  | Shows the system is utilizing distributed resources effectively without being overloaded. |
