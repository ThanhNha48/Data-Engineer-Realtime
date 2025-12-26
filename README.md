# 🚀 Real-Time Data Engineer Project

## 📌 Overview

The **Real-Time Data Engineer Project** simulates a complete **real-time data processing pipeline**, using popular technologies commonly applied in modern Data Engineering systems:

* **Apache Airflow** – Workflow orchestration & scheduling
* **Apache Kafka** – Message queue / streaming platform
* **Apache Spark Structured Streaming** – Real-time data processing
* **PostgreSQL** – Data sink / warehouse
* **Docker & Docker Compose** – Containerized environment

The pipeline **collects user data from an external API**, **streams it through Kafka**, **cleans and processes it using Spark**, and **stores the final results in PostgreSQL**.

---

## 🏗️ System Architecture

```
┌────────────┐
│ RandomUser │
│    API     │
└─────┬──────┘
      │ (REST API)
      ▼
┌────────────┐
│  Airflow   │  (DAG: user_automation)
│ (Python)   │
└─────┬──────┘
      │ (Kafka Producer)
      ▼
┌────────────┐
│   Kafka    │  (Topic: users_created)
└─────┬──────┘
      │ (Structured Streaming)
      ▼
┌────────────┐
│   Spark    │  (Clean & Validate)
└─────┬──────┘
      │ (JDBC)
      ▼
┌────────────┐
│ PostgreSQL │
└────────────┘
```

---

## ⚙️ Detailed Data Flow

### 1️⃣ Airflow – Data Ingestion & Formatting

* DAG name: `user_automation`
* Schedule: `@daily`

**Task 1 – get_and_format_data**

* Calls external API: `https://randomuser.me/api/`
* Formats user information (name, email, phone, address, etc.)
* Pushes formatted data to **XCom**

**Task 2 – stream_to_kafka**

* Pulls data from XCom
* Streams JSON messages to Kafka topic `users_created`
* Streams continuously for **60 seconds**

---

### 2️⃣ Kafka – Message Streaming Layer

* Broker: `broker:29092`
* Topic: `users_created`
* Message format: JSON (UTF-8 encoded)

Kafka acts as a **decoupling layer** between data producers (Airflow) and consumers (Spark).

---

### 3️⃣ Spark Structured Streaming – Real-Time Processing

**Processing steps:**

* Read streaming data from Kafka (`readStream`)
* Parse JSON messages using `from_json` with a predefined schema
* Clean and standardize data:

  * Trim whitespace
  * Normalize uppercase/lowercase text
  * Validate email format
  * Normalize phone numbers
  * Convert timestamps
* Deduplicate records based on `email`

**Trigger interval:**

* `processingTime = 10 seconds`

---

### 4️⃣ PostgreSQL – Data Storage

* Database: `airflow`
* Table: `created_users`
* Data written using `foreachBatch` with JDBC
* Write mode: `append`

PostgreSQL serves as the **final data sink / warehouse layer**.

---

## 📂 Project Structure (Recommended)

```
Data_Engineer_RealTime/
├── dags/
│   └── user_automation.py      # Airflow DAG
├── spark/
│   └── spark_streaming.py      # Spark Structured Streaming job
├── docker-compose.yml
├── requirements.txt
├── .gitignore
├── README.md
└── scripts/
```

---

## 🧪 Data Quality & Validation

* Deduplicate records based on email
* Standardize and clean input data

---

## 🚀 How to Run the Project (Quick Start)

```bash
# Start all services
docker-compose up -d

# Access Airflow UI
http://localhost:8080

# Trigger the DAG: user_automation

🔗 Access Services
Airflow UI: http://localhost:8080
Kafka Cluster: http://localhost:9021/Clusters
Spark UI: http://localhost:8083

## 🔄 Active Streaming

## Test Image
![test](images/anh.jpg)
