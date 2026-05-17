<div align="center">

# 🚀 NASA Log Analytics — ETL Pipeline Framework

### A Unified Big Data ETL Tool for Processing 3.4M+ NASA HTTP Access Logs

**Hadoop MapReduce** · **Apache Pig** · **Apache Hive** · **MongoDB** · **MySQL**

[![Java](https://img.shields.io/badge/Java-8-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://www.java.com/)
[![Hadoop](https://img.shields.io/badge/Hadoop-2.10-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=white)](https://hadoop.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0-47A248?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://www.mysql.com/)

---

*A production-grade command-line ETL tool that ingests raw NASA web server logs from July and August 1995, processes them through four independent Big Data pipelines, stores structured results in a MySQL relational database, and provides built-in reporting and cross-pipeline comparison — all from a single unified CLI.*

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Analytical Queries](#-analytical-queries)
- [Batch Processing Strategies](#-batch-processing-strategies)
- [MySQL Schema](#-mysql-schema)
- [Prerequisites](#-prerequisites)
- [Setup & Installation](#-setup--installation)
- [Usage](#-usage)
  - [CLI Commands](#cli-commands)
  - [Interactive Mode](#interactive-mode)
- [Example Workflow](#-example-workflow)
- [Configuration](#-configuration)

---

## 🔍 Overview

This project implements a **unified ETL (Extract, Transform, Load) framework** that processes **3.4 million+ NASA HTTP access log records** (July & August 1995) through four independent Big Data processing engines:

| Pipeline | Engine | Processing Approach |
|----------|--------|---------------------|
| 🐘 **MapReduce** | Hadoop MapReduce (Local Standalone) | Custom Java Mapper/Reducer classes |
| 🐷 **Pig** | Apache Pig (Local Mode) | Pig Latin scripts with `REGEX_EXTRACT` |
| 🐝 **Hive** | Apache Hive (Local Mode + Embedded Metastore) | HiveQL with `regexp_extract()` UDFs |
| 🍃 **MongoDB** | MongoDB (Standalone) | Aggregation Framework with `$regexFind` |

Each pipeline independently:
1. **Extracts** raw log lines from text files
2. **Transforms** them using regex parsing (via the engine's native capabilities)
3. **Loads** structured results into a shared **MySQL** relational database

The framework then provides **reporting** and **cross-pipeline comparison** tools, enabling side-by-side performance benchmarking and result consistency validation across all four engines.

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CLI Entry Point                         │
│              Main.java (Subcommand Router)                  │
│    run │ report │ list-runs │ compare │ init-db │ help      │
└────────────────────────┬────────────────────────────────────┘
                         │
              ┌──────────▼─────────┐
              │    Orchestrator    | 
              │  (Batch Loop +     │
              │   Runtime Timing)  │
              └──────────┬─────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
  │  Batching   │ │  Pipeline   │ │  Result     │
  │  Strategy   │ │  Interface  │ │  Loader     │
  ├─────────────┤ ├─────────────┤ ├─────────────┤
  │ record_count│ │ MapReduce   │ │ MySQL       │
  │ file_based  │ │ Pig         │ │ INSERT INTO │
  └─────────────┘ │ Hive        │ └─────────────┘
                  │ MongoDB     │
                  └─────────────┘
```

**Data Flow:** Raw `.log` files → Batch Splitter → Temp Files → Pipeline Engine → Parsed Results → MySQL Tables

---

## 📁 Project Structure

```
etl_project/
│
├── pom.xml                          # Maven build config (Hadoop, MongoDB, MySQL deps)
│
├── src/main/java/com/etl/
│   ├── Main.java                    # CLI entry point (subcommand router)
│   ├── cli/
│   │   └── InteractiveMode.java     # Guided step-by-step menu interface
│   ├── config/
│   │   └── AppConfig.java           # Centralized DB credentials, paths, regex
│   ├── orchestrator/
│   │   └── Orchestrator.java        # Core batch loop, timing, coordination
│   ├── batching/
│   │   ├── BatchStrategy.java       # Strategy interface
│   │   ├── RecordCountBatchStrategy  # Fixed N-records-per-batch splitter
│   │   ├── FileBasedBatchStrategy    # One-batch-per-file splitter
│   │   └── BatchWriter.java         # Writes batch data to temp files
│   ├── pipeline/
│   │   ├── Pipeline.java            # Common pipeline interface
│   │   ├── MapReducePipeline.java   # Launches Hadoop MapReduce jobs
│   │   ├── PigPipeline.java         # Launches Pig Latin scripts
│   │   ├── HivePipeline.java        # Launches HiveQL scripts
│   │   └── MongoDBPipeline.java     # Runs MongoDB aggregation pipelines
│   ├── mapreduce/
│   │   ├── Query1Mapper.java        # MR Mapper: Daily traffic parsing
│   │   ├── Query1Reducer.java       # MR Reducer: Daily traffic aggregation
│   │   ├── Query2Mapper.java        # MR Mapper: Resource parsing
│   │   ├── Query2Reducer.java       # MR Reducer: Resource aggregation
│   │   ├── Query3Mapper.java        # MR Mapper: Error parsing
│   │   └── Query3Reducer.java       # MR Reducer: Error aggregation
│   ├── db/
│   │   ├── ConnectionManager.java   # JDBC connection factory
│   │   ├── SchemaInitializer.java   # CREATE/DROP table management
│   │   └── ResultLoader.java        # INSERT results + metadata into MySQL
│   ├── model/
│   │   ├── BatchData.java           # Batch ID + list of raw lines
│   │   └── PipelineResult.java      # Query results + malformed count
│   └── report/
│       └── ReportGenerator.java     # Report, list-runs, compare logic
│
├── scripts/
│   ├── pig/
│   │   ├── etl_query1.pig           # Pig Latin: Daily traffic summary
│   │   ├── etl_query2.pig           # Pig Latin: Top requested resources
│   │   └── etl_query3.pig           # Pig Latin: Hourly error analysis
│   └── hive/
│       ├── create_table.hql         # HiveQL: External table definition
│       ├── query1.hql               # HiveQL: Daily traffic summary
│       ├── query2.hql               # HiveQL: Top requested resources
│       └── query3.hql               # HiveQL: Hourly error analysis
│
├── sql/
│   └── schema.sql                   # MySQL schema (6 tables)
│
├── data/                            # NASA log files (not in repo, see Setup)
│   ├── NASA_access_log_Jul95        # ~1.57M records (205 MB)
│   └── NASA_access_log_Aug95        # ~1.89M records (168 MB)
│
└── src/main/resources/
    └── log4j.properties             # Suppresses Hadoop/MongoDB verbose logs
```

---

## 📊 Analytical Queries

All four pipelines execute the same three analytical queries on the NASA log data:

### Query 1: Daily Traffic Summary
Groups all requests by **date** and **HTTP status code**, computing the total number of requests and total bytes transferred for each combination.

### Query 2: Top Requested Resources
Identifies the most frequently accessed **URL paths**, counting the total requests, total bytes, and distinct client hosts for each resource.

### Query 3: Hourly Error Analysis
Analyzes **HTTP error patterns** (status codes 400–599) grouped by date and hour. Calculates the error rate (errors / total requests) and counts distinct error-producing hosts for each hourly window.

---

## 🔄 Batch Processing Strategies

The framework supports two configurable batching strategies for splitting the raw input data:

| Strategy | Flag | Description |
|----------|------|-------------|
| **Record Count** | `--batch-strategy record_count --batch-size N` | Splits across all files into fixed-size batches of exactly N records. Records from different files can be combined in the same batch. If the last batch has fewer than N records, it is still processed as a valid batch. |
| **File Based** | `--batch-strategy file_based` | Creates one batch per input file. July = Batch 1 (~1.57M records), August = Batch 2 (~1.89M records). |

---

## 🗄 MySQL Schema

Results from all pipelines are stored in a shared MySQL database (`etl_results`) with **6 relational tables**:

| Table | Purpose |
|-------|---------|
| `run_metadata` | One row per ETL execution (pipeline name, batch strategy, total records, runtime, timestamp) |
| `batch_metadata` | Per-batch statistics (batch size, malformed count, valid records, batch runtime) |
| `query1_daily_traffic` | Query 1 results: date, status code, request count, total bytes |
| `query2_top_resources` | Query 2 results: resource path, request count, bytes, distinct hosts |
| `query3_hourly_errors` | Query 3 results: date, hour, error count, total count, error rate, distinct hosts |
| `malformed_summary` | Malformed record counts per batch |

All result tables include `run_id` and `batch_id` foreign keys with `ON DELETE CASCADE`, enabling clean data management.

---

## ✅ Prerequisites

| Software | Version | Purpose |
|----------|---------|---------|
| **Java JDK** | OpenJDK 8 (1.8.x) | Required by Hive 2.3.9 (fails on Java 11+); compatible with Pig 0.18.0, Hadoop 2.10.2 |
| **Apache Maven** | 3.6+ | Build tool and dependency management |
| **Apache Hadoop** | 2.10.2 | Last stable Hadoop 2.x release; MapReduce execution engine |
| **Apache Pig** | 0.18.0 (`hadoop2` binary) | Pig Latin execution engine |
| **Apache Hive** | 2.3.9 | HiveQL execution engine; embedded Derby metastore |
| **MongoDB** | 7.0.x (Community) | MongoDB aggregation engine |
| **MySQL** | 8.0.x (Community) | Relational result storage |

> ⚠️ **Java 8 is the ONLY JDK that works across all components simultaneously.** Do not use Java 11, 17, or 21.

> 📌 This project is designed for **Ubuntu on WSL (Windows Subsystem for Linux)** or native Ubuntu. All pipelines run in **local/standalone mode** — no cluster setup is required.

---

## 🛠 Setup & Installation (WSL / Ubuntu)

Perform the following steps in order. Later tools depend on earlier ones.

### Step 1: Install Java 8

```bash
sudo apt update
sudo apt install openjdk-8-jdk -y
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
java -version   # Must show 1.8.x
```

### Step 2: Install Apache Maven

```bash
sudo apt install maven -y
mvn -version   # Must show Maven 3.6+ and Java 1.8.x
```

### Step 3: Install and Configure Hadoop 2.10.2

```bash
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz
tar -xzf hadoop-2.10.2.tar.gz
sudo mv hadoop-2.10.2 /opt/hadoop

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CLASSPATH=$(hadoop classpath)
```

Edit `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

For **local (standalone) mode**, no further configuration of `core-site.xml` or `hdfs-site.xml` is needed. Verify:
```bash
hadoop version   # Should show 2.10.2
```

### Step 4: Install Apache Pig 0.18.0

```bash
wget https://downloads.apache.org/pig/pig-0.18.0/pig-0.18.0-hadoop2.tar.gz
tar -xzf pig-0.18.0-hadoop2.tar.gz
sudo mv pig-0.18.0 /opt/pig

export PIG_HOME=/opt/pig
export PATH=$PATH:$PIG_HOME/bin
export PIG_CLASSPATH=$HADOOP_CONF_DIR
```

Verify:
```bash
pig -x local -version   # Should show 0.18.0
```

### Step 5: Install Apache Hive 2.3.9

```bash
wget https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz
tar -xzf apache-hive-2.3.9-bin.tar.gz
sudo mv apache-hive-2.3.9-bin /opt/hive

export HIVE_HOME=/opt/hive
export PATH=$PATH:$HIVE_HOME/bin
```

Initialize embedded Derby metastore:
```bash
cd /opt/hive
$HIVE_HOME/bin/schematool -dbType derby -initSchema
```

Create/edit `$HIVE_HOME/conf/hive-site.xml`:
```xml
<configuration>
  <property>
    <name>hive.execution.engine</name>
    <value>mr</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/tmp/hive/warehouse</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
</configuration>
```

Verify:
```bash
hive --version   # Should show 2.3.9
```

### Step 6: Install MongoDB 7.0

```bash
sudo apt install gnupg curl -y

curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
  sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
  https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

sudo apt update
sudo apt install -y mongodb-org
sudo systemctl start mongod
sudo systemctl enable mongod
mongosh --eval "db.runCommand({ping: 1})"   # Should return { ok: 1 }
```

### Step 7: Install MySQL 8.0

```bash
sudo apt install mysql-server -y
sudo systemctl start mysql
sudo systemctl enable mysql
```

Create the project database and user:
```bash
sudo mysql -u root
```
```sql
CREATE DATABASE etl_results;
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON etl_results.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

### Step 8: Download the NASA Dataset

```bash
mkdir -p data
cd data
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
gunzip NASA_access_log_Jul95.gz
gunzip NASA_access_log_Aug95.gz
cd ..
```

> **Dataset Source:** [NASA HTTP Access Logs](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) — July 1, 1995 to August 31, 1995.

### Step 9: Persist Environment Variables

Add all exports to `~/.bashrc` so they survive terminal restarts:
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc
echo 'export PIG_HOME=/opt/pig' >> ~/.bashrc
echo 'export HIVE_HOME=/opt/hive' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$HIVE_HOME/bin:$PATH' >> ~/.bashrc
echo 'export PIG_CLASSPATH=$HADOOP_CONF_DIR' >> ~/.bashrc
source ~/.bashrc
```

### Step 10: Configure MySQL Credentials in the Project

Edit `src/main/java/com/etl/config/AppConfig.java` and update these lines with your MySQL credentials:

```java
public static final String MYSQL_USER = "your_mysql_user";
public static final String MYSQL_PASSWORD = "your_mysql_password";
```

### Step 11: Build the Project

```bash
mvn clean package -DskipTests
```

This compiles all Java source files and packages them into a single executable fat JAR at `target/etl-nasa-logs-1.0-SNAPSHOT.jar`.

### Step 12: Initialize the MySQL Schema

```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db
```

This creates all 6 tables with foreign key relationships. Safe to run multiple times — uses `CREATE TABLE IF NOT EXISTS`.

---

## 💻 Usage

### CLI Commands

The tool uses a **subcommand pattern**:

```
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar <command> [options]
```

| Command | Description |
|---------|-------------|
| `run` | Execute an ETL pipeline |
| `report` | Display results from a past run |
| `list-runs` | Show summary table of all past runs |
| `compare` | Compare results and runtimes across pipelines |
| `init-db` | Initialize or reset the MySQL schema |
| `help` | Show complete usage information |
| *(no command)* | Launch interactive guided mode |

#### `run` — Execute a Pipeline

```bash
# Run all queries on MongoDB with file-based batching
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mongodb --query all --batch-strategy file_based

# Run only Query 1 on Pig with record-count batching
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline pig --query query1 --batch-strategy record_count --batch-size 500000
```

| Flag | Values | Required |
|------|--------|----------|
| `--pipeline` | `mongodb`, `mapreduce`, `pig`, `hive` | Yes |
| `--query` | `query1`, `query2`, `query3`, `all` | No (default: `all`) |
| `--batch-strategy` | `record_count`, `file_based` | Yes |
| `--batch-size` | Any positive integer | Only for `record_count` |
| `--data-dir` | Path to log files | No (default: `./data`) |

#### `report` — View Run Results

```bash
# Report for a specific run
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --run-id 3

# Report for the latest run
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --latest

# Latest run filtered by pipeline
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --latest --pipeline pig
```

#### `list-runs` — View All Past Runs

```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar list-runs

# Filter by pipeline
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar list-runs --pipeline mongodb
```

#### `compare` — Cross-Pipeline Comparison

```bash
# Compare latest run per pipeline for a given batch strategy
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar compare --batch-strategy file_based

# Compare specific runs by ID
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar compare --run-ids 1,2,3,4
```

Outputs a side-by-side comparison table, a **runtime ranking** (fastest to slowest), and a **result consistency check** that verifies all pipelines produce identical row counts.

#### `init-db` — Schema Management

```bash
# Safe create (preserves existing data)
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db

# Destructive reset (drops and recreates all tables)
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db --reset
```

---

### Interactive Mode

Launch the tool with **no arguments** to enter a guided step-by-step menu:

```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar
```

```
======================================================================
NASA Log Analytics ETL Tool -- Interactive Mode
======================================================================

What would you like to do?

  [1] Run an ETL pipeline
  [2] View report for a past run
  [3] List all past runs
  [4] Compare pipelines
  [5] Initialize MySQL schema
  [6] Exit
```

The interactive mode walks you through each step with input validation — it will never crash on bad input.

---

## 🎬 Example Workflow

Here is a complete end-to-end workflow demonstrating all features:

```bash
# 1. Build the project
mvn clean package -DskipTests

# 2. Clean temporary files from previous runs
rm -rf /tmp/etl_* /tmp/hive_*

# 3. Reset the database (clean slate)
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db --reset

# 4. Initialize the database
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db

# 5. Run all 4 pipelines with file-based batching
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mongodb --query all --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mapreduce --query all --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline pig --query all --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline hive --query all --batch-strategy file_based

# 6. List all completed runs
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar list-runs

# 7. View detailed report for the latest Pig run
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --latest --pipeline pig

# 8. Compare all pipelines side-by-side
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar compare --batch-strategy file_based

# 9. Try record-count batching with 500K records per batch
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mongodb --query all --batch-strategy record_count --batch-size 500000
```

---

## ⚙ Configuration

All configuration is centralized in [`src/main/java/com/etl/config/AppConfig.java`](src/main/java/com/etl/config/AppConfig.java):

| Setting | Default | Description |
|---------|---------|-------------|
| `MYSQL_URL` | `jdbc:mysql://localhost:3306/etl_results` | MySQL JDBC connection URL |
| `MYSQL_USER` | *(set by user)* | MySQL username |
| `MYSQL_PASSWORD` | *(set by user)* | MySQL password |
| `MONGO_CONNECTION_STRING` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DATABASE` | `etl_nasa` | MongoDB database name |
| `DEFAULT_DATA_DIR` | `./data` | Path to NASA log files |
| `LOG_FILE_ENCODING` | `ISO-8859-1` | Character encoding for log files |
| `TEMP_BATCH_DIR` | `/tmp/etl_batches` | Temp directory for batch files |

