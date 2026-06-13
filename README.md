🚦 Real-Time Traffic Analytics Lakehouse

A production-style streaming data pipeline that ingests real-time traffic sensor events, applies data quality rules, and serves analytics-ready data to PowerBI — all built on the Medallion Architecture (Bronze → Silver → Gold).


Architecture

Kafka Producer (Python)
        │
        ▼
 [Kafka Topic: traffic-topic]
        │
        ▼
  ┌─────────────┐
  │   BRONZE    │  Raw ingestion from Kafka → Delta Lake (no transforms)
  └─────────────┘
        │
        ▼
  ┌─────────────┐
  │   SILVER    │  Data quality: casting, validation, enrichment, quarantine
  └─────────────┘
        │
        ▼
  ┌─────────────┐
  │    GOLD     │  Star Schema: Facts + Dimensions → PowerBI via Thrift Server
  └─────────────┘


Tech Stack

ComponentTechnologyMessage BrokerApache Kafka (KRaft mode)Stream ProcessingApache Spark 3.5.1 (PySpark)Storage FormatDelta Lake 3.2.0MetastoreApache Hive 3.1.3 + PostgreSQLOrchestrationDocker ComposeBI ToolMicrosoft PowerBI (DirectQuery)ProducerPython (kafka-python, Faker)


Data Flow

Producer

Simulates real-time traffic sensor events with:


80% clean events — valid sensor readings with speed, weather, incident, toll data
20% dirty events — null values, wrong datatypes, corrupt JSON, invalid coordinates, duplicates, out-of-range values


Bronze Layer (traffic_bronze.py)


Reads raw JSON from Kafka topic
Stores everything as-is into Delta Lake (no rejection at this stage)
Preserves raw_json and kafka_timestamp for full auditability


Silver + Gold Layer (traffic_silver_gold.py)

Runs as a single streaming job using foreachBatch to save resources.

Silver transforms:


Type casting (StringType → DoubleType, IntegerType, BooleanType)
String standardisation (UPPER + TRIM)
Value validation against allowed lists (vehicle types, weather conditions, etc.)
Range clipping (speed, geo coordinates, humidity, CO2, toll amounts)
Derived columns: speed_band, speed_excess_kmh, safety_score_clean, weather_severity, is_peak_hour
Corrupt / incomplete rows → Quarantine table


Gold Star Schema:

Fact Tables:


fact_traffic_event — main event fact with all measures
fact_incident — filtered incident events with safety context


Dimension Tables:


dim_vehicle — vehicle attributes (type, fuel, make, weight class)
dim_road — road segment attributes (zone, surface, speed limit, coordinates)
dim_weather — weather conditions and severity
dim_time — event timestamp breakdown (year, month, day, hour, DOW, peak hour flag)
dim_signal — traffic signal state
dim_payment — payment method
dim_incidentdet — incident details



Project Structure

Traffic/
├── apps/
│   ├── traffic_bronze.py         # Bronze streaming job
│   └── traffic_silver_gold.py    # Silver + Gold combined streaming job
├── producer/
│   └── traffic_producer.py       # Kafka event producer
├── hive-conf/
│   └── hive-site.xml             # Hive metastore config
├── warehouse/                    # Delta Lake storage (mounted volume)
├── kafka-data/                   # Kafka persistent storage (mounted volume)
├── spark-ivy/                    # Spark dependency cache
└── docker-compose.yaml


Getting Started

Prerequisites


Docker Desktop (4GB+ RAM allocated)
Python 3.x with kafka-python and faker packages


1. Start the infrastructure

bashdocker compose up -d

2. Create Kafka topic

bashdocker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic traffic-topic \
  --bootstrap-server kafka:9092 \
  --partitions 3 --replication-factor 1

3. Run Bronze (Terminal 1)

bashdocker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=512m \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_bronze.py

4. Run Producer (Terminal 2)

bashcd producer && python traffic_producer.py

5. Stop Bronze & Producer, then run Silver+Gold (Terminal 3)

bashdocker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=512m \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_silver_gold.py

6. Register tables & start Thrift Server

bashdocker exec -it spark-worker bash

/opt/spark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/opt/spark/warehouse

Inside spark-sql shell:

sqlCREATE DATABASE IF NOT EXISTS Traffic;
USE Traffic;
CREATE TABLE IF NOT EXISTS fact_traffic_event USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/fact_traffic_event';
CREATE TABLE IF NOT EXISTS fact_incident      USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/fact_incident';
CREATE TABLE IF NOT EXISTS dim_vehicle        USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/dim_vehicle';
CREATE TABLE IF NOT EXISTS dim_road           USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/dim_road';
CREATE TABLE IF NOT EXISTS dim_weather        USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/dim_weather';
CREATE TABLE IF NOT EXISTS dim_time           USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/dim_time';
CREATE TABLE IF NOT EXISTS dim_signal         USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/dim_signal';
CREATE TABLE IF NOT EXISTS dim_payment        USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/dim_payment';
CREATE TABLE IF NOT EXISTS dim_incidentdet    USING DELTA LOCATION '/opt/spark/warehouse/traffic_gold/dim_incidentdet';





Data Quality Rules

IssueHandlingCorrupt JSON→ Quarantine tableNull mandatory fields (event_id, vehicle_id)→ Quarantine tableInvalid categorical values→ Nulled outOut-of-range numeric values→ Clipped to valid rangeWrong datatypes (e.g. speed = "FAST")→ Cast attempt, null on failureInvalid coordinates→ Clipped to valid lat/lon range


Key Design Decisions

Silver + Gold in one job — saves 1 Spark executor core by processing both layers in the same foreachBatch call instead of running two separate streaming jobs.

StringType in Bronze — all fields ingested as strings to avoid schema enforcement failures on dirty data. Type casting happens in Silver only.

Surrogate keys via MD5 — dimension keys generated as md5(concat_ws('|', col1, col2, ...)) for consistency across batches without a sequence generator.

Persistent Kafka volume — ./kafka-data mounted as a Docker volume so topics survive container restarts.

<img width="1912" height="747" alt="Screenshot 2026-06-10 185909" src="https://github.com/user-attachments/assets/b65ef872-8cd1-4246-a926-fce922cfdb71" />
<img width="1917" height="872" alt="Screenshot 2026-06-09 001335" src="https://github.com/user-attachments/assets/7f2e2bbe-f100-4909-9a7b-7124a468ec8d" />
<img width="1453" height="440" alt="Screenshot 2026-06-09 062802" src="https://github.com/user-attachments/assets/13e03b3e-7d53-4220-aa41-7b9e7fb3a9a5" />
