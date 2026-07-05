# Real-Time Traffic Analytics Pipeline

## Project Overview
This repository contains the architecture and source code for a distributed real-time data pipeline engineered to process traffic telemetry and clickstream data. The system is designed to ingest high-velocity data via a local message broker, process the streams using Apache Spark, and enforce data quality within a Delta Lake Medallion architecture for downstream analytics.

![Architecture Diagram](assets/architecture_diagram.png) 

## Architecture & Technology Stack
*   **Containerization:** Docker Desktop
*   **Event Streaming:** Apache Kafka
*   **Stream Processing:** PySpark Structured Streaming (Databricks)
*   **Storage Layer:** Delta Lake (Bronze, Silver, Gold Architecture)
*   **Analytics & Visualization:** Power BI

## Pipeline Workflow & Engineering Responsibilities
The project was executed through a distributed development approach, dividing the pipeline into two primary engineering phases:

### Phase 1: Infrastructure & Ingestion
*   **Local Environment Setup:** Configured and deployed a containerized Apache Kafka cluster utilizing Docker Desktop.
*   **Data Streaming:** Developed a producer mechanism to simulate and transmit high-throughput traffic telemetry data.
*   **Bronze Layer Implementation:** Handled the initial ingestion process, consuming raw JSON events from Kafka topics and writing them continuously to the Delta Lake Bronze layer to establish an immutable historical record.

### Phase 2: Stream Processing & Analytics (Engineered by Yousef Mohamed Mahmoud)
*   **Silver Layer (Cleansed & Conformed):** 
    *   Enforced schemas and executed data type casting on the streaming data.
    *   Applied data quality rules to filter corrupt JSON payloads and validate numerical thresholds (e.g., speed and timestamp bounds).
    *   Implemented event-time watermarking and deduplication logic to systematically handle late-arriving records.

![PySpark Streaming Code](assets/streaming_code.png)

*   **Gold Layer (Business Ready):** Aggregated the cleansed streams to compute traffic-specific Key Performance Indicators (KPIs) and optimized the Delta tables for read-heavy operations.
*   **Serving & Visualization:** Established a direct connection via Databricks SQL endpoint to Power BI, developing an interactive dashboard to visualize real-time traffic metrics.

![Power BI Dashboard](assets/powerbi_dashboard.png)

## Data Evolution (Medallion Architecture Samples)
To logically illustrate the data transformation process and data lineage, below is a trace of specific events as they flow through the pipeline.

### 1. Bronze Layer (Raw Data)
*Notice the first record contains an invalid speed type ("FAST") which will be caught by the DQ rules, while the other two are valid.*

| kafka_timestamp | vehicle_id | raw_json |
| :--- | :--- | :--- |
| 2026-06-27 17:54:24.346 | 0f799de4-74e7-4ae4-a3d4-eabc1cebaab0 | `{"vehicle_id": "0f799de4-...", "road_id": "R300", "city_zone": "SUBURB", "speed": "FAST", "congestion_level": 2, "weather": "FOG", "event_time": "2026-06-27T17:54:24.346396+00:00"}` |
| 2026-06-27 17:54:26.749 | 2d943837-539b-41d9-a8ff-d2c04bb45d45 | `{"vehicle_id": "2d943837-...", "road_id": "R300", "city_zone": "TRAINSTATION", "speed": 38, "congestion_level": 2, "weather": "RAIN", "event_time": "2026-06-27T17:54:26.748997+00:00"}` |
| 2026-06-27 17:53:58.789 | 569b4f3e-7c5a-4edf-b10b-d5526a8adc98 | `{"vehicle_id": "569b4f3e-...", "road_id": "R200", "city_zone": "TRAINSTATION", "speed": 27, "congestion_level": 1, "weather": "STORM", "event_time": "2026-06-27T17:53:58.789779+00:00", "road_condition": "GOOD"}` |

### 2. Silver Layer (Cleansed & Conformed)
*The malformed record (`0f799de4-...`) has been automatically dropped by the Spark validation filter. The remaining valid records are parsed, cast, and enriched with `speed_band`.*

| vehicle_id | speed_int | event_ts | speed_band | dq_flag | time_valid |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 2d943837-539b-41d9-a8ff-d2c04bb45d45 | 38 | 2026-06-27T17:54:26.748Z | medium | ok | 1 |
| 569b4f3e-7c5a-4edf-b10b-d5526a8adc98 | 27 | 2026-06-27T17:53:58.789Z | low | ok | 1 |

### 3. Gold Layer (Star Schema for Power BI)
*Business-level aggregations and materialized views structured in a Star Schema, optimized for direct querying by the Power BI dashboard.*

**fact_traffic (Fact Table):**
| vehicle_id | road_id | city_zone | speed_int | congestion_level | event_ts | peak_flag | speed_band | weather |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2d943837-539b-41d9-a8ff-d2c04bb45d45 | R300 | TRAINSTATION | 38 | 2 | 2026-06-27 17:54:26.748 | 1 | medium | RAIN |
| 569b4f3e-7c5a-4edf-b10b-d5526a8adc98 | R200 | TRAINSTATION | 27 | 1 | 2026-06-27 17:53:58.789 | 1 | low | STORM |

**dim_zone (Dimension Table):**
| city_zone | zone_type | traffic_risk |
| :--- | :--- | :--- |
| TRAINSTATION | Transit Hub | High |
| AIRPORT | Transit Hub | High |
| SUBURB | Residential | Low |

**dim_road (Dimension Table):**
| road_id | road_type | speed_limit |
| :--- | :--- | :--- |
| R400 | City Road | 60 |
| R300 | City Road | 60 |
| R100 | Highway | 100 |

## Technical Implementations & Optimizations
*   **Exactly-Once Semantics:** Configured Spark checkpointing and watermark thresholds to prevent data duplication and ensure idempotent writes during micro-batch processing.
*   **Storage Optimization:** Executed automated Delta Lake `OPTIMIZE` and `Z-ORDER` operations to compact small Parquet files generated by continuous streaming, preventing read performance degradation in the BI layer.

## Contributors
*   **Mohamed Amin:** Data Engineer (Infrastructure, Kafka, Bronze Layer)
*   **Yousef Mohamed Mahmoud:** Data Engineer (PySpark, Silver/Gold Layers, Analytics)
