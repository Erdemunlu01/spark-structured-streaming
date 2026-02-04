# Spark Structured Streaming Case Studies

This repository contains two **case studies** built using **Spark Structured Streaming**,
along with **theoretical summary notes** related to the topic.  
The studies are developed using a file-based streaming approach via a data generator.

---

## Repository Structure

```
spark-structured-streaming/
│
├─ README.md
│
├─ notes/
│  └─ spark_structured_streaming_summary.md
│
├─ case_study_1_sensor_routing/
│  ├─ delta_table_creation.py
│  └─ delta_table_upsert_streaming.py
│
├─ case_study_2_windowed_aggregation/
│  ├─ spark_streaming_windowed.py
│  └─ groupby_window_aggregation.py
```

---

## Notes

### `notes/spark_structured_streaming_summary.md`

This document provides a summary of the core concepts related to Spark Structured Streaming.

Covered topics include:
- Streaming and micro-batch execution model
- Exactly-once semantics
- Stateless / Stateful transformations
- Output modes (append / update / complete)
- Event time & processing time
- Watermark
- Delta Lake integration
- Checkpoint mechanism

---

## Case Study 1 – Sensor-Based Sink Routing

### Directory
`case_study_1_sensor_routing/`

### Objective
To separate records belonging to **different sensors** from IoT telemetry data
and write each sensor’s data to a **different sink system**.

### Used Sinks
- PostgreSQL
- Hive
- Delta Table

### Files
- `delta_table_creation.py`  
  Delta table creation and initial batch write

- `delta_table_upsert_streaming.py`  
  Real-time upsert implementation using
  Spark Structured Streaming with `foreachBatch` + `merge`

---

## Case Study 2 – Windowed Streaming Aggregation

### Directory
`case_study_2_windowed_aggregation/`

### Objective
To generate sensor-level metrics based on **event-time window aggregation**
over IoT telemetry data.

### Performed Operations
- Event time transformation
- 10-minute window
- 5-minute sliding interval
- Per sensor (device):
  - Signal count
  - Average CO value
  - Average humidity value

### Files
- `spark_streaming_windowed.py`  
  Window aggregation with console sink

- `groupby_window_aggregation.py`  
  Example application demonstrating GroupBy + window usage

---

## Data Generator

### `data_generator_command.txt`

Contains the `data-generator` command used to read the CSV dataset
as a file-based stream.

This setup allows Spark Structured Streaming applications to be tested
in a way that closely resembles real-time data ingestion.

GitHub: https://github.com/erkansirin78/data-generator.git

---

## General Notes

- The checkpoint mechanism is mandatory for stateful operations
- Window aggregation is a stateful operation
- Delta Lake is well-suited for streaming + batch scenarios
- The code is intended for educational and case study purposes

---
