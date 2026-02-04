# Spark Structured Streaming – Core Concepts and Streaming Flow

This document summarizes the core concepts of Spark Structured Streaming and streaming systems,
covering critical topics such as **exactly-once**, **output modes**, **stateful/stateless transformations**,
**event time**, **Kafka**, and **Delta Lake**.

---

## 1. What is Streaming?

Streaming refers to processing data not as a **completed file**,
but as a **continuously flowing stream**.

Examples:
- Events flowing from Kafka topics
- Log lines coming from a socket
- Sensor (IoT) data

Spark Structured Streaming processes this flow using a **micro-batch** execution model.

---

## 2. Delivery Semantics

### at-most-once
- Messages **may be lost**
- No duplicates
- Weakest guarantee

### at-least-once
- Messages are delivered **at least once**
- No data loss
- **Duplicates may occur**

### exactly-once (end-to-end)
- No data loss and no duplicates
- Strongest guarantee
- Difficult and costly to implement

---

## 3. End-to-End Exactly-Once Requirements

To achieve end-to-end exactly-once semantics:

✔ The data source (e.g., Kafka) must retain messages  
✔ Transformations must be **deterministic**  
✔ The sink system must be **idempotent**  
✔ A checkpoint mechanism must be used  

❌ Non-deterministic transformations break exactly-once guarantees.

---

## 4. Stateless vs Stateful Transformations

### Stateless Transformations
Do not keep state from previous data.

Examples:
- map
- select
- filter

Advantages:
- Fast
- Low resource consumption

---

### Stateful Transformations
Maintain state across batches.

Examples:
- groupBy().count()
- window aggregation
- mapGroupsWithState

Disadvantages:
- Require state storage
- Checkpointing is mandatory

---

## 5. Output Modes

### Append Mode
- Only **new rows** are written
- Commonly used for stateless operations

✔ Most performant mode

---

### Update Mode
- Only updated rows are written
- Used for stateful operations

---

### Complete Mode
- Entire result table is rewritten on each trigger
- Resource intensive

⚠ Used only for aggregation queries

---

## 6. Output Mode vs Transformation Compatibility

| Transformation Type | Supported Output Modes |
|---------------------|------------------------|
| Stateless           | Append                 |
| Aggregation         | Update, Complete       |
| mapGroupsWithState  | Append, Update         |
| Join                | Append, Complete       |

---

## 7. Event Time vs Processing Time

### Event Time
- The actual time when the data was generated
- Crucial for accurate time-based analytics

### Processing Time
- The time when Spark processes the data
- Late data may cause inconsistencies

Therefore:
➡ Event Time + Watermark should be used together

---

## 8. What is a Watermark?

Watermark:
- Defines how long Spark waits for late-arriving data
- Prevents state from growing indefinitely

Example:
```python
.withWatermark("event_time", "10 minutes")
```

---

## 9. Kafka and Spark Streaming

### Reading from Kafka
- Subscribes to topics
- Offset management handled by Spark

### Writing to Kafka
- Topic parameter is mandatory

Kafka exactly-once requires:
- Idempotent producers
- Transactions enabled

---

## 10. Delta Lake and Streaming

Delta Lake:
- Provides ACID guarantees
- Works with both streaming and batch
- Ideal for exactly-once semantics

Writing to a Delta table:
```python
df.write.format("delta").mode("overwrite").save(path)
```

Advantages:
- Transaction log
- Schema enforcement
- Time travel

---

## 11. Why is Checkpointing Mandatory?

Checkpointing:
- Stores offsets
- Persists state
- Enables recovery after failures

Without checkpointing:
❌ No exactly-once guarantee  
❌ Stateful queries cannot run  

---

## 12. Key Takeaways

- Streaming = infinite data
- Stateless operations are fast, stateful ones are expensive
- Append is the lightest output mode
- Exactly-once requires determinism, idempotency, and checkpointing
- Event time reflects real-world time
- Delta Lake is the gold standard for streaming pipelines
