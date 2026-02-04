# IoT Data Control – Case Study 1 (English)

This document contains the validation steps performed to **verify that data has been written to the correct sinks**
after running the Spark Structured Streaming application for  
**Case Study 1 – Sensor-Based Sink Routing**.

---

## PostgreSQL Validation

The following query was used to validate the data written to PostgreSQL:

```sql
SELECT * FROM public.iot_stream LIMIT 10;
```

Sample output:

```
ts                |device           |co          |humidity|light|lpg         |motion|smoke      |temp              |time
------------------+-----------------+------------+--------+-----+------------+------+-----------+------------------+-----------------------
1594512094.7355676|00:0f:00:70:91:0a|0.0028400887|    76.0|False|0.0051143835|False |0.013274836|19.700000762939453|2020-07-12 03:01:34
...
```

Total record count:

```sql
SELECT COUNT(*) FROM public.iot_stream;
```

```
count
-----
69
```

✔ Only data belonging to sensor **00:0f:00:70:91:0a** is present.

---

## Hive Validation

Data written to Hive was validated using the following query:

```sql
SELECT * FROM test1.case_study_io LIMIT 10;
```

Sample output:

```
ts                |device           |co          |humidity|light|lpg         |motion|smoke      |temp|time
------------------+-----------------+------------+--------+-----+------------+------+-----------+----+-----------------------
1594512294.1652725|b8:27:eb:bf:9d:51|0.0049673636|    51.0|False|0.0076635773|False |0.020447621|22.7|2020-07-12 03:04:54
...
```

Total record count:

```sql
SELECT COUNT(*) FROM test1.case_study_io;
```

```
_c0
---
115
```

✔ Only data belonging to sensor **b8:27:eb:bf:9d:51** is present.

---

## Delta Lake Validation

The Delta Lake table was validated by reading it through Spark:

```python
spark.read.format("delta")     .load("hdfs://localhost:9000/user/train/delta-stream-caseStudy")     .show()
```

Sample output:

```
+------------------+-----------------+------------+--------+-----+------------+------+-----------+-----------------+-------------------+
|ts                |device           |co          |humidity|light|lpg         |motion|smoke      |temp             |time
+------------------+-----------------+------------+--------+-----+------------+------+-----------+-----------------+-------------------+
|1594512463.8974855|1c:bf:ce:15:ec:4d|0.004341545 |78.0    |True |0.0069522546|False |0.018426906|27.0             |2020-07-12 03:07:43
...
```

Total record count:

```
66
```

✔ Only data belonging to sensor **1c:bf:ce:15:ec:4d** is present.

---

## Conclusion

Based on these validations:

- Each sensor’s data has been written **only to its designated sink**
- No sensor data appears in an incorrect sink
- All **Case Study 1** requirements have been successfully met
