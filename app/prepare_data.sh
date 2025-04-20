#!/bin/bash

echo ">>> [prepare_data.sh] Generating plain text .txt files in HDFS /data..."

# Run PySpark job to extract 1000 articles and upload as .txt to HDFS
spark-submit \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=4g \
  /app/prepare_data.py

echo ">>> [prepare_data.sh] Cleaning previous index output..."
hdfs dfs -rm -r /index/data

echo ">>> [prepare_data.sh] Running RDD transformation for MapReduce input format..."

# Transform HDFS /data/*.txt into tab-separated RDD file under /index/data
spark-submit \
  --master local \
  /app/rdd_prepare_index.py

hdfs dfs -mv /index/data/part-00000 /index/data/index.txt

echo ">>> [prepare_data.sh] Done."

