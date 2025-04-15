#!/bin/bash

# Run PySpark job to generate plain text docs
spark-submit /app/prepare_data.py

# Put the docs into HDFS /data
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /app/data/* /data/

# 🧹 Remove existing output if needed
hdfs dfs -rm -r /index/data

# Transform for MapReduce pipeline
spark-submit --master local /app/rdd_prepare_index.py

