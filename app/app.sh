#!/bin/bash
# Start ssh server
service ssh restart 

bash start-services.sh

# Create directory for Spark jars in HDFS
hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars

# Copy Spark jars to HDFS
hdfs dfs -put /usr/local/spark/jars/* /apps/spark/jars/
hdfs dfs -chmod +rx /apps/spark/jars/

# Create root user directory in HDFS
hdfs dfs -mkdir -p /user/root

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Upload data to HDFS
hdfs dfs -put -f /app/a.parquet /a.parquet

# Collect data
bash prepare_data.sh

# echo ">>> [CLEANUP] Removing Spark/Hadoop temp directories..."
# rm -rf /tmp/* /app/spark-temp/* /app/hadoop/tmp/* /app/hadoop/logs/*

# Run the indexer
bash index.sh 

# Run the ranker
bash search.sh "machine"

tail -f /dev/null

