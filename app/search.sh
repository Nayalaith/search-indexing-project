#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: bash search.sh \"your query here\""
  exit 1
fi

QUERY="$1"
echo "Running search for query: $QUERY"

chmod +x /app/query.py

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.executor.cores=1 \
  --archives .venv.tar.gz#.venv \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
  /app/query.py "$QUERY"

