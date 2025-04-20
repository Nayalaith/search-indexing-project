#!/bin/bash
# app/index.sh

echo "Starting Hadoop MapReduce..."

# Check if a local file is provided as the first argument
if [ -z "$1" ]; then
    # No file is provided, so use the default HDFS directory
    echo "No local file provided. Using /index/data as the input directory..."
    INPUT_PATH="/index/data/"
else
    # A local file is provided, upload it to HDFS
    filename=$(basename "$1")
    echo "Input file path: $1"
    echo "Uploading file $filename to HDFS..."

    # Clean previous output 
    hdfs dfs -rm -r -f /tmp/index_output

    # Ensure HDFS directories exist
    hdfs dfs -mkdir -p /index/data/
    hdfs dfs -mkdir -p /tmp/

    # Upload the file to HDFS
    hdfs dfs -put -f "$1" "/index/data/$filename"
    hdfs dfs -ls /index/data/

    # Set the HDFS input path to the uploaded file
    INPUT_PATH="/index/data/$filename"
fi

# Run Hadoop Streaming with virtual environment
mapred streaming \
  -D mapred.reduce.tasks=3 \
  -files "/app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py" \
  -archives "/app/.venv.tar.gz#.venv" \
  -input "$INPUT_PATH" \
  -output "/tmp/index_output" \
  -mapper ".venv/bin/python3 mapper1.py" \
  -reducer ".venv/bin/python3 reducer1.py" \

