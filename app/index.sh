### app/index.sh
#!/bin/bash
INPUT_PATH=${1:-/index/data}

# Run first MapReduce job: inverted index + DF
hdfs dfs -rm -r /tmp/index
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -input $INPUT_PATH \
    -output /tmp/index \
    -mapper "/app/mapreduce/mapper1.py" \
    -reducer "/app/mapreduce/reducer1.py" \
    -file "/app/mapreduce/mapper1.py" \
    -file "/app/mapreduce/reducer1.py"

# Run second MapReduce job: document lengths
hdfs dfs -rm -r /tmp/doc_stats
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -input $INPUT_PATH \
    -output /tmp/doc_stats \
    -mapper "/app/mapreduce/mapper2.py" \
    -reducer "/app/mapreduce/reducer2.py" \
    -file "/app/mapreduce/mapper2.py" \
    -file "/app/mapreduce/reducer2.py"

