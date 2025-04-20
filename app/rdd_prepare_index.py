from pyspark import SparkContext
import os

# Ensure output dir exists in HDFS
os.system("hdfs dfs -mkdir -p /index")

# Initialize Spark context
sc = SparkContext(appName="PrepareIndex")

# Read all documents from /data in HDFS
rdd = sc.wholeTextFiles("hdfs:///data")

def transform(doc):
    path, content = doc
    filename = os.path.basename(path)

    try:
        if "_" not in filename:
            return None

        doc_id, title_with_ext = filename.split("_", 1)
        title = os.path.splitext(title_with_ext)[0]
        text = content.strip().replace("\n", " ").replace("\t", " ")

        return f"{doc_id}\t{title}\t{text}"
    except Exception as e:
        print(f"[WARN] Skipping file {filename}: {e}")
        return None

# Transform and clean RDD
output = rdd.map(transform).filter(lambda x: x is not None)

# Save result as a single file in HDFS
output.coalesce(1).saveAsTextFile("hdfs:///index/data")

print(">>> [rdd_prepare_index.py] Prepared input for MapReduce written to /index/data")

