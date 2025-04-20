import os
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

# Read input data
df = spark.read.parquet("/a.parquet")

# Sample N articles
n = 100
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)

# Ensure temporary local directory exists
os.makedirs("/tmp/docs", exist_ok=True)

def save_to_hdfs(row):
    try:
        # Skip rows with missing fields
        if not row['title'] or not row['text']:
            print(f">>> [WARN] Skipping row with empty title or text (id={row['id']})")
            return

        # Clean and format filename
        clean_title = sanitize_filename(row['title']).replace(" ", "_")
        if not clean_title.strip():
            print(f">>> [WARN] Skipping row with invalid title: {row['title']}")
            return

        filename = f"{row['id']}_{clean_title}.txt"
        local_path = f"/tmp/docs/{filename}"

        # Write file locally
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(row['text'])

        # Upload to HDFS (quote paths safely)
        if os.path.exists(local_path):
            os.system("hdfs dfs -mkdir -p /data")
            os.system(f"hdfs dfs -put -f '{local_path}' /data/")
            print(f">>> [OK] Uploaded {filename} to HDFS /data/")
        else:
            print(f">>> [WARN] File not found before upload: {local_path}")

    except Exception as e:
        print(f">>> [ERROR] Failed to process row {row['id']}: {e}")

# Apply the save function on each row
df.foreach(save_to_hdfs)

print(">>> [prepare_data.py] Finished writing documents to HDFS /data/")

