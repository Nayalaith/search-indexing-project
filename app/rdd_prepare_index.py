### app/rdd_prepare_index.py
from pyspark import SparkContext
import os

sc = SparkContext(appName="PrepareIndex")
rdd = sc.wholeTextFiles("/data")

def transform(doc):
    path, content = doc
    filename = os.path.basename(path)
    try:
        doc_id, title_with_ext = filename.split("_", 1)
        title = os.path.splitext(title_with_ext)[0]
        return f"{doc_id}\t{title}\t{content.strip()}"
    except:
        return None

output = rdd.map(transform).filter(lambda x: x is not None)
output.coalesce(1).saveAsTextFile("/index/data")

