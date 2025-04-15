### app/query.py
from pyspark import SparkConf, SparkContext
from cassandra.cluster import Cluster
import math
import sys

conf = SparkConf().setAppName("BM25Query").set("spark.local.dir", "/app/spark-temp")
sc = SparkContext(conf=conf)

cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect('user12_keyspace')

k1 = 1.5
b = 0.75

term_stats = session.execute("SELECT term, df FROM term_stats")
inverted_index = session.execute("SELECT term, doc_id FROM inverted_index")

term_df = {row.term: row.df for row in term_stats}
doc_list = [(row.term, row.doc_id) for row in inverted_index]
rdd = sc.parallelize(doc_list)

query = sys.stdin.readline().strip().lower().split()
N = len(set([d for _, d in doc_list]))

def bm25(term_doc):
    term, doc_id = term_doc
    if term not in query:
        return None
    df = term_df.get(term, 0)
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
    tf = 1
    score = idf * ((tf * (k1 + 1)) / (tf + k1))
    return (doc_id, score)

scores = rdd.map(bm25).filter(lambda x: x).reduceByKey(lambda a, b: a + b)
top10 = scores.takeOrdered(10, key=lambda x: -x[1])
for doc_id, score in top10:
    print(f"Doc: {doc_id}\tScore: {score:.4f}")
