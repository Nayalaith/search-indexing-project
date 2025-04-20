from pyspark import SparkContext, SQLContext
from cassandra.cluster import Cluster
import math
import sys

# Initialize Spark context and SQL context
sc = SparkContext(appName="BM25_Query")
sqlContext = SQLContext(sc)

# Connect to Cassandra
cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect('user12_keyspace')  # Connect to the correct keyspace

# Function to calculate BM25 score
def calculate_bm25(tf, doc_length, doc_freq, total_docs, avg_doc_length, k1=1.5, b=0.75):
    idf = math.log((total_docs - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0)
    tf_term = tf * (k1 + 1) / (tf + k1 * (1 - b + b * doc_length / avg_doc_length))
    return idf * tf_term

# Function to retrieve documents from the index and calculate BM25
def query_bm25(query_terms):
    # Get total number of documents and average document length
    total_docs = session.execute("SELECT count(*) FROM doc_stats").one()[0]
    avg_doc_length = session.execute("SELECT avg(length) FROM doc_stats").one()[0]

    # Get the term statistics and inverted index from Cassandra
    term_stats = session.execute("SELECT * FROM term_stats")
    inverted_index = session.execute("SELECT * FROM inverted_index")

    # Convert to RDDs for processing
    term_stats_rdd = sc.parallelize(list(term_stats))
    inverted_index_rdd = sc.parallelize(list(inverted_index))

    # Build dictionaries from the Cassandra data
    term_df = {row.term: row.df for row in term_stats}
    inverted_index_dict = {}
    for row in inverted_index:
        if row.term not in inverted_index_dict:
            inverted_index_dict[row.term] = {}
        inverted_index_dict[row.term][row.doc_id] = row.tf

    # For each query term, retrieve its documents and compute BM25 score
    doc_scores = {}
    for term in query_terms:
        if term in inverted_index_dict:
            for doc_id, tf in inverted_index_dict[term].items():
                doc_length = session.execute(f"SELECT length FROM doc_stats WHERE doc_id = '{doc_id}'").one()[0]
                doc_freq = term_df[term]
                score = calculate_bm25(tf, doc_length, doc_freq, total_docs, avg_doc_length)
                if doc_id not in doc_scores:
                    doc_scores[doc_id] = 0
                doc_scores[doc_id] += score

    # Sort documents by score and return the top 10
    sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_docs[:10]

# Read query from stdin
query = sys.stdin.read().strip()
query_terms = query.split()

# Perform BM25 ranking and retrieve the top 10 documents
top_docs = query_bm25(query_terms)

# Output top 10 documents with their IDs and Titles
for doc_id, score in top_docs:
    doc_title = session.execute(f"SELECT title FROM inverted_index WHERE doc_id = '{doc_id}' LIMIT 1").one()[0]
    print(f"Doc ID: {doc_id}, Title: {doc_title}, Score: {score}")

# Close Cassandra connection
cluster.shutdown()

