### app/mapreduce/reducer1.py
#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster

# Connect to Docker Cassandra
cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect()

# Create keyspace if not exists
session.execute("""
CREATE KEYSPACE IF NOT EXISTS user12_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('user12_keyspace')

# Create tables
session.execute("""
CREATE TABLE IF NOT EXISTS inverted_index (
    term TEXT,
    doc_id TEXT,
    PRIMARY KEY (term, doc_id)
)""")
session.execute("""
CREATE TABLE IF NOT EXISTS term_stats (
    term TEXT PRIMARY KEY,
    df INT
)""")

current_term = None
doc_ids = set()

for line in sys.stdin:
    term, doc_id = line.strip().split("\t")
    if current_term and term != current_term:
        for d in doc_ids:
            session.execute("INSERT INTO inverted_index (term, doc_id) VALUES (%s, %s)", (current_term, d))
        session.execute("INSERT INTO term_stats (term, df) VALUES (%s, %s)", (current_term, len(doc_ids)))
        doc_ids = set()
    current_term = term
    doc_ids.add(doc_id)

if current_term:
    for d in doc_ids:
        session.execute("INSERT INTO inverted_index (term, doc_id) VALUES (%s, %s)", (current_term, d))
    session.execute("INSERT INTO term_stats (term, df) VALUES (%s, %s)", (current_term, len(doc_ids)))

