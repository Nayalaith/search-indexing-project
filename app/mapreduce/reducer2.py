### app/mapreduce/reducer2.py
#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster

# Connect to Docker Cassandra
cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect()

# Create keyspace and table
session.execute("""
CREATE KEYSPACE IF NOT EXISTS user12_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('user12_keyspace')

session.execute("""
CREATE TABLE IF NOT EXISTS doc_stats (
    doc_id TEXT PRIMARY KEY,
    doc_len INT
)""")

current_doc = None
length = 0

for line in sys.stdin:
    doc_id, _ = line.strip().split("\t")
    if current_doc and doc_id != current_doc:
        session.execute("INSERT INTO doc_stats (doc_id, doc_len) VALUES (%s, %s)", (current_doc, length))
        length = 0
    current_doc = doc_id
    length += 1

if current_doc:
    session.execute("INSERT INTO doc_stats (doc_id, doc_len) VALUES (%s, %s)", (current_doc, length))


