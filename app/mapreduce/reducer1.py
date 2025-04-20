#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster

# Establish Cassandra connection
try:
    cass_cluster = Cluster(['cassandra-server'], port=9042)
    cass_session = cass_cluster.connect()
    print("[INFO] Successfully connected to Cassandra", file=sys.stderr)
except Exception as conn_err:
    print(f"[ERROR] Could not connect to Cassandra: {conn_err}", file=sys.stderr)
    sys.exit(1)

# Define keyspace and table structures
try:
    cass_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS user12_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    cass_session.set_keyspace('user12_keyspace')

    cass_session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term TEXT,
            doc_id TEXT,
            tf INT,
            PRIMARY KEY (term, doc_id)
        )
    """)

    cass_session.execute("""
        CREATE TABLE IF NOT EXISTS term_stats (
            term TEXT PRIMARY KEY,
            df INT
        )
    """)

    cass_session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id TEXT PRIMARY KEY,
            length INT
        )
    """)

    print("[INFO] Schema created and initialized in Cassandra", file=sys.stderr)
except Exception as schema_err:
    print(f"[ERROR] Failed to create schema: {schema_err}", file=sys.stderr)
    sys.exit(1)

# Internal accumulators
active_term = None
term_postings = {}    # {doc_id: tf}
doc_term_lengths = {} # {doc_id: total term frequency}

def save_term_to_cassandra(term, postings):
    doc_freq = len(postings)
    try:
        for doc, freq in postings.items():
            cass_session.execute(
                "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
                (term, doc, freq)
            )
            doc_term_lengths[doc] = doc_term_lengths.get(doc, 0) + freq

        cass_session.execute(
            "INSERT INTO term_stats (term, df) VALUES (%s, %s)",
            (term, doc_freq)
        )

        print(f"[INFO] Term '{term}' indexed (DF={doc_freq})", file=sys.stderr)
    except Exception as insert_err:
        print(f"[ERROR] Insert failed for term '{term}': {insert_err}", file=sys.stderr)

# Processing sorted mapper output from stdin
for raw_line in sys.stdin:
    raw_line = raw_line.strip()
    if not raw_line:
        continue

    try:
        term_part, value_part = raw_line.split("\t")
        doc_id_part, tf_string = value_part.split(":", 1)
        tf_value = int(tf_string)
    except ValueError as parse_err:
        print(f"[WARNING] Skipping malformed line: {raw_line} — {parse_err}", file=sys.stderr)
        continue

    if active_term is None:
        active_term = term_part

    if term_part != active_term:
        save_term_to_cassandra(active_term, term_postings)
        term_postings = {}
        active_term = term_part

    term_postings[doc_id_part] = term_postings.get(doc_id_part, 0) + tf_value

# Final flush
if active_term and term_postings:
    save_term_to_cassandra(active_term, term_postings)

# Store document length statistics
try:
    for doc_id, total_terms in doc_term_lengths.items():
        cass_session.execute(
            "INSERT INTO doc_stats (doc_id, length) VALUES (%s, %s)",
            (doc_id, total_terms)
        )
        print(f"[INFO] Saved doc length: {doc_id} => {total_terms}", file=sys.stderr)
except Exception as doc_stats_err:
    print(f"[ERROR] Failed to write doc_stats: {doc_stats_err}", file=sys.stderr)

cass_cluster.shutdown()
print("[INFO] Cassandra session closed.", file=sys.stderr)

