#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    return re.findall(r"\w+", text.lower())

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        doc_id, doc_title, doc_text = line.split("\t", 2)
        tokens = tokenize(doc_text)
        for token in tokens:
            sys.stdout.write(f"{token}\t{doc_id}:1\n")
    except ValueError:
        # Log malformed lines to stderr
        sys.stderr.write(f"Skipping malformed line: {line}\n")

