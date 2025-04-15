### app/mapreduce/mapper2.py
#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    return re.findall(r"\b\w+\b", text.lower())

for line in sys.stdin:
    try:
        doc_id, title, content = line.strip().split("\t", 2)
        words = tokenize(content)
        for _ in words:
            print(f"{doc_id}\t1")
    except Exception:
        continue

