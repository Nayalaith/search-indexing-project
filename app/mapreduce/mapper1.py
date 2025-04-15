### app/mapreduce/mapper1.py
#!/usr/bin/env python3
import sys
import re

def tokenize(text):
    return re.findall(r"\b\w+\b", text.lower())

for line in sys.stdin:
    try:
        doc_id, title, content = line.strip().split("\t", 2)
        words = tokenize(content)
        for word in words:
            print(f"{word}\t{doc_id}")
    except Exception:
        continue

