#!/usr/bin/env python3
import os
BASE = "/Users/moyong/project/ai/tsdb/tsdb2/crates"
def wf(rel_path, content):
    path = os.path.join(BASE, rel_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)
    print(f"wrote {path}")
