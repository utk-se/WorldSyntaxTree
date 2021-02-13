
import signal

import neo4j
from tqdm import tqdm
import pebble

from neo4j import GraphDatabase

texts = {}

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))

@pebble.sighandler((signal.SIGINT, signal.SIGTERM))
def signal_handler(signum, frame):
    driver.close()

with driver.session() as session:
    r = session.run("MATCH (n:WSTIndexableText) RETURN n")
    for x in tqdm(r):
        # print(x)
        t = x.value()
        # print(t)
        if t['length'] not in texts:
            # print(f"created {t['length']}")
            texts[t['length']] = {}
        if t['text'] not in texts[t['length']]:
            # print(f"created {t['text']}")
            texts[t['length']][t['text']] = 0

        # print(f"{t['length']} increment")
        texts[t['length']][t['text']] += 1

for l, n in tqdm(texts.items()):
    c = sum(n.values())
    print(f"{l}, {len(n)}, {c}")
