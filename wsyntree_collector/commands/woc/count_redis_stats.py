"""
This "command" is only to be run as a job on the DA cluster, not used as a subcommand.
"""

import argparse
import subprocess
import traceback
import time
import tempfile
import enum
import os
from typing import List, Optional
from pathlib import Path, PurePath, PurePosixPath
from collections import Counter, namedtuple, deque
from concurrent import futures
from itertools import islice

import networkx as nx
from networkx.readwrite import json_graph
import orjson
import pebble
import tenacity
from pebble import concurrent
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from pympler import tracker
import redis

import wsyntree.exceptions
from wsyntree import log
from wsyntree.wrap_tree_sitter import (
    TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator, get_TSABL_for_file,
)
from wsyntree.hashtypes import WSTNodeHashV1

from wsyntree_collector.file.parse_file_treesitter import build_networkx_graph
from wsyntree_collector.wociterators import all_blobs as all_blobs_iterator, BlobStatus

#import oscar


tqdm_smoothing_factor = 0.01

redis_pool = redis.ConnectionPool(
    host=os.environ.get("WST_REDIS_HOST", "wst-redis"),
    port=6379, db=0,
)
redis_client = redis.Redis(connection_pool=redis_pool)
redis_decoded = redis.Redis(connection_pool=redis_pool, decode_responses=True)

def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch

if __name__ == "__main__":
    parser = argparse.ArgumentParser("WST Collector NHV1 BlobFuncs v0")
    # parser.add_argument("-w", "--workers", type=int, default=4)
    parser.add_argument("-v", "--verbose", action="store_true")
    # parser.add_argument("--prescan", help="Scan output dir before attempting new blobs", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)

    counter = Counter()
    try:
        with logging_redirect_tqdm(), log.suppress_stdout():
            key_it = tqdm(redis_decoded.scan_iter(count=5000), unit_scale=True)
            for batch in batched(key_it, 5000):
                #log.debug(f"batch size {len(batch)}")
                vals = redis_decoded.mget(batch)
                counter.update(vals)
    except KeyboardInterrupt as e:
        log.warn(f"Caught KeyboardInterrupt, stopping ...")
    for m in counter.most_common():
        print(m)
