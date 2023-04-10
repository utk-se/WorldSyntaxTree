"""
This "command" is only to be run as a job on the DA cluster, not used as a subcommand.
"""

import argparse
import subprocess
import traceback
import time
import tempfile
from typing import List, Optional
from pathlib import Path, PurePath, PurePosixPath
from collections import Counter, namedtuple

import networkx as nx
from networkx.readwrite import json_graph
import orjson
import pebble

from wsyntree import log
from wsyntree.wrap_tree_sitter import (
    TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator, get_TSABL_for_file,
)
from wsyntree.hashtypes import WSTNodeHashV1

from wsyntree_collector.file.parse_file_treesitter import build_networkx_graph
from wsyntree_collector.wociterators import all_blobs as all_blobs_iterator

#import oscar


getValuesScript = Path("~/lookup/getValues").expanduser()
assert getValuesScript.exists(), f"Expected getValues at {getValuesScript}"
output_root_dir = Path("/da7_data/WorldSyntaxTree/nhv1/blobfuncs_v0")
assert output_root_dir.is_dir(), f"Output directory does not exist."

FUNCDEF_TYPES = (
    "function_definition",
    "function_declaration",
    "method_declaration",
    "method", # ruby
    "function_item", # rust
)

#def get_filenames_for_blob(b: str) -> List[PurePosixPath]:
#    """Inefficient version: calls lookup b2f because I am lazy to rewrite it in python"""
#    c = subprocess.run(
#        ["bash", getValuesScript, "b2f"],
#        input=f"{b}\n",
#        text=True,
#        capture_output=True,
#    )
#    if c.returncode != 0:
#        log.trace(log.warn, c.stdout)
#        log.trace(log.warn, c.stderr)
#    c.check_returncode()
#    lines = c.stdout.strip().split("\n")
#    if len(lines) != 1:
#        log.warn(f"getValues gave {len(lines)} lines for {b}")
#    filenames = []
#    for line in lines:
#        for fname in line.split(";")[1:]:
#            filenames.append(PurePosixPath(fname))
#    return filenames

def wst_supports_fnames(fnames):
    filenames = fnames
    if not filenames:
        return False
    exts = Counter([x.suffix for x in filenames if x.suffix])
    primary_ext = exts.most_common(1)
    if not primary_ext:
        return False
    primary_ext = primary_ext[0][0]
    if get_TSABL_for_file(str(primary_ext)) is not None:
        return True
    #log.debug(f"WST {primary_ext} not supported: {filenames[0]}")
    return False

def output_path_for_blob(b):
    outdir = output_root_dir / b[0:2] / b[2:4]
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / f"{b}.jsonl"

def blob_not_yet_processed(b):
    p = output_path_for_blob(b)
    if p.is_file():
        return False
    return True

def run_blob(blob_pair):
    """"""
    blob, content, filenames = blob_pair
    if not filenames:
        return (None, "NO_FILENAMES")
    exts = Counter([x.suffix for x in filenames if x.suffix])
    primary_ext = exts.most_common(1)
    if not primary_ext:
        return (None, "NO_FILENAME_SUFFIX")
    primary_ext = primary_ext[0][0]
    tsabl = get_TSABL_for_file(str(primary_ext)) # pebble synchronized cache
    #log.debug(f"Start parsing {blob} with {tsabl}")

    with tempfile.TemporaryDirectory(prefix="wst_") as tempdir:
        log.debug(f"{blob}: working in {tempdir}")
        outfile = output_path_for_blob(blob)
        tempdir = Path(tempdir)
        codepath = (tempdir / f"file{primary_ext}")
        with codepath.open("wb") as f:
            f.write(content)
        graph = build_networkx_graph(tsabl, codepath, include_text=False)
        hashed_nodes = []
        for node in nx.dfs_preorder_nodes(graph):
            if graph.nodes[node]['type'] in FUNCDEF_TYPES:
                hashed_nodes.append(WSTNodeHashV1(graph, node))

    log.debug(f"{blob}: finished")
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser("WST Collector NHV1 BlobFuncs v0")
    parser.add_argument("-w", "--workers", type=int, default=4)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)

    with pebble.ThreadPool(max_workers=args.workers) as pool:
        log.info(f"pool workers {args.workers}")
        try:
            blobs = all_blobs_iterator(blobfilter=blob_not_yet_processed, filefilter=lambda f: wst_supports_fnames(f))
            def _rb(*a, **kwa):
                try:
                    run_blob(*a, **kwa)
                except Exception as e:
                    log.trace(log.error, traceback.format_exc())
                    log.error(f"{e}")
            results = pool.map(_rb, blobs)
            pool.join()
        except KeyboardInterrupt as e:
            log.warn(f"Caught KeyboardInterrupt, stopping pool...")
            pool.close()
            pool.stop()
            pool.join()

        try:
            for res in results.result():
                pass
        except Exception as e:
            log.trace(log.error, traceback.format_exc())
            log.error(f"{e}")
