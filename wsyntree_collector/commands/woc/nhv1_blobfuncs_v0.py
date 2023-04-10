"""
This "command" is only to be run as a job on the DA cluster, not used as a subcommand.
"""

import argparse
import subprocess
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
    log.debug(f"WST {primary_ext} not supported: {filenames[0]}")
    return False

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
    log.debug(f"Start parsing {blob} with {tsabl}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser("WST Collector NHV1 BlobFuncs v0")
    parser.add_argument("-w", "--workers", type=int, default=4)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)

    blobs = all_blobs_iterator(filefilter=lambda f: wst_supports_fnames(f))

    with pebble.ThreadPool(max_workers=args.workers) as pool:
        pool.map(run_blob, blobs)
