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
from collections import Counter, namedtuple, deque
from concurrent import futures

import networkx as nx
from networkx.readwrite import json_graph
import orjson
import pebble
import tenacity
from pebble import concurrent
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from pympler import tracker

import wsyntree.exceptions
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
errors_dir = output_root_dir / "errors"
errors_dir.mkdir(exist_ok=True)

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

def run_blob(blob, content, filenames):
    """"""
    #blob, content, filenames = blob_pair
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

        with outfile.open('wb') as f:
            for nh in hashed_nodes:
                f.write(orjson.dumps({
                    "sha512": nh._get_sha512_hex(),
                    "blob": blob,
                    "x1": nh._node_props['x1'],
                    "x2": nh._node_props['x2'],
                    "y1": nh._node_props['y1'],
                    "type": nh._node_props['type'],
                    "lang": tsabl.lang,
                }, option=orjson.OPT_APPEND_NEWLINE))

    log.debug(f"{blob}: finished")
    return (blob, len(hashed_nodes), outfile)

@concurrent.process(name="wst-nhv1-blobfuncs")
#@concurrent.thread(name="wst-nhv1-blobfuncs")
# @tenacity.retry( # retry here is not working: the C-level 'munmap' error exits the whole python process
#     retry=tenacity.retry_if_exception_type(pebble.common.ProcessExpired),
#     stop=tenacity.stop_after_attempt(5),
#     reraise=True,
# )
def _rb(*a, **kwa):
    try:
        return run_blob(*a, **kwa)
        #return None
    except wsyntree.exceptions.RootTreeSitterNodeIsError as e:
        log.debug(f"skip {a[0]}: {e}")
        return (str(e), traceback.format_exc())
    except Exception as e:
        log.trace(log.error, traceback.format_exc())
        log.error(f"{e}")
        raise e

blobjob = namedtuple("BlobJob", ["result", "args", "kwargs", "retry"])

def record_failure(job, e):
    fail_file = (errors_dir / f"{job.args[0]}.txt")
    with fail_file.open("at") as f:
        f.write(f"JOB FAILURE REPORT {job.args[0]}:\n")
        f.write(f"Current traceback:\n")
        f.write(traceback.format_exc())
        f.write("\n")
        f.write(str(e))
    log.warn(f"Wrote failure report to {fail_file}")
    return fail_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser("WST Collector NHV1 BlobFuncs v0")
    parser.add_argument("-w", "--workers", type=int, default=4)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)

    log.info(f"pool workers {args.workers}")
    q = deque(maxlen=args.workers)
    try:
        blobs = tqdm(all_blobs_iterator(
            blobfilter=blob_not_yet_processed,
            filefilter=lambda f: wst_supports_fnames(f)
        ))
        tr = tracker.SummaryTracker()
        def collect_or_retry(job: blobjob):
            q.remove(job)
            try:
                job.result.result()
            except pebble.common.ProcessExpired as e:
                if job.retry > 5:
                    record_failure(job, e)
                    return
                log.error(f"{job.args[0]} attempt {job.retry} failed: {e}")
                q.append(blobjob(
                    _rb(*job.args),
                    job.args,
                    {},
                    job.retry + 1,
                ))

        with logging_redirect_tqdm(), log.suppress_stdout():
            #log.logger.addHandler(log.OutputHandler(tqdm.write))
            for blob_pair in blobs:
                if len(q) >= args.workers:
                    # block until slots available
                    done, not_done = futures.wait([j.result for j in q], return_when=futures.FIRST_COMPLETED)
                    done = list(done)
                    for job in list(q): # done jobs
                        if job.result in done:
                            collect_or_retry(job)
                            done.remove(job.result)
                    assert len(done) == 0

                jargs = [*blob_pair]
                q.append(blobjob(_rb(*jargs), jargs, {}, 0))
            # finish up:
            for job in list(q):
                collect_or_retry(job)
    except KeyboardInterrupt as e:
        log.warn(f"Caught KeyboardInterrupt, stopping ...")
        tr.print_diff()
        for j in q:
            j.result.cancel()
        tr.print_diff()
