
import argparse
import multiprocessing
from multiprocessing import Pool
import sys
import os
import psutil
from urllib.parse import urlparse
import time
import signal
import traceback
from pathlib import Path

import pygit2 as git
from arango import ArangoClient
import enlighten
import bpdb

from wsyntree import log, multiprogress
from wsyntree.exceptions import *
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.utils import strip_url, desensitize_url
import wsyntree.tree_models as tree_models
from wsyntree.tree_models import (
    WSTRepository, _db_collections, _db_edgecollections, _graph_edge_definitions
)

from .jsonl_writer import WST_FileExporter, write_from_queue
# from .arango_collector import WST_ArangoTreeCollector
from .jsonl_collector import WST_JSONLCollector
from .batch_analyzer import set_batch_analyze_args


def analyze(args):

    pr = urlparse(args.repo_url)

    multiprogress.main_proc_setup()
    multiprogress.start_server_thread()
    en_manager_proxy = multiprogress.get_manager_proxy()
    en_manager = multiprogress.get_manager()

    with multiprocessing.Manager() as mp_manager:
        export_q = mp_manager.Queue(200)
        # exporter = WST_FileExporter(output_path, delete_existing=True)
        collector = WST_JSONLCollector(
            args.repo_url,
            export_q=export_q,
            workers=args.workers,
            commit_sha=args.target_commit,
            en_manager=en_manager,
        )
        collector.setup()
        log.debug(f"Set up collector: {collector}")

        output_path = args.output_dir or Path(f"output/{pr.path[1:]}/{collector.get_commit_hash()}")
        if args.skip_exists and output_path.exists() and output_path.glob("*.jsonl"):
            log.warn(f"Skipping collection: output dir {output_path} already exists")
            return
        elif not args.overwrite and output_path.exists() and output_path.glob("*.jsonl"):
            log.error(f"Output already exists: {output_path}, to overwrite use --overwrite")
            raise FileExistsError(f"Output dir already present: {output_path}")
        export_proc = write_from_queue(
            export_q,
            en_manager_proxy,
            output_path,
            delete_existing=args.overwrite,
        )

        if args.interactive_debug:
            log.warn("Starting debugging:")
            bpdb.set_trace()

        try:
            collector.collect_all()
        except RepoExistsError as e:
            if args.skip_exists:
                log.warn(f"Skipping collection since repo document already present for commit {collector._current_commit_hash}")
                return
            else:
                raise
        except Exception as e:
            log.crit(f"{collector} run failed.")
            raise e
        finally:
            export_q.put(None)
            export_proc.result()

def delete(args):
    if '/' in args.which_repo:
        # it's a URL/URI
        collector = WST_ArangoTreeCollector(
            args.which_repo,
            database_conn=args.db,
        )
    else:
        # find by commit
        collector = WST_ArangoTreeCollector(
            None, # this collector only used to delete
            commit_sha=args.which_repo,
        )
    collector.delete_all_tree_data()

def database_init(args):
    client = ArangoClient(hosts=strip_url(args.db))
    p = urlparse(args.db)
    odb = client.db(p.path[1:], username=p.username, password=p.password)

    if args.delete:
        log.warn(f"deleting all data ...")
        # deleting old stuff could take awhile
        jobs = []
        db = odb.begin_async_execution()

        jobs.append(db.delete_graph(tree_models._graph_name, ignore_missing=True))
        for c in _db_collections:
            jobs.append(db.delete_collection(c, ignore_missing=True))
        for c in _db_edgecollections:
            jobs.append(db.delete_collection(c, ignore_missing=True))

        jt_wait = len(jobs)
        while len(jobs) > 0:
            time.sleep(1)
            for j in jobs:
                if j.status() == 'done':
                    jobs.remove(j)
            if jt_wait != len(jobs):
                log.debug(f"delete: waiting on {len(jobs)} jobs to finish ...")
                jt_wait = len(jobs)

    # back to non-async
    db = odb

    log.info(f"Creating collections ...")

    colls = {}
    for cn in _db_collections:
        if db.has_collection(cn):
            colls[cn] = db.collection(cn)
        else:
            colls[cn] = db.create_collection(cn, user_keys=True)
    for cn in _db_edgecollections:
        if db.has_collection(cn):
            colls[cn] = db.collection(cn)
        else:
            colls[cn] = db.create_collection(cn, user_keys=True, edge=True)

    graph = None
    if not db.has_graph(tree_models._graph_name):
        graph = db.create_graph(tree_models._graph_name)
    else:
        graph = db.graph(tree_models._graph_name)
    edgedefs = {}

    for gk, gv in _graph_edge_definitions.items():
        if not graph.has_edge_definition(gv['edge_collection']):
            log.debug(f"Added graph edges {gv}")
            edgedefs[gk] = graph.create_edge_definition(**gv)

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db", "--database",
        type=str,
        help="Database connection string",
        default=os.environ.get('WST_DB_URI', "http://wst:wst@localhost:8529/wst")
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    parser.set_defaults(
        en_manager=enlighten.get_manager()
    )
    subcmds = parser.add_subparsers(
        title="Collector commands"
    )

    # analysis
    cmd_analyze = subcmds.add_parser(
        'analyze', aliases=['analyze', 'a'], help="Analyze repositories to file output")
    cmd_analyze.set_defaults(func=analyze)
    cmd_analyze.add_argument(
        "repo_url",
        type=str,
        help="URI for cloning the repository",
    )
    cmd_analyze.add_argument(
        "-w", "--workers",
        type=int,
        help="Number of workers to use for processing files, default: os.cpu_count()",
        default=None,
    )
    cmd_analyze.add_argument(
        "-o", "--output-dir",
        type=Path,
        help="Path to write result files to",
        default=None,
    )
    cmd_analyze.add_argument(
        "--skip-exists", "--skip-existing",
        action="store_true",
        help="Skip the analysis if the output dir already exists and contains data",
    )
    cmd_analyze.add_argument(
        "--interactive-debug",
        action="store_true",
        help="Start the interactive debugger after repo setup",
    )
    cmd_analyze.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete any existing files in the output dir before starting",
    )
    cmd_analyze.add_argument(
        "-t", "--target-commit",
        type=str,
        help="Checkout and analyze a specific commit from the repo",
        default=None,
    )
    # batch analysis
    cmd_batch = subcmds.add_parser(
        'batch', aliases=['addbatch', 'addmulti'],
        help="Analyze multiple repos from a JSON specification list"
    )
    set_batch_analyze_args(cmd_batch)
    # delete data selectively
    cmd_delete = subcmds.add_parser(
        'delete', aliases=['del'], help="Delete tree data selectively")
    cmd_delete.set_defaults(func=delete)
    cmd_delete.add_argument(
        "which_repo",
        type=str,
        help="URI or commit SHA for which repo's data to delete"
    )
    # db setup
    cmd_db = subcmds.add_parser(
        'db', aliases=['database'], help="Manage the database")
    subcmds_db = cmd_db.add_subparsers(title="Manage the database")
    cmd_db_init = subcmds_db.add_parser(
        'initialize', aliases=['init', 'setup'], help="Set up the database")
    cmd_db_init.set_defaults(func=database_init)
    cmd_db_init.add_argument(
        "-d", "--delete",
        help="Delete any existing data in the database",
        action="store_true",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    log.info(f"DB connection: {desensitize_url(args.db)}")

    if 'func' not in args:
        log.warn(f"Please supply a valid subcommand!")
        return

    try:
        args.func(args)
    except KeyboardInterrupt as e:
        log.warn(f"Stopping all child processes...")
        cur_proc = psutil.Process()
        children = cur_proc.children(recursive=True)
        for c in children:
            os.kill(c.pid, signal.SIGINT)
        psutil.wait_procs(children, timeout=5)
        children = cur_proc.children(recursive=True)
        for c in children:
            c.terminate()
        raise e

if __name__ == '__main__':
    os.setpgrp()
    __main__()
