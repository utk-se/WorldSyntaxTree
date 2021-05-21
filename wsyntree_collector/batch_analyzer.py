"""
The batch analysis is used for creating the official datasets

Input consists of a list of repos and specific commits to analyze.
All results are written to one db.
"""

import sys
import os
import time
import json
import uuid
from pathlib import Path
from urllib.parse import urlparse
from multiprocessing import Queue, Manager
import concurrent.futures as futures

import pygit2 as git
from arango import ArangoClient
from tqdm import tqdm
from pebble import ProcessPool

from wsyntree import log, multiprogress
from wsyntree.exceptions import *
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.utils import strip_url, desensitize_url
from wsyntree.tree_models import WSTRepository

from .arango_collector import WST_ArangoTreeCollector
from .arango_collector_worker import _tqdm_node_receiver


def set_batch_analyze_args(cmd_batch):
    cmd_batch.set_defaults(func=batch_analyze)
    cmd_batch.add_argument(
        "repo_list_file",
        type=str,
        help="File containing a list of repos and respective commits to analyze"
    )
    cmd_batch.add_argument(
        "-w", "--workers",
        type=int,
        help="Number of workers for a single repo job",
        default=8
    )
    cmd_batch.add_argument(
        "-j", "--jobs",
        type=int,
        help="Number of jobs to run in parallel at one time",
        default=(os.cpu_count() // 8) or 1
    )
    cmd_batch.add_argument(
        "--skip-exists", "--skip-existing",
        action="store_true",
        help="Ignores error of \"repo document already exists in the database\""
    )

def repo_worker(
        repo_dict: dict,
        node_q = None,
        **kwargs, # passed to WST_ArangoTreeCollector constructor
    ):
    """Setup and run one repo's analysis job"""
    collector = WST_ArangoTreeCollector(
        repo_dict['url'],
        commit_sha=repo_dict.get('commit', repo_dict.get('sha')),
        **kwargs
    )
    try:
        collector.setup()
    except Exception as e:
        log.err(f"Failed to setup {collector}: {type(e)}: {e}")
        raise

    # check if exists already
    if repo := WSTRepository.get(collector._db, collector._current_commit_hash):
        raise RepoExistsError(f"Repo document already exists: {repo.__dict__}")

    try:
        collector.collect_all(node_q)
    except Exception as e:
        # log.err(f"Failed to analyze {collector}: {type(e)}: {e}")
        raise e

    return (repo_dict, collector._tree_repo)

def batch_analyze(args):
    repo_list_file = Path(args.repo_list_file)
    if not repo_list_file.exists():
        log.err(f"Input file not found: {args.repo_list_file}")
    try:
        with repo_list_file.open('r') as f:
            repolist = json.load(f)
    except Exception as e:
        log.err(f"Failed to read repo list file")
        raise

    client = ArangoClient(hosts=strip_url(args.db))
    p = urlparse(args.db)
    db = client.db(p.path[1:], username=p.username, password=p.password)
    batch_id = uuid.uuid4().hex
    log.info(f"Batch ID {batch_id}")
    _mp_manager = Manager()
    node_q = _mp_manager.Queue()

    log.debug(f"checking {len(repolist)} items in repo list")

    try:
        multiprogress.main_proc_setup()
        multiprogress.start_server_thread()
        en_manager_proxy = multiprogress.get_manager_proxy()
        en_manager = multiprogress.get_manager()
        node_receiver = _tqdm_node_receiver(node_q, en_manager_proxy)

        with ProcessPool(max_workers=args.jobs) as executor:
            ret_futures = []
            all_repos_sched_cntr = en_manager.counter(
                desc="adding repo jobs", total=len(repolist), unit='repos'
            )
            all_repos_cntr = en_manager.counter(
                desc="repos in batch", total=len(repolist), unit='repos',
                autorefresh=True
            )
            for repo in repolist:
                ret_futures.append(executor.schedule(
                    repo_worker,
                    (repo, node_q),
                    {'workers': args.workers}
                ))
                all_repos_sched_cntr.update()
            all_repos_sched_cntr.close()
            try:
                for r in futures.as_completed(ret_futures):
                    try:
                        repo_dict, tr = r.result()
                    except RepoExistsError as e:
                        if args.skip_exists:
                            log.debug(f"{e}")
                            all_repos_cntr.update()
                            continue
                        else:
                            log.err(f"{e}")
                            raise e
                    # save the original repo data to the db as well:
                    tr.wst_extra = {
                        "wst_batch": batch_id,
                        **repo_dict
                    }
                    tr.update_in_db(db)
                    all_repos_cntr.update()
            except KeyboardInterrupt as e:
                log.warn(f"stopping batch worker pool...")
                executor.stop()
                for rf in ret_futures:
                    rf.cancel()
                log.warn(f"waiting for already started jobs to finish...")
                executor.join()
    finally:
        try:
            node_q.put(None)
            receiver_exit = node_receiver.result(timeout=1)
        except (BrokenPipeError, KeyboardInterrupt) as e:
            pass
