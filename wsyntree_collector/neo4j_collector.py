
import functools
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from multiprocessing import Queue, Manager
# from multiprocessing.pool import ThreadPool, Pool
import concurrent.futures as futures
import os

import pygit2 as git
from tqdm import tqdm
from pebble import ProcessPool, ThreadPool

from wsyntree import log
from wsyntree.tree_models import (
    SCM_Host, WSTRepository, WSTFile, WSTNode, WSTText
)
from wsyntree.localstorage import LocalCache
from wsyntree.utils import list_all_git_files, pushd

from .neo4j_collector_worker import _process_file, _tqdm_node_receiver


class WST_Neo4jTreeCollector():
    def __init__(self, repo_url: str, *, workers: int = None):
        self.repo_url = repo_url
        # self.database_conn_str = database_conn

        pr = urlparse(self.repo_url)
        self._url_scheme = pr.scheme
        self._url_hostname = pr.hostname
        self._url_path = pr.path[1:]

        self._tree_scm_host = None
        self._tree_repo = None

        self._worker_count = workers or os.cpu_count()
        self._mp_manager = None
        self._node_queue = None

    ### NOTE private control functions:

    def _get_git_repo(self):
        repodir = self._local_repo_path
        if not repodir.exists():
            repodir.mkdir(mode=0o770, parents=True)
            log.debug(f"{self} cloning repo...")
            return git.clone_repository(
                self.repo_url,
                repodir.resolve()
            )
        else:
            repopath=git.discover_repository(
                repodir.resolve()
            )
            return git.Repository(repopath)


    ### NOTE immutable properties

    def __repr__(self):
        return f"WST_Neo4jTreeCollector<{self.repo_url}>"
    __str__ = __repr__

    @functools.cached_property
    def _local_repo_path(self):
        cachedir = LocalCache.get_local_cache_dir() / 'collector_repos'
        if not cachedir.exists():
            cachedir.mkdir(mode=0o770)
            log.debug(f"created dir {cachedir}")
        return cachedir.joinpath(self._url_path)

    @functools.cached_property
    def _current_commit_hash(self):
        return self._get_git_repo().revparse_single('HEAD').hex

    ### NOTE public control functions

    def delete_all_tree_data(self):
        """Delete all data in the tree associated with this repo object"""
        # TODO
        raise NotImplementedError()

    def collect_all(self):
        """Creates every node down the tree for this repo"""
        # create the main Repos
        nr = WSTRepository(
            type='git',
            url=self.repo_url,
            analyzed_commit=self._current_commit_hash,
            path=self._url_path,
        )
        nr.save()
        self._tree_repo = nr
        log.debug(f"{nr} is hosted on {self._tree_scm_host}")
        nr.host.connect(self._tree_scm_host)

        # file-level processing
        file_paths = []
        with pushd(self._local_repo_path), Manager() as self._mp_manager:
            self._node_queue = self._mp_manager.Queue()
            node_receiver = _tqdm_node_receiver(self._node_queue)
            with ProcessPool(max_workers=self._worker_count) as executor:
                self._stoppable = executor
                log.info(f"scanning git for files ...")
                ret_futures = []
                for p in tqdm(list_all_git_files(self._get_git_repo())):
                    file_paths.append(p)
                    ret_futures.append(executor.schedule(
                        _process_file,
                        (p, self._tree_repo),
                        {'node_q': self._node_queue}
                    ))
                log.info(f"processing files with {self._worker_count} workers ...")
                try:
                    for r in tqdm(futures.as_completed(ret_futures), total=len(ret_futures), desc="processing files"):
                        nf = r.result()
                        # log.debug(f"added {nf}")
                except KeyboardInterrupt as e:
                    log.warn(f"stopping collection ...")
                    for rf in ret_futures:
                        rf.cancel()
                    executor.close()
                    executor.join(5)
                    executor.stop()
                    self._node_queue.put(None)
                    # raise e
            self._node_queue.put(None)

    def setup(self):
        """Clone the repo, connect to the DB, create working directories, etc."""
        # self._connect_db()
        self._get_git_repo()
        self._tree_scm_host = SCM_Host.get_or_create({'host': self._url_hostname})[0]
        # self._tree_scm_host = SCM_Host.match(self.graph, self._url_hostname).first()
