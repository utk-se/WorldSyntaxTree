
import functools
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from multiprocessing import Queue, Manager
# from multiprocessing.pool import ThreadPool, Pool
import concurrent.futures as futures
import os
import time

import pygit2 as git
from tqdm import tqdm
from pebble import ProcessPool, ThreadPool

from wsyntree import log
from wsyntree.tree_models import (
    WSTRepository, WSTFile, WSTNode, WSTText
)
from wsyntree.localstorage import LocalCache
from wsyntree.utils import list_all_git_files, pushd


class WST_ArangoTreeCollector():
    def __init__(
            self,
            repo_url: str,
            *,
            database_conn: str = "http://wst:wst@localhost:8529/wst",
            workers: int = None
        ):
        """
        database_conn: Full URI including user:password@host:port/database
        """
        self.repo_url = repo_url
        self.database_conn_str = database_conn

        pr = urlparse(self.repo_url)
        self._url_scheme = pr.scheme
        self._url_hostname = pr.hostname
        self._url_path = pr.path[1:]

        ur = urlparse(self.database_conn_str)
        self._db_scheme = ur.scheme or 'http'
        self._db_hostname = ur.hostname or 'localhost'
        self._db_port = ur.port or 8529
        self._db_username = ur.username or 'wst'
        self._db_password = ur.password or 'wst'
        self._db_database = ur.path[1:] or 'wst'
        self._coll = {} # collections
        self._graph = {}

        self._tree_repo = None

        self._worker_count = workers or os.cpu_count()
        self._mp_manager = None
        self._node_queue = None

    ### NOTE private control functions:

    def _connect_db(self):
        self._client = client = ArangoClient(hosts=strip_url(self.database_conn_str))
        self._db = self._client.db(
            self._db_database,
            username=self._db_username,
            password=self._db_password,
        )

        clls = [
            'wstfiles', 'wstrepos', 'wstnodes', 'wsttexts',
            'wst-fromfile', 'wst-fromrepo', 'wst-nodeparent', 'wst-nodetext'
        ]
        for cn in clls:
            self._coll[cn] = db.collection(cn)
        graphs = ["wst-repo-files", "wst-file-nodes", "wst-node-parents", "wst-node-text"]
        for cn in graphs:
            self._graph[cn] = db.graph(cn)

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
        return f"WST_ArangoTreeCollector<{self.repo_url}@{self._current_commit_hash}>"
    __str__ = __repr__

    @functools.cached_property
    def _local_repo_path(self):
        cachedir = LocalCache.get_local_cache_dir() / 'collector_repos'
        if not cachedir.exists():
            cachedir.mkdir(mode=0o770)
            log.debug(f"created dir {cachedir}")
        return cachedir.joinpath(self._url_path)

    @functools.cached_property
    def _current_commit_hash(self) -> str:
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
            _key=self._current_commit_hash,
            type='git',
            url=self.repo_url,
            commit=self._current_commit_hash,
            path=self._url_path,
            analyzed_time=int(time.time()),
        )
        # nr.save()
        self._tree_repo = nr
        # log.debug(f"{nr} is hosted on {self._tree_scm_host}")
        # nr.host.connect(self._tree_scm_host)
        self._colls['wstrepos'].insert(nr)

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
        self._connect_db()
        self._get_git_repo()
