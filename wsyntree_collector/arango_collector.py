
import functools
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from multiprocessing import Queue, Manager
# from multiprocessing.pool import ThreadPool, Pool
import concurrent.futures as futures
import os
import time

from arango import ArangoClient
import pygit2 as git
from pebble import ProcessPool, ThreadPool
import enlighten

from wsyntree import log, tree_models
from wsyntree.tree_models import * # __all__
from wsyntree.localstorage import LocalCache
from wsyntree.utils import (
    list_all_git_files, pushd, strip_url, sha1hex, chunkiter
)
from .arango_collector_worker import _tqdm_node_receiver, process_file


class WST_ArangoTreeCollector():
    def __init__(
            self,
            repo_url: str,
            *,
            database_conn: str = "http://wst:wst@localhost:8529/wst",
            workers: int = None,
            commit_sha: str = None,
            en_manager = None,
        ):
        """
        database_conn: Full URI including user:password@host:port/database
        commit_sha: full sha1 hex commit, optional, if present will checkout
        workers: number of file processes in parallel
        """
        self.repo_url = repo_url
        self.database_conn_str = database_conn
        self.en_manager = en_manager or enlighten.get_manager()

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
        self._vert_colls = {}
        # unconnected:
        self._db = None
        self._graph = None

        self._target_commit = commit_sha
        self._tree_repo = None

        self._worker_count = workers or os.cpu_count()
        self._mp_manager = None
        self._node_queue = None

    ### NOTE private control functions:

    def _connect_db(self):
        self._client = client = ArangoClient(
            hosts=strip_url(self.database_conn_str),
        )
        self._db = self._client.db(
            self._db_database,
            username=self._db_username,
            password=self._db_password,
        )

        vert_colls = ['wstfiles', 'wstrepos', 'wstnodes', 'wsttexts']
        edge_colls = ['wst-fromfile', 'wst-fromrepo', 'wst-nodeparent', 'wst-nodetext']
        clls = vert_colls + edge_colls
        for cn in clls:
            self._coll[cn] = self._db.collection(cn)
        self._graph = self._db.graph(tree_models._graph_name)

        for ecn in vert_colls:
            self._vert_colls[ecn] = self._graph.vertex_collection(ecn)

    def _get_git_repo(self):
        repodir = self._local_repo_path
        if not (repodir / '.git').exists():
            repodir.mkdir(mode=0o770, parents=True, exist_ok=True)
            log.debug(f"cloning repo to {repodir} ...")
            return git.clone_repository(
                self.repo_url,
                repodir.resolve()
            )
        else:
            repopath=git.discover_repository(
                repodir.resolve()
            )
            return git.Repository(repopath)

    @property
    def _current_commit(self) -> git.Commit:
        try:
            return self._get_git_repo().revparse_single('HEAD')
        except KeyError as e:
            log.error(f"repo in {self._local_repo_path} might not have HEAD?")

    @property
    def _current_commit_hash(self) -> str:
        return self._current_commit.hex

    ### NOTE immutable properties

    def __repr__(self):
        repo_url = str(self.repo_url)
        hash = str(self._current_commit_hash)
        return f"WST_ArangoTreeCollector<{repo_url}@{hash}>"
    __str__ = __repr__

    @functools.cached_property
    def _local_repo_path(self):
        cachedir = LocalCache.get_local_cache_dir() / 'collector_repos'
        if not cachedir.exists():
            cachedir.mkdir(mode=0o770, exist_ok=True)
            log.debug(f"created dir {cachedir}")
        return cachedir.joinpath(self._url_path)

    ### NOTE public control functions

    def delete_all_tree_data(self):
        """Delete all data in the tree associated with this repo object"""

        raise NotImplementedError(f"Deletion not updated to support new tree structure.")

        if not self._db:
            self._connect_db()
        if not self._tree_repo:
            log.info(f"Attempting to fetch WSTRepository document from db...")
            if self._target_commit:
                self._tree_repo = WSTRepository.get(self._db, self._target_commit)
            else:
                self._tree_repo = WSTRepository.get(self._db, self._current_commit_hash)
        if self._tree_repo is None:
            raise RuntimeError(f"Repo does not exist in db.")
        if self._tree_repo.wst_status != "deleting":
            log.info(f"Repo status {self._tree_repo.wst_status} -> deleting")
            self._tree_repo.wst_status = "deleting"
            self._tree_repo.update_in_db(self._db)
        else:
            log.info(f"Resuming deletion ...")

        nodechunksize = 1000
        files = WSTFile.iterate_from_parent(self._db, self._tree_repo)
        for f in files:
            nodes = WSTNode.iterate_from_parent(self._db, f, return_inflated=False)
            for chunk in chunkiter(nodes, nodechunksize):
                with self._db.begin_batch_execution() as bdb:
                    graph = bdb.graph(tree_models._graph_name)
                    vertcoll = graph.vertex_collection(WSTNode._collection)
                    log.debug(f"Deleting {len(chunk)} nodes of {f.path}")
                    for n in chunk:
                        vertcoll.delete(n)
            log.debug(f"Delete file {f.path}")
            self._vert_colls[WSTFile._collection].delete(f._key)
        log.debug(f"Deleted files & nodes")
        self._vert_colls[WSTRepository._collection].delete(self._tree_repo._key)
        log.info(f"Deleted repo {self._tree_repo.url} @ {self._tree_repo.commit}")
        self._tree_repo = None

    def collect_all(self, existing_node_q = None):
        """Creates every node down the tree for this repo"""
        # create the main Repos
        self._tree_repo = WSTRepository(
            type='git',
            url=self.repo_url,
            path=self._url_path,
            analyzed_time=int(time.time()),
            wst_status="started",
        )
        # self._coll['wstrepos'].insert(nr.__dict__)
        self._tree_repo.insert_in_db(self._db)

        # attempt to find an existing commit in the db:
        if not (commit := WSTCommit.get(self._db, self._current_commit_hash)):
            _cc = self._current_commit
            self._wst_commit = WSTCommit(
                _key=_cc.hex,
                commit_time=_cc.commit_time,
                commit_time_offset=_cc.commit_time_offset,
                parent_ids=[str(i) for i in _cc.parent_ids],
                tree_id=str(_cc.tree_id),
            )
            log.debug(f"Inserting {self._wst_commit}")
            self._wst_commit.insert_in_db(self._db)
        else:
            self._wst_commit = commit
        rel_repo_commit = self._tree_repo / self._wst_commit
        rel_repo_commit.insert_in_db(self._db)

        index = self._get_git_repo().index
        index.read()

        # file-level processing
        # files = []
        with pushd(self._local_repo_path), Manager() as self._mp_manager:
            if not existing_node_q:
                self._node_queue = self._mp_manager.Queue()
                node_receiver = _tqdm_node_receiver(self._node_queue)
            else:
                self._node_queue = existing_node_q
            with ProcessPool(max_workers=self._worker_count) as executor:
                self._stoppable = executor
                log.info(f"scanning git for files ...")
                ret_futures = []
                cntr_add_jobs = self.en_manager.counter(
                    desc=f"scanning files for {self._url_path}", total=len(index),
                )
                for gobj in index:
                    if not os.path.isfile(gobj.path):
                        continue
                    _file = Path(gobj.path)
                    # check size of file first:
                    _fstat = _file.stat()

                    nf = WSTFile(
                        # _key=f"{nr.commit}-{gobj.hex}-{sha1hex(gobj.path)}",
                        path=gobj.path,
                        mode=gobj.mode,
                        size=_fstat.st_size,
                        git_oid=gobj.hex,
                    )
                    # file_paths.append(p)
                    ret_futures.append(executor.schedule(
                        process_file,
                        (nf, self._wst_commit, self.database_conn_str),
                        {'node_q': self._node_queue, 'en_manager': self.en_manager}
                    ))
                    cntr_add_jobs.update()
                cntr_add_jobs.close()
                log.info(f"processing files with {self._worker_count} workers ...")
                try:
                    cntr_files_processed = self.en_manager.counter(
                        desc=f"processing {self._url_path}",
                        total=len(ret_futures), unit="files",
                        leave=False,
                    )
                    for r in futures.as_completed(ret_futures):
                        completed_file = r.result()
                        # log.debug(f"result {nf}")
                        cntr_files_processed.update()
                    # after all results returned
                    self._tree_repo.wst_status = "completed"
                    self._tree_repo.update_in_db(self._db)
                except KeyboardInterrupt as e:
                    log.warn(f"stopping collection ...")
                    for rf in ret_futures:
                        rf.cancel()
                    executor.close()
                    executor.join(5)
                    executor.stop()
                    # raise e
                    self._tree_repo.wst_status = "cancelled"
                    self._tree_repo.update_in_db(self._db)
                except Exception as e:
                    self._tree_repo.wst_status = "error"
                    self._tree_repo.update_in_db(self._db)
                    raise e
                finally:
                    cntr_files_processed.close()
                    if not existing_node_q:
                        self._node_queue.put(None)
            if not existing_node_q:
                self._node_queue.put(None)
                receiver_exit = node_receiver.result(timeout=3)

    def setup(self):
        """Clone the repo, connect to the DB, create working directories, etc."""
        self._connect_db()
        repo = self._get_git_repo()
        # to _target_commit if set
        if self._target_commit and self._target_commit != self._current_commit_hash:
            log.info(f"Checking out commit {self._target_commit}...")
            try:
                commit = repo.get(self._target_commit)
                log.debug(f"target commit {commit}")
                # commit might not exist for a variety of reasons (need to fetch, DNE, corrupt, etc)
                repo.checkout_tree(commit.tree)
                repo.head.set_target(commit.id)
            except Exception as e:
                raise
            log.info(f"Repo at {self._local_repo_path} now at {self._current_commit_hash}")
        elif self._target_commit and self._target_commit == self._current_commit_hash:
            log.info(f"Repo in {self._local_repo_path} is already at {self._target_commit}")
