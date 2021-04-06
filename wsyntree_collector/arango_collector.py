
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
from tqdm import tqdm
from pebble import ProcessPool, ThreadPool

from wsyntree import log
from wsyntree.tree_models import * # __all__
from wsyntree.localstorage import LocalCache
from wsyntree.utils import list_all_git_files, pushd, strip_url, sha1hex
from .arango_collector_worker import _tqdm_node_receiver, process_file


class WST_ArangoTreeCollector():
    def __init__(
            self,
            repo_url: str,
            *,
            database_conn: str = "http://wst:wst@localhost:8529/wst",
            workers: int = None,
            commit_sha: str = None,
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
        self._db = None # unconnected

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

        clls = [
            'wstfiles', 'wstrepos', 'wstnodes', 'wsttexts',
            'wst-fromfile', 'wst-fromrepo', 'wst-nodeparent', 'wst-nodetext'
        ]
        for cn in clls:
            self._coll[cn] = self._db.collection(cn)
        graphs = ["wst-repo-files", "wst-file-nodes", "wst-node-parents", "wst-node-text"]
        for cn in graphs:
            self._graph[cn] = self._db.graph(cn)

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

    ### NOTE immutable properties

    def __repr__(self):
        return f"WST_ArangoTreeCollector<{self.repo_url}@{self._current_commit_hash}>"
    __str__ = __repr__

    @functools.cached_property
    def _local_repo_path(self):
        cachedir = LocalCache.get_local_cache_dir() / 'collector_repos'
        if not cachedir.exists():
            cachedir.mkdir(mode=0o770, exist_ok=True)
            log.debug(f"created dir {cachedir}")
        return cachedir.joinpath(self._url_path)

    @functools.cached_property
    def _current_commit_hash(self) -> str:
        try:
            return self._get_git_repo().revparse_single('HEAD').hex
        except KeyError as e:
            log.error(f"repo in {self._local_repo_path} might not have HEAD?")
            raise e

    ### NOTE public control functions

    def delete_all_tree_data(self):
        """Delete all data in the tree associated with this repo object"""
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

        files = WSTFile.iterate_from_parent(self._db, self._tree_repo)
        for f in files:
            log.debug(f"Delete file {f.path}")
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
            wst_status="started",
        )
        self._tree_repo = nr
        # self._coll['wstrepos'].insert(nr.__dict__)
        nr.insert_in_db(self._db)

        # file-level processing
        files = []
        if self._worker_count == 1:
            with pushd(self._local_repo_path):
                index = self._get_git_repo().index
                index.read()
                for gobj in tqdm(index, desc="scanning git"):
                    if not os.path.isfile(gobj.path):
                        continue
                    nf = WSTFile(
                        _key=f"{nr.commit}-{gobj.hex}-{sha1hex(gobj.path)}",
                        path=gobj.path,
                        oid=gobj.hex,
                    )
                    files.append(nf)
                log.info(f"{len(files)} to process")
                for file in files:
                    try:
                        r = process_file(file, self._tree_repo, self.database_conn_str)
                        log.debug(f"{file.path} processing done: {r}")
                    except Exception as e:
                        log.err(f"during {file.path}, document {file._key}")
                        raise e
                self._tree_repo.wst_status = "completed"
                self._tree_repo.update_in_db(self._db)
                return
        with pushd(self._local_repo_path), Manager() as self._mp_manager:
            self._node_queue = self._mp_manager.Queue()
            node_receiver = _tqdm_node_receiver(self._node_queue)
            with ProcessPool(max_workers=self._worker_count) as executor:
                self._stoppable = executor
                log.info(f"scanning git for files ...")
                ret_futures = []
                index = self._get_git_repo().index
                index.read()
                for gobj in tqdm(index):
                    if not os.path.isfile(gobj.path):
                        continue
                    nf = WSTFile(
                        _key=f"{nr.commit}-{gobj.hex}-{sha1hex(gobj.path)}",
                        path=gobj.path,
                        oid=gobj.hex,
                    )
                    # file_paths.append(p)
                    ret_futures.append(executor.schedule(
                        process_file,
                        (nf, self._tree_repo, self.database_conn_str),
                        {'node_q': self._node_queue}
                    ))
                log.info(f"processing files with {self._worker_count} workers ...")
                try:
                    for r in tqdm(futures.as_completed(ret_futures), total=len(ret_futures), desc="processing files"):
                        nf = r.result()
                        # s = str(nf)
                        # log.debug(f"result {nf}")
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
                    self._node_queue.put(None)
            self._node_queue.put(None)

    def setup(self):
        """Clone the repo, connect to the DB, create working directories, etc."""
        self._connect_db()
        self._get_git_repo()
        # TODO checkout self._target_commit
