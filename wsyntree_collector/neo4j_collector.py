
import functools
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from multiprocessing.pool import ThreadPool as Pool
import concurrent.futures
import os

import pygit2 as git
from tqdm import tqdm
from pebble import ProcessPool, ThreadPool
from py2neo import Graph, Node, Relationship
from py2neo.ogm import Repository as py2neoRepo

from wsyntree import log
from wsyntree.wrap_tree_sitter import get_TSABL_for_file
from wsyntree.tree_models import (
    SCM_Host, WSTRepository, File, WSTNode, WSTText
)
from wsyntree.localstorage import LocalCache
from wsyntree.utils import list_all_git_files, pushd


class WST_Neo4jTreeCollector():
    def __init__(self, repo_url: str, database_conn: str):
        self.repo_url = repo_url
        self.database_conn_str = database_conn

        pr = urlparse(self.repo_url)
        self._url_scheme = pr.scheme
        self._url_hostname = pr.hostname
        self._url_path = pr.path[1:]

        self._tree_scm_host = None
        self._tree_repo = None

    ### NOTE private control functions:

    def _connect_db(self):
        log.debug(f"connecting neo4j graph ...")
        self.graph = Graph(self.database_conn_str)
        self.neorepo = py2neoRepo.wrap(self.graph)

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

    def _insert_filenode_for_path(self, path: Path):
        nf = Node(
            "File",
            path=str(path)
        )
        nf = File.wrap(nf)
        nf.repo.add(self._tree_repo)
        self.neorepo.save(nf)

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

    def cancel(self):
        if self._stoppable is not None:
            self._stoppable.stop()
        else:
            log.warn(f"nothing to stop")

    def delete_all_tree_data(self):
        """Delete all data in the tree associated with this repo object"""
        # TODO
        raise NotImplementedError()

    def collect_all(self):
        """Creates every node down the tree for this repo"""
        # create the main Repos
        nr = Node("WSTRepository",
            type='git',
            url=self.repo_url,
            analyzed_commit=self._current_commit_hash,
            path=self._url_path,
        )
        nr = WSTRepository.wrap(nr)
        log.debug(f"{nr} is hosted on {self._tree_scm_host}")
        nr.host.add(self._tree_scm_host)
        tx = self.graph.begin()
        tx.merge(nr)
        tx.commit()
        self._tree_repo = nr

        # file-level processing
        file_paths = []
        with pushd(self._local_repo_path):
            with ThreadPool(max_workers=32) as executor:
                self._stoppable = executor
                log.info(f"scanning git for files ...")
                ret_futures = []
                for p in tqdm(list_all_git_files(self._get_git_repo())):
                    file_paths.append(p)
                    ret_futures.append(executor.schedule(
                        self._insert_filenode_for_path,
                        (p, )
                    ))
                log.info(f"writing file documents to db ...")
                for r in tqdm(ret_futures):
                    try:
                        r.result()
                    except KeyboardInterrupt as e:
                        self.cancel()
                        raise e

    def setup(self):
        """Clone the repo, connect to the DB, create working directories, etc."""
        self._connect_db()
        self._get_git_repo()
        nh = Node("SCM_Host", host=self._url_hostname)
        nh = SCM_Host.wrap(nh)
        tx = self.graph.auto()
        tx.merge(nh)
        self._tree_scm_host = nh
        # self._tree_scm_host = SCM_Host.match(self.graph, self._url_hostname).first()
