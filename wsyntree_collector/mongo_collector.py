
import functools
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from multiprocessing.pool import ThreadPool as Pool
import concurrent.futures
import os
import itertools

import pygit2 as git
import mongoengine
from tqdm import tqdm
from pebble import ProcessPool

from wsyntree import log
from wsyntree.wrap_tree_sitter import get_TSABL_for_file, TreeSitterAutoBuiltLanguage
from wsyntree.tree_models import Repository, File, Node, NodeText
from wsyntree.localstorage import LocalCache
from wsyntree.utils import list_all_git_files, pushd


def _connect_mongoengine(conn_str: str):
    """Use this when forking new processes."""
    log.debug(f"connecting mongodb...")
    mongoengine.connect(
        host=conn_str,
        connect=False
    )


def _grow_nodes_by_file(file: File, tsabl: TreeSitterAutoBuiltLanguage):
    """Grows the nodes for a single File"""

    tree = tsabl.parse_file(file.path)

    log.debug(f"growing nodes for {file}")
    cursor = tree.walk()
    # iteration loop
    cur_tree_parent = None
    # prev_tree_node = None
    while cursor.node is not None:
        cur_node = cursor.node
        nn = Node(
            file=file,
            name=cur_node.type if cur_node.is_named else "",
            text=NodeText.get_or_create(cur_node.text.tobytes().decode()),
            parent=cur_tree_parent,
            children=[],
        )
        (nn.x1,nn.y1) = cur_node.start_point
        (nn.x2,nn.y2) = cur_node.end_point
        # log.debug(f"grew {cur_node}")
        # TODO text storage
        nn.save()
        if cur_tree_parent is not None:
            cur_tree_parent.children.append(nn)
            cur_tree_parent.save()

        # now determine where to move to next:
        next_child = cursor.goto_first_child()
        if next_child == True:
            cur_tree_parent = nn
            continue # cur_node to next_child
        next_sibling = cursor.goto_next_sibling()
        if next_sibling == True:
            continue # cur_node to next_sibling
        # go up parents
        while cursor.goto_next_sibling() == False:
            goto_parent = cursor.goto_parent()
            if goto_parent:
                # reversing up the tree
                if cur_tree_parent.parent:
                    cur_tree_parent = cur_tree_parent.parent.fetch()
                else:
                    cur_tree_parent = None
            else:
                # we are done iterating
                return goto_parent


def _grow_file_nodes(path: Path, tsabl: TreeSitterAutoBuiltLanguage, repo):
    """Grows a single File object without any Nodes"""
    nf = File(
        repo=repo,
        path=str(path)
    )
    nf.save()
    _grow_nodes_by_file(nf, tsabl)


class WST_MongoTreeCollector():
    def __init__(self, repo_url: str, database_conn: str, force=False):
        self.repo_url = repo_url
        self.database_conn_str = database_conn
        self._force = force

        pr = urlparse(self.repo_url)
        self._url_scheme = pr.scheme
        self._url_hostname = pr.hostname
        self._url_path = pr.path[1:]

        self._tree_repo = None
        # self._tree_files = []

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
        return f"WST_MongoTreeCollector<{self.repo_url}>"
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
        self._tree_repo = Repository.objects.get(
            clone_url=self.repo_url,
            analyzed_commit=self._current_commit_hash
        )
        log.warn(f"clearing all tree data for {self} commit {self._tree_repo.analyzed_commit}")
        files = File.objects(repo=self._tree_repo)
        def del_nodes(f):
            Node.objects(file=f).delete()
        log.info(f"deleting ...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            ret_futures = []
            for f in files:
                ret_futures.append(executor.submit(del_nodes, f))
            for r in tqdm(ret_futures):
                r.result()
        files.delete()
        self._tree_repo.delete()
        # done deleting everything:
        self._tree_files = []
        self._tree_repo = None

    def setup(self):
        """Clone the repo, connect to the DB, create working directories, etc."""
        _connect_mongoengine(self.database_conn_str)
        self._get_git_repo()

    def grow_repo(self):
        """Populate the Repo object."""
        log.info(f"{self} growing repo...")
        # check for existing:
        preexists = Repository.objects(
            clone_url=self.repo_url,
            analyzed_commit=self._current_commit_hash
        )
        if len(preexists) > 0:
            raise FileExistsError(f"repo document already exists as {preexists[0].id} in tree")
        else:
            self._tree_repo = Repository(
                clone_url=self.repo_url,
                analyzed_commit=self._current_commit_hash,
                added_time=datetime.now(tz=timezone.utc)
            )
            self._tree_repo.save()

    def grow_tree(self):
        """Creates the tree files and nodes"""
        assert self._tree_repo is not None
        log.info(f"{self} growing the tree ...")
        with pushd(self._local_repo_path):
            n_success = 0
            pool = ProcessPool(
                max_workers=os.cpu_count(),
                initializer=_connect_mongoengine,
                initargs=[self.database_conn_str]
            )
            try:
                log.info(f"scanning git files ...")
                paths = []
                for p in tqdm(list_all_git_files(self._get_git_repo())):
                    lang = get_TSABL_for_file(str(p))
                    if lang is None:
                        log.debug(f"no language available for {p}")
                        continue
                    paths.append((p, lang))
                ret_futures = []
                log.info(f"starting workers ...")
                for p, l in tqdm(paths):
                    ret_futures.append(pool.schedule(
                        _grow_file_nodes,
                        args=(p, l, self._tree_repo.id)
                    ))
                log.info(f"processing files and trees ...")
                # pmap_future = pool.map(
                #     _grow_file_nodes,
                #     *zip(*paths),
                #     itertools.repeat(self._tree_repo),
                #     # chunksize=1000
                # )
                # for r in tqdm(pmap_future.result()):
                for r in tqdm(ret_futures):
                    r.result()
                    n_success += 1
                log.info(f"{self} grew {len(ret_futures)} files")
            except KeyboardInterrupt as e:
                log.warn(f"Stopping processing pool ...")
                pool.stop()
            finally:
                pool.close()
                pool.join()

    # def grow_nodes(self):
    #     """Parse each file and generate it's nodes"""
    #
    #     with pushd(self._local_repo_path):
    #         # lots of files to analyze:
    #         with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
    #             # executor.map(
    #             #     self._grow_nodes_by_file,
    #             #     self._tree_files,
    #             #     chunksize=1
    #             # )
    #             log.info(f"starting node jobs...")
    #             ret_futures = []
    #             for f in self._tree_files:
    #                 ret_futures.append(executor.submit(_grow_nodes_by_file, f))
    #             log.info(f"analyzing files...")
    #             for r in tqdm(ret_futures):
    #                 r.result()

    def collect_all(self):
        """Performs all collection steps for this instance."""
        self.grow_repo()
        self.grow_tree()
        # self.grow_files()
        # self.grow_nodes()
