
import functools
import shutil
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from multiprocessing import Queue, Manager
# from multiprocessing.pool import ThreadPool, Pool
import concurrent.futures as futures
import os
import time

import pygit2 as git
from pebble import ProcessPool, ThreadPool
import enlighten

from wsyntree import log, tree_models, multiprogress
from wsyntree.exceptions import *
from wsyntree.tree_models import * # __all__
from wsyntree.localstorage import LocalCache
from wsyntree.utils import (
    list_all_git_files, pushd, strip_url, sha1hex, chunkiter
)
# from .arango_collector_worker import _tqdm_node_receiver
from .jsonl_worker import process_file


class WST_JSONLCollector():
    def __init__(
            self,
            repo_url: str,
            *,
            export_q = None,
            # database_conn: str = "http://wst:wst@localhost:8529/wst",
            workers: int = None,
            commit_sha: str = None,
            en_manager = None,
        ):
        """
        export_q: Queue to write completed documents to
        commit_sha: full sha1 hex commit, optional, if present will checkout
        workers: number of file processes in parallel
        """
        self.repo_url = repo_url

        multiprogress.setup_if_needed()
        self.en_manager = en_manager or multiprogress.get_manager()
        if multiprogress.is_proxy(en_manager):
            self.en_manager_proxy = en_manager
        else:
            self.en_manager_proxy = multiprogress.get_manager_proxy()

        pr = urlparse(self.repo_url)
        self._url_scheme = pr.scheme
        self._url_hostname = pr.hostname
        self._url_path = pr.path[1:]

        self._export_q = export_q

        self._target_commit = commit_sha
        self._tree_repo = None

        self._worker_count = workers or os.cpu_count()
        self._mp_manager = None
        # self._node_queue = None

    ### NOTE private control functions:

    def _get_git_repo(self):
        repodir = self._local_repo_path
        if not (repodir / '.git').exists():
            repodir.mkdir(mode=0o770, parents=True, exist_ok=True)
            log.info(f"cloning repo to {repodir} ...")
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

    def collect_all(self, existing_node_q = None, overwrite_incomplete: bool = False):
        """Creates every node down the tree for this repo"""
        # create the main Repos
        self._tree_repo = WSTRepository(
            type='git',
            url=self.repo_url,
            path=self._url_path,
            analyzed_time=int(time.time()),
            wst_status="started",
        )
        self._tree_repo._genkey()

        _cc = self._current_commit
        self._wst_commit = WSTCommit(
            _key=_cc.hex,
            commit_time=_cc.commit_time,
            commit_time_offset=_cc.commit_time_offset,
            parent_ids=[str(i) for i in _cc.parent_ids],
            tree_id=str(_cc.tree_id),
        )

        rel_repo_commit = self._tree_repo / self._wst_commit
        self._export_q.put([
            self._wst_commit,
            rel_repo_commit,
        ])

        index = self._get_git_repo().index
        index.read()

        # file-level processing
        with pushd(self._local_repo_path), Manager() as self._mp_manager:
            with ProcessPool(max_workers=self._worker_count) as executor:
                self._stoppable = executor
                log.info(f"scanning git for files ...")
                ret_futures = []
                cntr_add_jobs = self.en_manager.counter(
                    desc=f"scanning files for {self._url_path}",
                    total=len(index), autorefresh=True, leave=False
                )
                for gobj in index:
                    if not gobj.mode in (git.GIT_FILEMODE_BLOB, git.GIT_FILEMODE_BLOB_EXECUTABLE, git.GIT_FILEMODE_LINK):
                        continue
                    _file = Path(gobj.path)
                    # check size of file first:
                    _fstat = _file.lstat()

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
                        (nf, self._export_q),
                        {'en_manager': self.en_manager_proxy}
                    ))
                    cntr_add_jobs.update()
                cntr_add_jobs.close(clear=True)
                log.info(f"processing files with {self._worker_count} workers ...")
                try:
                    cntr_files_processed = self.en_manager.counter(
                        desc=f"processing {self._url_path}",
                        total=len(ret_futures), unit="files",
                        leave=False, autorefresh=True
                    )
                    for r in futures.as_completed(ret_futures):
                        completed_file = r.result()
                        if not hasattr(completed_file, '_key'):
                            completed_file._genkey()
                        self._export_q.put([
                            completed_file,
                            self._wst_commit / completed_file,
                        ])
                        cntr_files_processed.update()
                    # after all results returned
                    self._tree_repo.wst_status = "completed"
                    # self._tree_repo.update_in_db(self._db)
                    log.info(f"{self._url_path} marked completed.")
                except KeyboardInterrupt as e:
                    log.warn(f"stopping collection ...")
                    for rf in ret_futures:
                        rf.cancel()
                    executor.close()
                    executor.join(5)
                    executor.stop()
                    # raise e
                    self._tree_repo.wst_status = "cancelled"
                    # self._tree_repo.update_in_db(self._db)
                    log.info(f"{self._tree_repo.url} wst_status marked as cancelled")
                except Exception as e:
                    self._tree_repo.wst_status = "error"
                    raise e
                finally:
                    cntr_files_processed.close()
                    self._export_q.put(self._tree_repo)

    def setup(self):
        """Clone the repo, create working directories, etc."""
        repo = self._get_git_repo()
        if self._current_commit is None:
            log.warn(f"Deleting and re-cloning repo in {self._local_repo_path}")
            try:
                shutil.rmtree(self._local_repo_path)
                repo = self._get_git_repo()
            except Exception as e:
                log.error(f"Failed to repair repository: {type(e)}: {e}")
                raise e
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
                raise e
            log.info(f"Repo at {self._local_repo_path} now at {self._current_commit_hash}")
        elif self._target_commit and self._target_commit == self._current_commit_hash:
            log.debug(f"Repo in {self._local_repo_path} is already at {self._target_commit}")
