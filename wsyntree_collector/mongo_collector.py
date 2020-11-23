
import functools
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse

import pygit2 as git
import mongoengine

from wsyntree import log
from wsyntree.tree_models import Repository, File, Node, NodeText
from wsyntree.localstorage import LocalCache
from wsyntree.utils import list_all_git_files, pushd


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
        self._tree_files = []

    ### NOTE private control functions:

    def _connect_mongoengine(self):
        log.debug(f"connecting mongodb...")
        mongoengine.connect(
            host=self.database_conn_str
        )

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

    def _grow_file_by_path(self, path: Path):
        """Grows a single File object without any Nodes"""
        nf = File(
            repo=self._tree_repo,
            path=str(path)
        )
        nf.save()
        # will update it's nodes later:
        self._tree_files.append(nf)

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

        # TODO delete all nodes of the file

        for f in files:
            f.delete()
        self._tree_repo.delete()
        # done deleting everything:
        self._tree_files = []
        self._tree_repo = None

    def setup(self):
        """Clone the repo, connect to the DB, create working directories, etc."""
        self._connect_mongoengine()
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

    def grow_files(self):
        """Create File objects for the tree"""
        assert self._tree_repo is not None
        log.info(f"{self} growing files...")
        with pushd(self._local_repo_path):
            for p in list_all_git_files(self._get_git_repo()):
                self._grow_file_by_path(p)
        log.info(f"{self} grew {len(self._tree_files)} files")

    def grow_nodes(self):
        """Parse each file and generate it's nodes"""
        log.warn("NotImplemented!")

    def collect_all(self):
        """Performs all collection steps for this instance."""
        self.grow_repo()
        self.grow_files()
        self.grow_nodes()
