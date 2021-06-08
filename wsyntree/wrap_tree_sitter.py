
from pathlib import Path
from typing import AnyStr, Callable
import functools
import re

import pebble
from tree_sitter import Language, Parser, TreeCursor, Node
import pygit2 as git
from filelock import FileLock, Timeout

from . import log
from .localstorage import LocalCache
from .constants import wsyntree_langs, wsyntree_file_to_lang


class TreeSitterAutoBuiltLanguage():
    def __init__(self, lang):
        self.lang = lang
        self.parser = None
        self.ts_language = None
        # use this lock when modifying the cachedir:
        self.ts_lang_cache_lock = FileLock(self._get_language_cache_dir() / "tsabl.lock")

    def __repr__(self):
        return f"TreeSitterAutoBuiltLanguage<{self.lang}>"

    def _get_language_cache_dir(self):
        lc_d = LocalCache.get_local_cache_dir() / self.lang
        lc_d.mkdir(mode=0o770, exist_ok=True)
        return lc_d

    def _get_language_repo_path(self):
        return self._get_language_cache_dir() / "tsrepo"

    def _get_language_repo(self):
        repodir = self._get_language_repo_path()
        try:
            self.ts_lang_cache_lock.acquire(timeout=60)
            if not repodir.exists():
                repodir.mkdir(mode=0o770)
                log.debug(f"cloning treesitter repo for {self}")
                return git.clone_repository(
                    wsyntree_langs[self.lang]["tsrepo"],
                    repodir.resolve()
                )
            else:
                repopath=git.discover_repository(
                    repodir.resolve()
                )
                return git.Repository(repopath)
        finally:
            self.ts_lang_cache_lock.release()

    def _get_language_library(self):
        try:
            self.ts_lang_cache_lock.acquire(timeout=300)
            lib = self._get_language_cache_dir() / "language.so"
            repo = self._get_language_repo()
            repodir = self._get_language_repo_path()
            if not lib.exists():
                log.warn(f"building library for {self}, this could take a while...")
                start = time.time()
                Language.build_library(
                    str(lib.resolve()),
                    [repodir]
                )
                log.debug(f"library build of {self} completed after {round(time.time() - start)} seconds")
            return lib
        except filelock.Timeout as e:
            log.error(f"Failed to acquire lock on TSABL {self}")
            log.debug(f"lock object is {self.ts_lang_cache_lock}")
            raise e
        finally:
            self.ts_lang_cache_lock.release()

    def _get_ts_language(self):
        if self.ts_language is not None:
            return self.ts_language
        self.ts_language = Language(
            self._get_language_library(),
            self.lang
        )
        return self.ts_language

    def _get_parser(self):
        if self.parser is not None:
            return self.parser
        self.parser = Parser()
        self.parser.set_language(self._get_ts_language())
        return self.parser

    ### NOTE public functions:

    def get_parser(self):
        return self._get_parser()

    def parse_file(self, file):
        if issubclass(type(file), Path):
            return self._get_parser().parse(
                file.open('rb').read()
            )
        elif issubclass(type(file), str):
            return self._get_parser().parse(
                open(file, 'rb').read()
            )
        else:
            raise NotImplementedError(f"cannot understand file argument of type {type(file)}")

class TreeSitterCursorIterator(): # cannot subclass TreeCursor because it's C
    """Iterator wrapper for a TreeCursor

    This iterates through every node of the tree in parsed order.

    It yields one node of the tree at a time.
    """
    def __init__(
            self,
            cursor: TreeCursor,
            nodefilter: Callable[[Node], bool] = lambda x: True
        ):
        self._cursor = cursor
        self.nodefilter = nodefilter
        self._depth = 0
        self._preorder = 0

        assert self._cursor.goto_parent() == False, f"TreeSitterCursorIterator requires the root node to start with"

    def __iter__(self):
        return self

    def _next_node_in_tree(self) -> Node:
        next_child = self._cursor.goto_first_child()
        if next_child == True:
            self._depth += 1
            return self._cursor.node
        next_sibling = self._cursor.goto_next_sibling()
        if next_sibling == True:
            return self._cursor.node
        # otherwise step to the parent:
        while not self._cursor.goto_next_sibling():
            goto_parent = self._cursor.goto_parent()
            self._depth -= 1
            if goto_parent == False:
                # finished iterating tree
                raise StopIteration()
        return self._cursor.node

    def __next__(self) -> Node:
        test_node = self._next_node_in_tree()
        self._preorder += 1
        while not self.nodefilter(test_node):
            test_node = self._next_node_in_tree()
            self._preorder += 1
        return test_node

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def preorder(self) -> int:
        return self._preorder

    def peek(self) -> Node:
        """Peek at the current node"""
        return self._cursor.node


@pebble.synchronized
@functools.lru_cache(maxsize=None)
def get_cached_TSABL(lang: str):
    return TreeSitterAutoBuiltLanguage(lang)

def get_TSABL_for_file(file: str):
    """Match the filename and get the respective TreeSitterAutoBuiltLanguage"""
    for k,v in wsyntree_file_to_lang.items():
        pattern = re.compile(k)
        if re.search(pattern, file):
            return get_cached_TSABL(v)
    return None
