
from pathlib import Path
from typing import AnyStr, Callable

from tree_sitter import Language, Parser, TreeCursor, Node
import pygit2 as git

from . import log
from .localstorage import LocalCache
from .constants import wsyntree_langs

class TreeSitterAutoBuiltLanguage():
    def __init__(self, lang):
        self.lang = lang
        self.parser = None
        self.ts_language = None

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

    def _get_language_library(self):
        lib = self._get_language_cache_dir() / "language.so"
        repo = self._get_language_repo()
        repodir = self._get_language_repo_path()
        if not lib.exists():
            log.debug(f"building library for {self}")
            Language.build_library(
                str(lib.resolve()),
                [repodir]
            )
        return lib

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
            self, cursor: TreeCursor,
            nodefilter: Callable[[Node], bool] = lambda x: True
        ):
        self._cursor = cursor
        self.nodefilter = nodefilter

    def __iter__(self):
        return self

    def _next_node_in_tree(self) -> Node:
        next_child = self._cursor.goto_first_child()
        if next_child == True:
            return self._cursor.node
        next_sibling = self._cursor.goto_next_sibling()
        if next_sibling == True:
            return self._cursor.node
        # otherwise step to the parent:
        while not self._cursor.goto_next_sibling():
            goto_parent = self._cursor.goto_parent()
            if goto_parent == False:
                # finished iterating tree
                raise StopIteration()
        return self._cursor.node

    def __next__(self) -> Node:
        test_node = self._next_node_in_tree()
        while not self.nodefilter(test_node):
            test_node = self._next_node_in_tree()
        return test_node
