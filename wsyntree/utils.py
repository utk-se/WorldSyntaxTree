
import os
from pathlib import Path
import contextlib

import pygit2 as git

def list_all_git_files(repo: git.Repository, relative=True):
    idx = repo.index
    idx.read()

    for entry in idx:
        if os.path.isfile(entry.path):
            p = Path(entry.path)
            yield p if relative else Path(repo.path).join(p)

@contextlib.contextmanager
def pushd(new_dir):
    previous_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(previous_dir)

def node_as_sexp(node, *, maxdepth=None):
    t = f"({node.name}"
    if maxdepth is not None and maxdepth <= 0:
        t += " ..."
    elif node.children:
        for child in node.children:
            t += " "
            t += node_as_sexp(child, maxdepth=maxdepth-1 if maxdepth else None)
    t += ")"
    return t
