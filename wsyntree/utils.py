
import os
import itertools
from pathlib import Path
import contextlib
import hashlib
from urllib.parse import urlparse

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

def node_as_sexp(
        node, *,
        maxdepth: int = None, only_named: bool = True,
        indent: int = None, _cur_indent=0,
        show_start_coords=False,
        ):
    if not node.name and only_named:
        return ""
    t = ""
    if indent is not None:
        t += "\n" + " " * (_cur_indent)
    t += f"({node.name}"
    if show_start_coords:
        t += f" {node.x1}, {node.y1}"
    if maxdepth is not None and maxdepth <= 0:
        t += " ..."
    elif node.children:
        for child in node.children:
            t += " " if indent is not None else ""
            t += node_as_sexp(
                child,
                maxdepth=maxdepth-1 if maxdepth else None,
                _cur_indent=(_cur_indent+indent) if indent is not None else None,
                only_named=only_named,
                indent=indent,
                show_start_coords=show_start_coords,
            )
    t += ")"
    return t

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def chunkiter(seq, size):
    it = iter(seq)
    while chunk := tuple(itertools.islice(it, size)):
        yield chunk

def strip_url(u):
    p = urlparse(u)
    return f"{p.scheme}://{p.hostname}:{p.port}"

def sha1hex(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()
