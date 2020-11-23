
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
