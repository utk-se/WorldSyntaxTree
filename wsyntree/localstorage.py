
from pathlib import Path
import os

default_basename = "wsyntree"

class StorageBase():
    def __init__(self, name):
        raise NotImplementedError()

class LocalCache(StorageBase):
    @staticmethod
    def get_local_cache_dir(name=default_basename):
        cachedir = Path(os.environ.get("XDG_CACHE_HOME", os.environ.get("HOME") + '/.cache'))
        cachedir = cachedir / name
        if not cachedir.exists():
            cachedir.mkdir(mode=0o770, parents=True)
        return cachedir
