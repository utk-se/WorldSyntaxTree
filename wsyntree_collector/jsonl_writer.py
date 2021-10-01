
from pathlib import Path
import time
import os
from typing import Union
import json
from contextlib import contextmanager

import filelock
from pebble import concurrent

from wsyntree import log, tree_models, multiprogress
from wsyntree.exceptions import *
from wsyntree.utils import dotdict, strip_url, sha1hex, sha512hex
from wsyntree.tree_models import * # __all__


class WST_FileExporter():
    def __init__(
            self,
            directory: Path,
            delete_existing: bool = False,
            en_manager = None,
        ):
        if isinstance(directory, str):
            directory = Path(directory)
        self.dir = directory.resolve()
        self.dir.mkdir(parents=True, exist_ok=True)
        self._coll_files = {}

        for collname in tree_models._db_collections:
            self._coll_files[collname] = self.dir / f"{collname}.vert.jsonl"
        for collname in tree_models._db_edgecollections:
            self._coll_files[collname] = self.dir / f"{collname}.edge.jsonl"

        if delete_existing:
            for cf in self._coll_files.values():
                cf.unlink(missing_ok=True)

        self._in_context = False
        self._open_files = {}
        self._pending_lines = {}

        self._locks = {}
        for collname, cf in self._coll_files.items():
            self._locks[collname] = filelock.FileLock(self.dir / f"{collname}.lock")
            self._pending_lines[collname] = []

    def write_document(self, doc: Union[WST_Document, WST_Edge]):
        """Output a document to the filesystem"""
        # with self._locks[doc._collection].acquire():
        # f = self._get_open_file(doc._collection)
        # f.write(json.dumps(doc.__dict__, sort_keys=True))
        # f.write('\n')
        self._pending_lines[doc._collection].append(json.dumps(doc.__dict__, sort_keys=True) + '\n')
        if len(self._pending_lines[doc._collection]) > 1000000:
            self._flush(only_collection=doc._collection)

    def _get_open_file(self, collname):
        if collname in self._open_files:
            return self._open_files[collname]
        else:
            raise Exception("slow writes are bad")

    def _open_all_append(self):
        for collname, cf in self._coll_files.items():
            self._open_files[collname] = cf.open('a')

    def _close_all(self):
        self._flush()
        for collname, cf in self._coll_files.items():
            self._open_files[collname].close()
            del self._open_files[collname]

    def _flush(self, only_collection = None):
        if only_collection is None:
            for collname, lines in self._pending_lines.items():
                f = self._get_open_file(collname)
                # for l in lines:
                #     f.write(l)
                f.write(''.join(lines))
                self._pending_lines[collname] = []
        else:
            collname = only_collection
            lines = self._pending_lines[collname]
            f = self._get_open_file(collname)
            # for l in lines:
            #     f.write(l)
            f.write(''.join(lines))
            self._pending_lines[collname] = []

@concurrent.process
def write_from_queue(q, en_manager, *args, **kwargs):
    """Write out any document that comes in from the queue"""
    self = WST_FileExporter(*args, **kwargs)
    log.debug(f"writing to target output dir: {self.dir}")
    time.sleep(1)
    cntr = en_manager.counter(
        desc="writing to files", position=1, unit='docs', autorefresh=True
    )
    try:
        self._open_all_append()
        while (incoming := q.get()) is not None:
            if isinstance(incoming, list):
                for doc in incoming:
                    if isinstance(doc, WST_Document) and not hasattr(doc, '_key'):
                        doc._genkey()
                    self.write_document(doc)
                cntr.update(len(incoming))
            elif isinstance(incoming, WST_Document):
                doc = incoming
                if not hasattr(doc, '_key'):
                    doc._genkey()
                self.write_document(doc)
                cntr.update(1)
            elif isinstance(incoming, WST_Edge):
                doc = incoming
                self.write_document(doc)
                cntr.update(1)
            else:
                raise RuntimeError(f"Invalid write input: {incoming}")
    finally:
        self._close_all()
