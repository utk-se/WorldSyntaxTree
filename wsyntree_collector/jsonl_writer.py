
from pathlib import Path
import time
import os
from typing import Union, List
import json
from contextlib import contextmanager
import cProfile

import orjson
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
        # self._pending_lines = {}
        self._pending_bytes = {}

        self._locks = {}
        for collname, cf in self._coll_files.items():
            self._locks[collname] = filelock.FileLock(self.dir / f"{collname}.lock")
            # self._pending_lines[collname] = []
            self._pending_bytes[collname] = bytearray()

    def _flush_if_needed(self, collname):
        if len(self._pending_bytes[collname]) > 10000000:
            self._flush(only_collection=collname)

    def write_many_documents(self, docs: List[Union[WST_Document, WST_Edge]]):
        """Output a list of documents to the filesystem"""
        modified_collections = set()
        for doc in docs:
            # self._pending_lines[doc._collection].append(json.dumps(doc.__dict__, sort_keys=True) + '\n')
            self._pending_bytes[doc._collection] += orjson.dumps(
                doc.__dict__, option=orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE
            )
            modified_collections.add(doc._collection)
        for collname in modified_collections:
            self._flush_if_needed(collname)

    def write_document(self, doc: Union[WST_Document, WST_Edge]):
        """Output a document to the filesystem"""
        # with self._locks[doc._collection].acquire():
        # f = self._get_open_file(doc._collection)
        # f.write(json.dumps(doc.__dict__, sort_keys=True))
        # f.write('\n')
        # self._pending_lines[doc._collection].append(json.dumps(doc.__dict__, sort_keys=True) + '\n')
        self._pending_bytes[doc._collection] += orjson.dumps(
            doc.__dict__, option=orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE
        )
        self._flush_if_needed(doc._collection)

    def _get_open_file(self, collname):
        if collname in self._open_files:
            return self._open_files[collname]
        else:
            raise Exception("slow writes are bad")

    def _open_all_append(self):
        for collname, cf in self._coll_files.items():
            try:
                self._locks[collname].acquire(timeout=60)
            except filelock.Timeout as e:
                log.error(f"Could not acquire output lock for {collname}")
                raise e
            self._open_files[collname] = cf.open('ab')

    def _close_all(self):
        self._flush()
        for collname, cf in self._coll_files.items():
            self._open_files[collname].close()
            del self._open_files[collname]
            self._locks[collname].release()

    def _flush(self, only_collection = None):
        if only_collection is None:
            # for collname, lines in self._pending_lines.items():
            for collname, _bytestr in self._pending_bytes.items():
                f = self._get_open_file(collname)
                # for l in lines:
                #     f.write(l)
                f.write(_bytestr)
                # self._pending_lines[collname] = []
                self._pending_bytes[collname] = bytearray()
        else:
            collname = only_collection
            _bytestr = self._pending_bytes[collname]
            f = self._get_open_file(collname)
            # for l in lines:
            #     f.write(l)
            f.write(_bytestr)
            # self._pending_lines[collname] = []
            self._pending_bytes[collname] = bytearray()

def profileit(name):
    def inner(func):
        def wrapper(*args, **kwargs):
            prof = cProfile.Profile()
            retval = prof.runcall(func, *args, **kwargs)
            # Note use of name from outer scope
            prof.dump_stats(name)
            return retval
        return wrapper
    return inner

@concurrent.process
@profileit('queue_writer.profile')
def write_from_queue(q, en_manager, *args, **kwargs):
    """Write out any document that comes in from the queue"""
    self = WST_FileExporter(*args, **kwargs)
    log.debug(f"writing to target output dir: {self.dir}")
    time.sleep(0.1)
    cntr = en_manager.counter(
        desc="writing to files", position=1, unit='docs', autorefresh=True
    )
    try:
        self._open_all_append()
        while (incoming := q.get()) is not None:
            if isinstance(incoming, list):
                for doc in incoming:
                    def get_docs():
                        if isinstance(doc, WST_Document) and not hasattr(doc, '_key'):
                            doc._genkey()
                        yield doc
                    self.write_many_documents(get_docs())
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
    except Exception as e:
        log.error(f"{type(e)}: {e}")
        raise e
    finally:
        self._close_all()
