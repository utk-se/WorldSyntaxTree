
from itertools import chain
from typing import Union
from json import JSONEncoder

from arango.job import AsyncJob
from arango.database import StandardDatabase, BatchDatabase
import tenacity
from tenacity.retry import retry_if_exception

from . import log
from .utils import dotdict, shake256hex
from .exceptions import *

__all__ = [
    'WST_Document', 'WST_Edge',
    'WSTRepository', 'WSTCommit', 'WSTFile',
    'WSTCodeTree', 'WSTNode', 'WSTText',
]

_graph_name = 'wst'
_collection_name_to_class = {} # populated at import time, bottom of this file


class WST_Document():
    __slots__ = [
        "__collection",
        "_key",
    ]

    @classmethod
    def get(cls, db, key):
        """Return an instance by key

        Returns None if document by key does not exist.
        """
        collection = db.collection(cls._collection)
        res = collection.get(key)
        if collection.context == "async":
            document = auto_asyncjobdone_retry(lambda: res.result())()
        else:
            document = res
        return cls(**document) if document is not None else None

    @classmethod
    def find(cls, db, spec):
        """Find instances matching spec

        Returns an iterable of matched instances.
        """
        collection = db.collection(cls._collection)
        cur = collection.find(spec)
        for document in cur:
            yield cls(**document)

    def __init__(self, *args, **kwargs):
        # slots = chain.from_iterable([getattr(cls, '__slots__', tuple()) for cls in type(self).__mro__])
        for k, v in kwargs.items():
            try:
                setattr(self, k, v)
            except AttributeError as e:
                if k == "_id":
                    # attr is @property / automatic
                    continue
                    # log.debug(f"ignore {k} = {v}")

    @classmethod
    def iterate_from_parent(cls, db, parent, return_inflated = True):
        """Iterate vertexes connected towards parent"""
        graph = db.graph(_graph_name)
        edge_coll = graph.edge_collection(cls._edge_to[parent._collection])

        edges = edge_coll.edges(parent.__dict__, "in")['edges']
        for e in edges:
            yield cls.get(db, e['_from']) if return_inflated else e['_from']

    @property
    def _id(self):
        return f"{self._collection}/{self._key}"

    @property
    def _collection(self):
        return self.__collection

    @_collection.setter
    def _collection(self, val):
        self.__collection = val

    @property
    def __dict__(self):
        slots = chain.from_iterable([getattr(cls, '__slots__', tuple()) for cls in type(self).__mro__])
        attrs = {
            s: getattr(self, s, None) for s in slots if not s.startswith('__')
        }
        return {
            **attrs,
            "_id": self._id,
        }

    def _genkey(self):
        if not hasattr(self, '_keyfmt') or not self._keyfmt:
            raise ValueError(f"instance of {type(self)} does not have _keyfmt specified, cannot automatically determine _key")
        self._key = self._keyfmt.format(self)
        return self._key

    @auto_writewrite_retry
    def insert_in_db(
            self,
            db: Union[StandardDatabase, BatchDatabase],
            wait_if_async: bool = True,
            **insert_kwargs,
        ):
        """Insert this document into a db"""
        if not hasattr(self, '_key') or not self._key:
            self._genkey()
        coll = db.collection(self._collection)
        res = coll.insert(self.__dict__, **insert_kwargs)
        if coll.context == "async" and wait_if_async:
            # wait for the async result and return that or error
            return auto_asyncjobdone_retry(lambda: res.result())()
        else:
            return res

    def update_in_db(
            self,
            db: Union[StandardDatabase, BatchDatabase],
            wait_if_async: bool = True,
            **update_kwargs
        ):
        assert self._key, '_key must already be set in order to update the document'
        coll = db.collection(self._collection)
        res = coll.update(self.__dict__, **update_kwargs)
        if coll.context == "async" and wait_if_async:
            # wait for the async result and return that or error
            return auto_asyncjobdone_retry(lambda: res.result())()
        else:
            return res

    def _make_edge(self, rhs):
        """
        {WST_Document A} / {WST_Document B}
        creates an edge document FROM A TO B
        """
        return WST_Edge(self, rhs)
    __floordiv__ = _make_edge
    __truediv__ = _make_edge

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        return f"{type(self)}({self.__dict__})"
    __str__ = __repr__

    def get_children(self, db, cls, return_inflated=True):
        """Iterate all children of this node

        cls: WST_Document class of the Child type to retrieve
        """
        graph = db.graph(_graph_name)
        edge_coll = graph.edge_collection(self._edge_to[cls._collection])

        edges = edge_coll.edges(self.__dict__, "out")['edges']
        for e in edges:
            yield cls.get(db, e['_to']) if return_inflated else e['_to']

    def get_parents(self, db, cls, return_inflated=True):
        """Iterate all parents of this node

        cls: WST_Document class of the Parent type to retrieve
        """
        graph = db.graph(_graph_name)
        edge_coll = graph.edge_collection(cls._edge_to[self._collection])

        edges = edge_coll.edges(self.__dict__, "in")['edges']
        for e in edges:
            yield cls.get(db, e['_from']) if return_inflated else e['_from']

class WST_Edge(dict):
    # These slots are NOT part of the inserted document in Arango
    __slots__ = [
        "_from_collection",
        "_from_key",
        "_to_collection",
        "_to_key",
        "_edge_collection",
    ]
    def __init__(self, nfrom: Union[WST_Document, str], nto: Union[WST_Document, str]):
        # FROM
        if isinstance(nfrom, str):
            if '/' not in nfrom:
                raise ValueError(f"when `from` is string: must be fully qualified document ID: invalid ID '{nfrom}'")
            self._from_collection, self._from_key = nfrom.split('/')
        else:
            self._from_collection, self._from_key = nfrom._collection, nfrom._key

        # TO
        if isinstance(nto, str):
            if '/' not in nto:
                raise ValueError(f"when `to` is string: must be fully qualified document ID: invalid ID '{nto}'")
            self._to_collection, self._to_key = nto.split('/')
        else:
            self._to_collection, self._to_key = nto._collection, nto._key

        _src_cls = _collection_name_to_class.get(self._from_collection)
        if _src_cls is None:
            raise TypeError(f"collection '{self._from_collection}' has no associated WST_Document model")
        if self._to_collection not in _src_cls._edge_to.keys():
            raise TypeError(f"cannot connect type {_src_cls.__name__} to {nto}, edge collection from {_src_cls._collection} to {self._to_collection} not set")
        self._edge_collection = _src_cls._edge_to[self._to_collection]

        # hash the keys because each vert's key could be >= half the max key size
        self["_key"] = shake256hex(f"{self._from_key}+{self._to_key}", 64)
        self["_from"] = f"{self._from_collection}/{self._from_key}"
        self["_to"] = f"{self._to_collection}/{self._to_key}"

    def insert_in_db(self, db, wait_if_async=True, overwrite: bool = False):
        """Uses the graph API!"""
        graph = db.graph(_graph_name)
        edges = graph.edge_collection(self._edge_collection)

        try:
            res = edges.insert(self)
            if db.context == "async" and wait_if_async:
                # wait for the async result and return that or error
                return auto_asyncjobdone_retry(lambda: res.result())()
        except arango.exceptions.DocumentInsertError as e:
            if overwrite and e.http_code == 409 and e.error_code == arango.errno.UNIQUE_CONSTRAINT_VIOLATED:
                res = edges.replace(self)
                if db.context == "async" and wait_if_async:
                    # wait for the async result and return that or error
                    return auto_asyncjobdone_retry(lambda: res.result())()
            else:
                raise
        return res

    def update_in_db(self, db, wait_if_async=True, **update_kwargs):
        """Uses the graph API!"""
        graph = db.graph(_graph_name)
        edges = graph.edge_collection(self._edge_collection)
        res = edges.update(self, **update_kwargs)

        if db.context == "async" and wait_if_async:
            # wait for the async result and return that or error
            return auto_asyncjobdone_retry(lambda: res.result())()
        else:
            return res


### DOCUMENT TYPES
# NOTE
# please try to keep all _key values constant length if possible
# notable exceptions include guarantees like WST `language` names length

class WSTText(WST_Document):
    _collection = "wst_texts"
    __slots__ = [
        "length",
        "text",
        # "content_hash", # 128 hex chars
    ]

    def _genkey(self):
        self._key = f"{self.length}-{shake256hex(self.text.encode(), 64)}"
        return self._key

    def insert_in_db(self, db: Union[StandardDatabase, BatchDatabase], **kwargs):
        """WSTTexts might be duplicate

        We prevent errors during insert if the document already exists:
        They are perfectly equivalent, enforced by the key having the hash of text,
        thus we will never lose data with overwrite
        """
        assert self._key
        return super().insert_in_db(db, overwrite_mode="replace", **kwargs)

class WSTNode(WST_Document):
    """A single node in the Abstract or Concrete syntax tree

    Basically copies the data structure from tree-sitter
    """
    _collection = "wst_nodes"
    _edge_to = {
        WSTText._collection: "wst-node-text", # singular
        _collection: "wst-node-children", # singular, own-kind
    }
    __slots__ = [
        # x: line, y: character (2d coords begin->end)
        "x1",
        "y1",
        "x2",
        "y2",

        # preorder: depth-first traversal order, unique within WSTCodeTree
        # aka: topologically sorted
        # root node is zero and has no parent
        "preorder",

        "named",
        "type",
    ]

class WSTCodeTree(WST_Document):
    """A code tree is a parsed syntax tree"""
    _collection = "wst_codetrees"
    _keyfmt = "{0.language}-{0.content_hash}"
    _edge_to = {
        WSTNode._collection: "wst-codetree-root-node", # singular
    }
    __slots__ = [
        "language", # WST lang id
        "lang_version", # probably the commit of tree-sitter language lib used
        "content_hash", # 128 hex chars
        "git_oid",

        # set when we could not generate all WSTNodes
        "error", # any reason the CodeTree may not be accurate or complete
    ]

class WSTFile(WST_Document):
    _collection = "wst_files"
    _edge_to = {
        WSTCodeTree._collection: "wst-file-codetree", # singular
        # WSTText._collection: "wst-file-text", # probably unneeded
    }
    __slots__ = [
        "path", # path as stored in git / relative to workdir
        "language", # WST lang id, could be calculated from path
        "mode", # git mode, not unix perms, but still octal
        "size", # in bytes
        "git_oid", # object ID from git
        "error", # any reason a CodeTree could not be generated for this file
        "symlink", # data about the symlink, if not a link, None / null

        # so that we can build _key / _id to a CodeTree without a lookup,
        # this needs to match WSTCodeTree.content_hash
        "content_hash", # 128 hex chars
    ]

    def _genkey(self):
        self._key = f"{shake256hex(self.path, 32)}-{oct(self.mode)}-{self.content_hash}"
        return self._key

class WSTCommit(WST_Document):
    _collection = "wst_commits"
    _edge_to = {
        # edges pointing to all files present with this commit checked out
        WSTFile._collection: "wst-commit-files", # multi
    }
    __slots__ = [
        "commit_time",
        "commit_time_offset",
        "parent_ids",
        "tree_id",
    ]

class WSTRepository(WST_Document):
    _collection = "wst_repos"
    _edge_to = {
        WSTCommit._collection: "wst-repo-commit", # singular
        _collection: "wst-repo-forkedfrom", # singular, own-kind
    }
    __slots__ = [
        "type", # =git
        "url", # same URL used to clone
        "status", # repo might be 'archived' or something else
        "path",
        "analyzed_time", # WST run timestamp
        "wst_status",
        "wst_extra", # the repo dict as it was in the batch input document
    ]

    def _genkey(self):
        self._key = f"{shake256hex(self.url, 64)}"
        return self._key

# autogenerate names for the database:
_db_collections = []
_db_edgecollections = []
_graph_edge_definitions = {}

for localname, value in dict(locals()).items():
    if localname.startswith('WST'):
        if hasattr(value, '_collection') and issubclass(value, WST_Document):
            if isinstance(value._collection, str):
                _db_collections.append(value._collection)
                _collection_name_to_class[value._collection] = value
            if hasattr(value, '_edge_to'):
                for targetcoll, edgecollname in value._edge_to.items():
                    _db_edgecollections.append(edgecollname)
                    if edgecollname in _graph_edge_definitions:
                        raise EdgeDefinitionDuplicateError(f"{edgecollname} already in use by a different vertex type pair")
                    _graph_edge_definitions[edgecollname] = {
                        "edge_collection": edgecollname,
                        "from_vertex_collections": [value._collection],
                        "to_vertex_collections": [targetcoll],
                    }

if __name__ == '__main__':
    import pprint
    pp = pprint.PrettyPrinter(indent=2)
    log.info(f"Collections: {_db_collections}")
    log.info(f"Edge Collections: {_db_edgecollections}")
    log.info(f"Graph construction:")
    pp.pprint(_graph_edge_definitions)
    log.info(f"Collection names to classes:")
    pp.pprint(_collection_name_to_class)
