
from itertools import chain
from typing import Union
from json import JSONEncoder

from arango.database import StandardDatabase, BatchDatabase

from . import log
from .utils import dotdict, shake256hex

__all__ = [
    'WSTRepository', 'WSTCommit', 'WSTFile',
    'WSTCodeTree', 'WSTNode', 'WSTText',
]

_graph_name = 'wst'


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
        document = collection.get(key)
        return cls(**document) if document is not None else None

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

    def insert_in_db(self, db: Union[StandardDatabase, BatchDatabase]):
        """Insert this document into a db"""
        if not hasattr(self, '_key') or not self._key:
            self._genkey()
        coll = db.collection(self._collection)
        return coll.insert(self.__dict__)

    def update_in_db(self, db: Union[StandardDatabase, BatchDatabase]):
        assert self._key, '_key must already be set in order to update the document'
        coll = db.collection(self._collection)
        return coll.update(self.__dict__)

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

class WST_Edge(dict):
    # These slots are NOT part of the inserted document in Arango
    __slots__ = [
        "_w_from",
        "_w_to",
        "_w_collection",
    ]
    def __init__(self, nfrom: WST_Document, to: Union[WST_Document, str]):
        if not nfrom._edge_to:
            raise TypeError(f"Cannot create an edge from {type(nfrom)}: no edge collections specified")
        if isinstance(to, str):
            if '/' not in to:
                raise ValueError(f"when `to` is string: must be fully qualified document ID: invalid ID '{to}'")
            coll, key = to.split('/')
            if coll not in nfrom._edge_to.keys():
                raise TypeError(f"cannot connect type {type(nfrom)} to a document of type {type(to)}, edge collection from {nfrom._collection} to {coll} not set")
            self._w_to = WST_Document(
                _collection=coll,
                _key=key,
            )
        elif isinstance(to, WST_Document) and to._collection not in nfrom._edge_to:
            raise TypeError(f"cannot connect type {type(nfrom)} to type {type(to)}, no edge collection set for these types")
        elif isinstance(to, WST_Document):
            self._w_to = to
        self._w_collection = nfrom._edge_to[self._w_to._collection]
        self._w_from = nfrom

        # hash the keys because each vert's key could be >= half the max key size
        self["_key"] = shake256hex(f"{self._w_from._key}+{self._w_to._key}", 64)
        self["_from"] = self._w_from._id
        self["_to"] = self._w_to._id

    def insert_in_db(self, db):
        """Uses the graph API!"""
        graph = db.graph(_graph_name)
        edges = graph.edge_collection(self._w_collection)
        edges.insert(self)

### DOCUMENT TYPES
# NOTE
# please try to keep all _key values constant length if possible
# notable exceptions include gaurantees like WST `language` names length

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

    def insert_in_db(self, db: Union[StandardDatabase, BatchDatabase]):
        """WSTTexts might be duplicate

        We prevent errors during insert if the document already exists:
        They are perfectly equivalent, enforced by the key having the hash of text,
        thus we will never lose data with overwrite
        """
        assert self._key
        coll = db.collection(self._collection)
        return coll.insert(self.__dict__, overwrite_mode="replace")

class WSTNode(WST_Document):
    """A single node in the Abstract or Concrete syntax tree

    Basically copies the data structure from tree-sitter
    """
    _collection = "wst_nodes"
    _edge_to = {
        WSTText._collection: "wst-node-text", # singular
        _collection: "wst-node-parent", # singular, own-kind
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

        # so that we can build _key / _id to a CodeTree without a lookup,
        # this needs to match WSTCodeTree.content_hash
        "content_hash", # 128 hex chars
    ]

    def _genkey(self):
        self._key = f"{shake256hex(self.path, 32)}-{self.content_hash}"
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
