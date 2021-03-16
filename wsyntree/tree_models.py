
from itertools import chain
from typing import Union
from json import JSONEncoder

from arango.database import StandardDatabase, BatchDatabase

from .utils import dotdict

__all__ = [
    'WSTRepository', 'WSTFile', 'WSTNode', 'WSTText'
]

_graph_name = 'wst'


class WST_Document():
    __slots__ = [
        "__collection",
        "_key",
    ]
    _edge_to = None

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

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
        return {s: getattr(self, s, None) for s in slots if not s.startswith('__')}

    def insert_in_db(self, db: Union[StandardDatabase, BatchDatabase]):
        """Insert this document into a db"""
        assert self._key
        coll = db.collection(self._collection)
        return coll.insert(self.__dict__)

    def update_in_db(self, db: Union[StandardDatabase, BatchDatabase]):
        assert self._key
        coll = db.collection(self._collection)
        return coll.update(self.__dict__)

    def _make_edge(self, rhs):
        return WST_Edge(self, rhs)
        # _to = None
        # if type(rhs) == str:
        #     _to = rhs
        #     _kc = rhs.split('/')[1]
        # elif isinstance(rhs, WST_Document):
        #     _to = f"{rhs._collection}/{rhs._key}"
        #     _kc = rhs._key
        # else:
        #     raise TypeError(f"Cannot create edges between {type(self)} and {type(rhs)}")
        # return {
        #     "_key": f"{self._key}+{_kc}",
        #     "_from": f"{self._collection}/{self._key}",
        #     "_to": _to,
        # }
    __floordiv__ = _make_edge
    __truediv__ = _make_edge

class WST_Edge(dict):
    # These slots are NOT part of the inserted document
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
                raise TypeError(f"type {type(nfrom)} cannot connect to document ID {to}")
            self._w_to = WST_Document(
                _collection=coll,
                _key=key,
            )
        elif isinstance(to, WST_Document) and to._collection not in nfrom._edge_to:
            raise TypeError(f"type {type(nfrom)} cannot connect to type {type(to)}")
        elif isinstance(to, WST_Document):
            self._w_to = to
        self._w_collection = nfrom._edge_to[self._w_to._collection]
        self._w_from = nfrom

        self["_key"] = f"{self._w_from._key}+{self._w_to._key}"
        self["_from"] = self._w_from._id
        self["_to"] = self._w_to._id

    def insert_in_db(self, db):
        graph = db.graph(_graph_name)
        edges = graph.edge_collection(self._w_collection)
        edges.insert(self)

# class WST_Serializer(JSONEncoder):
#     def default(self, o):
#         if isinstance(o, WST_Document):
#             return o.__dict__
#         else:
#             return JSONEncoder.default(self, o)

class WSTRepository(WST_Document):
    _collection = "wstrepos"
    __slots__ = [
        "type",
        "url",
        "path",
        "commit",
        "analyzed_time",
        "wst_status",
    ]

    # host = RelationshipTo(SCM_Host, 'HOSTED_ON', cardinality=One)

    # files = RelationshipFrom("WSTFile", 'IN_REPO')

class WSTFile(WST_Document):
    _collection = "wstfiles"
    _edge_to = {
        WSTRepository._collection: "wst-fromrepo",
    }
    __slots__ = [
        "path",
        "language",
        "error",
        "oid",
    ]

    # wstnodes = RelationshipFrom("WSTNode", 'IN_FILE')

class WSTText(WST_Document):
    _collection = "wsttexts"
    __slots__ = [
        "length",
        "text",
    ]

    # used_by = RelationshipFrom("WSTNode", 'CONTENT')

class WSTNode(WST_Document):
    _collection = "wstnodes"
    _edge_to = {
        WSTFile._collection: "wst-fromfile",
        WSTText._collection: "wst-nodetext",
        _collection: "wst-nodeparent",
    }
    __slots__ = [
        # x: line, y: character (2d coords begin->end)
        "x1",
        "y1",
        "x2",
        "y2",

        # preorder: depth-first traversal order, unique within file
        # aka: topologically sorted
        # root node is zero and has no parent
        "preorder",

        "named",
        "type",
    ]

    # file =   RelationshipTo(WSTFile, 'IN_FILE', cardinality=One)
    # parent = RelationshipTo("WSTNode", 'PARENT', cardinality=One)
    # text =   RelationshipTo(WSTText, 'CONTENT', cardinality=One)

    # children = RelationshipFrom("WSTNode", 'PARENT')
