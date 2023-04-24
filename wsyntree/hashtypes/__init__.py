"""
# Why are there multiple node hash types?

Multiple types are needed because different properties can be included or excluded
from the hash, e.g. to include or not to include named nodes.


"""
from enum import Enum
import hashlib
import functools

from wsyntree import log

import orjson
import networkx as nx

class WSTNodeHashV1():
    __included__ = ["named", "type"]
    def __init__(self, G, node):
        """"""
        self._graph = G
        self._node = node
        self._nodes = []
        nodes = nx.dfs_preorder_nodes(G, source=node)
        for node in nodes:
            self._nodes.append(node)

    @functools.lru_cache(maxsize=None) # functools.cache added in 3.9
    def _get_hashable_repr(self):
        s = bytearray(b"WSTNodeHashV1<")
        nodedata = list(map(
            lambda x: {k:v for k,v in x.items() if k in self.__included__},
            [self._graph.nodes[n] for n in self._nodes]
        ))
        # s += ",".join([f"{list(nd.items())}" for nd in nodedata])
        # we must sort keys here in case a python version is used that does not
        # preserve dict ordering is used
        s += orjson.dumps(nodedata, option=orjson.OPT_SORT_KEYS)
        # log.debug(f"{self}, {nodedata}")
        s += b">"
        return s

    @property
    def _node_props(self):
        return self._graph.nodes[self._node]

    def _get_sha512_hex(self):
        h = hashlib.sha512()
        h.update(self._get_hashable_repr())
        return h.hexdigest()

    def __str__(self):
        return f"WSTNodeHashV1"

# Once defined here the behavior should not change (stable versions)
class WSTNodeHashType(Enum):
    # V1 = WSTNodeHashV1
    pass
