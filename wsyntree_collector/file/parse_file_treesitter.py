
from itertools import chain
from pathlib import Path

# from dask import dataframe as dd
# from dask import bag as db

import networkx as nx

from wsyntree import log
from wsyntree.exceptions import RootTreeSitterNodeIsError
from wsyntree.utils import dotdict
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

# def build_dask_dataframe_for_file(lang: TreeSitterAutoBuiltLanguage, file: str):
#     tree = lang.parse_file(file)
#     cur = tree.walk()
#     # cur = TreeSitterCursorIterator(cur, nodefilter=lambda x: x.is_named)
#     cur = TreeSitterCursorIterator(cur)
#
#     log.debug(f"{cur}")
#
#     cols = ["repo", "file", "x1", "y1", "x2", "y2", "type", "text"]
#
#     nl = []
#
#     for node in cur:
#         # log.trace(log.debug, f"{node.type}: {node.text.tobytes().decode('utf-8')}")
#         nl.append(
#             [-1, file, *node.start_point, *node.end_point, node.type, node.text.tobytes()]
#         )
#
#     ndb = db.from_sequence(nl)
#     ndf = ndb.to_dataframe(columns=cols)
#
#     return ndf.persist().repartition(1)

def build_networkx_graph(
        lang: TreeSitterAutoBuiltLanguage,
        file: Path,
        only_named_nodes: bool = False,
        include_text: bool = False,
        node_name_prefix="",
    ):
    tree = lang.parse_file(file)
    cur = tree.walk()

    if only_named_nodes:
        cursor = TreeSitterCursorIterator(cur, nodefilter=lambda x: x.is_named)
    else:
        cursor = TreeSitterCursorIterator(cur)

    G = nx.DiGraph(lang=lang.lang)

    parent_stack = []
    ts_id_to_preorder = {}

    root = cursor.peek()
    if root.type == "ERROR":
        raise RootTreeSitterNodeIsError(f"the file content or language is likely wrong for this parser")
    # ts_id_to_preorder[root.id] = 0

    for cur_node in chain([root], cursor):
        preorder = cursor._preorder

        nn = dotdict({
            # preorder=preorder,
            "id": cur_node.id,
            "named": cur_node.is_named,
            "type": cur_node.type,
        })
        (nn.x1,nn.y1) = cur_node.start_point
        (nn.x2,nn.y2) = cur_node.end_point

        ts_id_to_preorder[cur_node.id] = preorder
        parent_order = parent_stack[-1] if parent_stack else None

        if include_text:
            try:
                nn.text = cur_node.text.decode()
            except:
                log.warn(f"Cannot decode text.")

        log.debug(f"adding node {preorder}: {nn}")
        # insert node and it's data
        G.add_node(preorder, **nn)

        # add the edge
        if cur_node.parent is not None:
            parent_preorder = ts_id_to_preorder[cur_node.parent.id]
            log.debug(f"connecting node {preorder}, to {parent_preorder}")
            G.add_edge(
                parent_preorder,
                preorder
            )

    return G
