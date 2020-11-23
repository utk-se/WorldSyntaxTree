
from dask import dataframe as dd
from dask import bag as db

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

def build_dask_dataframe_for_file(lang: TreeSitterAutoBuiltLanguage, file: str):
    tree = lang.parse_file(file)
    cur = tree.walk()
    # cur = TreeSitterCursorIterator(cur, nodefilter=lambda x: x.is_named)
    cur = TreeSitterCursorIterator(cur)

    log.debug(f"{cur}")

    cols = ["repo", "file", "x1", "y1", "x2", "y2", "type", "text"]

    nl = []

    for node in cur:
        # log.trace(log.debug, f"{node.type}: {node.text.tobytes().decode('utf-8')}")
        nl.append(
            [-1, file, *node.start_point, *node.end_point, node.type, node.text.tobytes()]
        )

    ndb = db.from_sequence(nl)
    ndf = ndb.to_dataframe(columns=cols)

    return ndf.persist().repartition(1)
