
import mongoengine
from wsyntree import log
from wsyntree.tree_models import Node

def node_matches(node, q):
    if node.name != q[0]:
        return False
    # TODO match additional attributes of this node
    if len(q) == 1:
        return True
    else:
        # TODO only place children items (sublists) into needed_matches
        needed_matches = q[1:]
        for child in node.children:
            if len(needed_matches) == 0:
                # all needed_matches consumed
                return True
            if node_matches(child, needed_matches[0]):
                needed_matches.pop(0)
        if len(needed_matches) == 0:
            # all needed_matches consumed
            return True
        return False

def filter_down(node_list, query):
    for n in node_list:
        if node_matches(n, query):
            yield n

def find_nodes_by_query(q: list):
    mongoengine.connect(host='mongodb://localhost/wsyntree')
    if len(q) == 0:
        raise ValueError("need something to search for")

    q = q[0]

    if isinstance(q[0], str):
        # initial results by matching single node type
        log.debug(f"initial filter to named nodes '{q[0]}'")
        initial_o = Node.objects(name=q[0])

    else:
        # matching by searching children of all nodes
        log.warn(f"unimplemented")
        return []

    final_o = filter_down(initial_o, q)

    return final_o
