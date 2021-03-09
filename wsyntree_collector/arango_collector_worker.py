
from pathlib import Path
import time
import os
from urllib.parse import urlparse
from contextlib import nullcontext

from arango import ArangoClient
from tqdm import tqdm
from pebble import concurrent

from wsyntree import log
from wsyntree.utils import dotdict, strip_url
from wsyntree.tree_models import * # __all__
from wsyntree.wrap_tree_sitter import get_TSABL_for_file

@concurrent.process
def _tqdm_node_receiver(q):
    log.debug(f"started counting added nodes")
    n = 0
    with tqdm(desc="adding nodes to db", position=1, unit='nodes', unit_scale=True) as tbar:
        while (nc := q.get()) is not None:
            n += nc
            tbar.update(nc)
    log.info(f"stopped counting nodes, total WSTNodes added: {n}")

# def create_WSTNode_root(tx, data: dict) -> int:
#     """Exactly the same as create_WSTNode, but with no parent node
#
#     Should only ever happen once per file. (the root / first node)
#     """
#     text = data['text']
#     n_t = "WSTIndexableText" if len(text) <= 4e3 else "WSTHugeText"
#     q = """
#     match (f:WSTFile)
#     where id(f) = $fileid
#     create (nn:WSTNode {
#         x1: $x1, x2: $x2, y1: $y1, y2: $y2,
#         named: $named, type: $type, preorder: $preorder
#     })-[:IN_FILE]->(f), (nn)-[:CONTENT]->(t:WSTText:"""+n_t+""" {
#         length: $textlength,
#         text: $text
#     })
#     return id(nn) as node_id"""
#     result = tx.run(q, data)
#     record = result.single()
#     return record["node_id"]

# # milliseconds timeout
# @neo4j.unit_of_work(timeout=30 * 60 * 1000)
# def managed_batch_insert(tx, entries: list, order_to_id_o: dict) -> dict:
#     order_to_id = order_to_id_o.copy() # stay pure until tx successful
#     qi = """
#     unwind $entries as data
#     match (f:WSTFile)
#     where id(f) = data.fileid
#     create (f)<-[:IN_FILE]-(c:WSTNode {
#         x1: data.x1, x2: data.x2, y1: data.y1, y2: data.y2,
#         named: data.named, type: data.type, preorder: data.preorder
#     })-[:CONTENT]->(t:WSTText {
#         length: data.textlength,
#         text: data.text
#     })
#     return c.preorder as preorder, id(c) as cid, data.parentorder as parentorder order by c.preorder
#     """
#     nresults = tx.run(qi, {"entries": entries})
#
#     nvals = nresults.data()
#     assert len(nvals) == len(entries), f"Ensure all nodes were created."
#
#     # use a dict for constant-time order to nodeid:
#     # create a list of all the parent connections we need to make:
#     rp_list = []
#     for v in nvals:
#         order_to_id[v['preorder']] = v['cid']
#         rp_list.append({
#             "cid": v['cid'],
#             "pid": order_to_id[v['parentorder']],
#         })
#
#     qr = """
#     unwind $connectlist as ctpi
#     match (c:WSTNode), (p:WSTNode)
#     where id(c) = ctpi.cid and id(p) = ctpi.pid
#     create (c)-[r:PARENT]->(p)
#     return id(c) as cid, id(p) as pid, id(r) as rid order by c.preorder
#     """
#     rresults = tx.run(qr, {"connectlist": rp_list})
#
#     rvals = rresults.data()
#     # assert len(nvals) == len(rvals), f"Ensure every child gets a parent node relation."
#     assert len(rvals) == len(entries), f"batch write size {len(entries)} made {len(rvals)} parent connections"
#     for v in rvals:
#         assert v['rid']
#
#     return order_to_id

# def batch_insert_WSTNode(session, entries: list, order_to_id: dict) -> int:
#     n_addt = session.write_transaction(managed_batch_insert, entries, order_to_id)
#     order_to_id.update(n_addt)

def batch_insert_WSTNode(db, nodes: list):
    pass

def _process_file(
        file: WSTFile,
        tree_repo: WSTRepository,
        database_conn_str: str,
        *,
        node_q = None,
        batch_write_size=100,
    ):
    """Runs one file's analysis from a repo.

    node_q: push integers for counting number of added syntax nodes
    notify_every: send integer to node_q at least `notify_every` nodes inserted
    """

    ur = urlparse(database_conn_str)
    _db_username = ur.username or 'wst'
    _db_password = ur.password or 'wst'
    _db_database = ur.path[1:] or 'wst'
    client = client = ArangoClient(
        hosts=strip_url(database_conn_str),
    )
    db = client.db(
        _db_database,
        username=_db_username,
        password=_db_password,
    )
    graph = db.graph('wst')
    edge_fromrepo = graph.edge_collection('wst-fromrepo')
    edge_fromfile = graph.edge_collection('wst-fromfile')
    edge_nodeparent = graph.edge_collection('wst-nodeparent')
    edge_nodetext = graph.edge_collection('wst-nodetext')

    lang = get_TSABL_for_file(file.path)
    file.language = lang.lang if lang else None

    file.insert_in_db(db)
    # file.repo.connect(tree_repo)
    edge_fromrepo.insert(file / tree_repo)

    if lang is None:
        return file

    tree = lang.parse_file(file.path)

    # log.debug(f"growing nodes for {file}")
    t_start = time.time()
    t_notified = False
    cursor = tree.walk()
    # iteration loop
    preorder = 0
    order_to_id = {}
    parent_stack = []
    root_written = False
    batch_writes = []
    try:
        # with driver.session() as session:
        with nullcontext():
            # definitions: nn = new node, nt = new text, nc = node count
            while cursor.node is not None:
                cur_node = cursor.node
                nn = WSTNode(
                    _key=f"{tree_repo.commit}-{file.oid}-{preorder}",
                    named=cur_node.is_named,
                    type=cur_node.type,
                    preorder=preorder,
                )
                (nn.x1,nn.y1) = cur_node.start_point
                (nn.x2,nn.y2) = cur_node.end_point

                # bail if we can't decode text
                try:
                    text = cur_node.text.tobytes().decode()
                    textlength = len(text)
                except UnicodeDecodeError as e:
                    log.warn(f"{file}: failed to decode content")
                    file.error = "UnicodeDecodeError"
                    file.update_in_db(db)
                    return file # ends process

                nn.insert_in_db(db)
                node_q.put(1)
                order_to_id[nn.preorder] = nn._key
                edge_fromfile.insert(nn / file)
                # if root_written:
                #     nnd.parentorder = parent_stack[-1]
                #     batch_writes.append(nnd)
                #     # batch write check:
                #     if len(batch_writes) >= batch_write_size:
                #         # log.debug(f"batch insert {len(batch_writes)}...")
                #         batch_insert_WSTNode(session, batch_writes, order_to_id)
                #         # progress reporting: desired to evaluate node insertion performance
                #         if node_q:
                #             node_q.put(len(batch_writes))
                #             if not t_notified and time.time() > t_start + (30*60):
                #                 log.warn(f"{file}: processing taking longer than expected.")
                #                 t_notified = True
                #         batch_writes = []
                # else:
                #     # log.debug("writing root node...")
                #     # root node is a different query
                #     with session.begin_transaction() as tx:
                #         nnid = create_WSTNode_root(tx, nnd)
                #         order_to_id[0] = nnid
                #     root_written = True

                preorder += 1

                # now determine where to move to next:
                next_child = cursor.goto_first_child()
                if next_child == True:
                    parent_stack.append(nn.preorder)
                    continue # cur_node to next_child
                next_sibling = cursor.goto_next_sibling()
                if next_sibling == True:
                    continue # cur_node to next_sibling
                # go up parents
                while cursor.goto_next_sibling() == False:
                    goto_parent = cursor.goto_parent()
                    if goto_parent:
                        parent_stack.pop()
                    else:
                        # we are done iterating
                        if len(parent_stack) != 0:
                            log.err(f"Bad tree iteration detected! Recorded more parents than ascended.")
                        if batch_writes:
                            batch_insert_WSTNode(session, batch_writes, order_to_id)
                            if node_q:
                                node_q.put(len(batch_writes))
                        return file # end process
    except Exception as e:
        file.error = str(e)
        file.update_in_db(db)
        raise e
    # finally:
    #     driver.close()