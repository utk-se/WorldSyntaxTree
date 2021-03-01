
from pathlib import Path
import time
import os
from urllib.parse import urlparse
from contextlib import nullcontext

import neo4j
from neomodel import db
from neo4j import GraphDatabase
from tqdm import tqdm
from pebble import concurrent

from wsyntree import log
from wsyntree.utils import dotdict
from wsyntree.tree_models import (
    SCM_Host, WSTRepository, WSTFile, WSTNode, WSTText, WSTIndexableText, WSTHugeText
)
from wsyntree.wrap_tree_sitter import get_TSABL_for_file

@concurrent.process
def _tqdm_node_receiver(q):
    log.debug(f"started counting added nodes from {q}")
    n = 0
    with tqdm(desc="adding nodes to db", position=1) as tbar:
        while (nc := q.get()) is not None:
            n += nc
            tbar.update(nc)
    log.debug(f"stopped counting nodes: total WSTNodes added: {n}")

def create_WSTNode(tx, data: dict) -> int:
    result = tx.run(
        """create (nn:WSTNode {
            x1: $x1, x2: $x2, y1: $y1, y2: $y2,
            named: $named, type: $type
        }) return id(nn) as node_id""",
        data
    )
    record = result.single()
    return record["node_id"]

def WSTNode_set_parent(tx, childid, parentid):
    result = tx.run(
        """match (p), (c)
        where id(c) = $childid and id(p) = $parentid
        create (c)-[r:PARENT]->(p)
        return id(r) as rel_id""",
        childid=childid,
        parentid=parentid,
    )
    record = result.single()
    return record["rel_id"]

def WSTNode_set_file(tx, nodeid, fileid):
    result = tx.run(
        """match (n:WSTNode), (f:WSTFile)
        where id(n) = $nodeid and id(f) = $fileid
        create (n)-[r:IN_FILE]->(f)
        return id(r) as rel_id""",
        {
            "nodeid": nodeid,
            "fileid": fileid,
        }
    )
    record = result.single()
    return record["rel_id"]

def WSTNode_set_text(tx, nodeid, textid):
    result = tx.run(
        """match (n:WSTNode), (t:WSTText)
        where id(n) = $nodeid and id(t) = $textid
        create (n)-[r:CONTENT]->(t)
        return id(r) as rel_id""",
        nodeid=nodeid,
        textid=textid
    )
    record = result.single()
    return record["rel_id"]

def create_WSTText(tx, text):
    n_t = "WSTIndexableText" if len(text) <= 4e3 else "WSTHugeText"
    result = tx.run(
        "create (nt:WSTText:"+n_t+""" {
            length: $length,
            text: $text
        }) return id(nt) as node_id""",
        length=len(text),
        text=text,
    )
    record = result.single()
    return record["node_id"]

def _process_file(path: Path, tree_repo: WSTRepository, *, node_q = None, notify_every: int=100):
    """Runs one file's analysis from a repo.

    node_q: push integers for counting number of added syntax nodes
    notify_every: send integer to node_q at least `notify_every` nodes inserted
    """

    file = WSTFile(
        path=str(path)
    )

    lang = get_TSABL_for_file(file.path)
    if lang is None:
        # log.debug(f"no language available for {file}")
        file.error = "NO_LANGUAGE"
        file.save()
        file.repo.connect(tree_repo)
        return file
    else:
        file.language = lang.lang
        file.save()
        file.repo.connect(tree_repo)

    tree = lang.parse_file(file.path)

    n4j_uri = urlparse(os.environ.get("NEO4J_BOLT_URL") or "bolt://neo4j:neo4j@localhost:7687")
    auth = (n4j_uri.username, n4j_uri.password) if n4j_uri.username else None
    uri_noauth = f"{n4j_uri.scheme}://{n4j_uri.hostname}:{n4j_uri.port}"
    # log.debug(f"parsed URI {uri_noauth}")
    driver = GraphDatabase.driver(
        uri_noauth,
        auth=auth,
    )

    # log.debug(f"growing nodes for {file}")
    cursor = tree.walk()
    # iteration loop
    nc = 0
    parent_stack = []
    try:
        with driver.session() as session, session.begin_transaction() as tx:
            # definitions: nn = new node, nt = new text, nc = node count
            while cursor.node is not None:
                cur_node = cursor.node
                nnd = dotdict({
                    "named": cur_node.is_named,
                    "type": cur_node.type,
                })
                (nnd.x1,nnd.y1) = cur_node.start_point
                (nnd.x2,nnd.y2) = cur_node.end_point

                parentid = None
                if len(parent_stack) > 0:
                    parentid = parent_stack[-1]

                # insert data into the database
                with nullcontext():
                    nnid = create_WSTNode(tx, nnd)
                    WSTNode_set_file(tx, nnid, file.id)

                    if parentid is not None:
                        WSTNode_set_parent(tx, nnid, parentid)
                    # text storage
                    try:
                        decoded_content = cur_node.text.tobytes().decode()
                        ntid = create_WSTText(tx, decoded_content)
                        WSTNode_set_text(tx, nnid, ntid)
                    except UnicodeDecodeError as e:
                        log.warn(f"{file}:{nn} failed to decode content")
                        file.error = "UNICODE_DECODE_ERROR"
                        file.save()
                        return file
                # end transaction

                # nn = inflate_WSTNode(nnid)

                # progress reporting: desired to evaluate node insertion performance
                nc += 1
                if node_q and nc >= notify_every:
                    node_q.put(nc)
                    nc = 0

                # now determine where to move to next:
                next_child = cursor.goto_first_child()
                if next_child == True:
                    parent_stack.append(nnid)
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
                        if node_q:
                            node_q.put(nc)
                        if len(parent_stack) != 0:
                            log.err(f"Bad tree iteration detected! Recorded more parents than ascended.")
                        return file
    except Exception as e:
        file.error = str(e)
        file.save()
        raise e
    finally:
        driver.close()
