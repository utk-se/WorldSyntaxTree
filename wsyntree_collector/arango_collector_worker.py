
from pathlib import Path
import time
import os
from urllib.parse import urlparse
from contextlib import nullcontext
import functools

from arango import ArangoClient
import enlighten
from pebble import concurrent
import cachetools.func

from wsyntree import log, tree_models
from wsyntree.utils import dotdict, strip_url, sha1hex, sha512hex
from wsyntree.tree_models import * # __all__
from wsyntree.wrap_tree_sitter import get_TSABL_for_file

@concurrent.process
def _tqdm_node_receiver(q, en_manager):
    """This is the cross-process aggregator for non-required data

    Even without this process the collection and analysis should run normally.
    It's mostly just used for debugging and informational output.
    """
    try:
        log.debug(f"start counting db inserts...")
        n = 0
        cache_stats = {
            "text_lfu_hit": 0,
            "text_lfu_miss": 0,
        }
        cntr = en_manager.counter(
            desc="writing to db", position=1, unit='docs',
        )
        # with tqdm(desc="writing documents to db", position=1, unit='docs', unit_scale=True) as tbar:
        while (nc := q.get()) is not None:
            if type(nc) == int:
                n += nc
                cntr.update(nc)
            elif nc[0] == "cache_stats":
                for k, v in nc[1].items():
                    cache_stats[k] += v
            else:
                log.error(f"node receiver process got invalid data sent of type {type(nc)}")
        log.info(f"stopped counting nodes, total documents inserted: {n}")
        cache_text_lfu_ratio = cache_stats["text_lfu_hit"] / (cache_stats["text_lfu_miss"] or 1)
        log.debug(f"text_lfu cache stats: ratio {cache_text_lfu_ratio}, hit {cache_stats['text_lfu_hit']}")
        return True
    except Exception as e:
        # need to print here, otherwise failure is silent if parent doesn't check the future
        log.err(f"node_receiver failed: {e}")
        raise e

def batch_insert_WSTNode(db, stuff_to_insert):
    with db.begin_batch_execution() as bdb:
        for thing in stuff_to_insert:
            thing.insert_in_db(bdb)

def process_file(*args, **kwargs):
    try:
        return _process_file(*args, **kwargs)
    except Exception as e:
        log.err(f"process_file error: {e}")
        raise e

def _process_file(
        file: WSTFile,
        tree_repo: WSTRepository,
        database_conn_str: str,
        *,
        node_q = None,
        en_manager = None,
        batch_write_size=1000,
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
    edge_fromrepo = db.graph(tree_models._graph_name).edge_collection('wst-fromrepo')

    lang = get_TSABL_for_file(file.path)
    file.language = lang.lang if lang else None

    file.insert_in_db(db)
    # file.repo.connect(tree_repo)
    edge_fromrepo.insert(file / tree_repo)

    if lang is None:
        return file

    tree = lang.parse_file(file.path)

    # log.debug(f"growing nodes for {file.path}")
    t_start = time.time()
    t_notified = False
    cursor = tree.walk()
    # memoization of WSTTexts
    known_exists_text_ids = set()
    memoiz_stats = [0, 0]
    # iteration loop
    preorder = 0
    order_to_id = {}
    parent_stack = []
    batch_writes = []
    try:
        def get_or_create_WSTText(text_key: str, length: int, content: str):
            nt = WSTText.get(db, text_key)
            if nt is None:
                nt = WSTText(
                    _key=text_key,
                    length=textlength,
                    text=text,
                )
            batch_writes.append(nt)
            return nt

        # definitions: nn = new node, nt = new text, nc = node count
        while cursor.node is not None:
            cur_node = cursor.node
            nn = WSTNode(
                _key=f"{tree_repo.commit}-{file.oid}-{sha1hex(file.path)}-{preorder}",
                named=cur_node.is_named,
                type=cur_node.type,
                preorder=preorder,
            )
            (nn.x1,nn.y1) = cur_node.start_point
            (nn.x2,nn.y2) = cur_node.end_point
            parentorder = parent_stack[-1] if parent_stack else None

            # bail if we can't decode text
            try:
                text = cur_node.text.tobytes().decode()
                textlength = len(text)
            except UnicodeDecodeError as e:
                log.warn(f"{file}: failed to decode content")
                file.error = "UnicodeDecodeError"
                file.update_in_db(db)
                return file # ends process

            # nn.insert_in_db(bdb)
            batch_writes.append(nn)
            order_to_id[nn.preorder] = nn._id
            # edge_fromfile.insert(nn / file)
            batch_writes.append(nn / file)
            if parentorder is not None:
                # edge_nodeparent.insert(nn / order_to_id[parentorder])
                batch_writes.append(nn / order_to_id[parentorder])

            # text storage (deduplication)
            text_key = f"{textlength}-{sha512hex(cur_node.text.tobytes())}"
            text_id = f"{WSTText._collection}/{text_key}"
            if text_id not in known_exists_text_ids:
                nt = get_or_create_WSTText(text_key, textlength, text)
                known_exists_text_ids.add(nt._id)
                memoiz_stats[1] += 1
            else:
                memoiz_stats[0] += 1
            # link node -> text
            batch_writes.append(nn / text_id)

            if len(batch_writes) >= batch_write_size:
                # log.debug(f"batch insert {len(batch_writes)}...")
                batch_insert_WSTNode(db, batch_writes)
                # progress reporting: desired to evaluate node insertion performance
                if node_q:
                    node_q.put(len(batch_writes))
                if not t_notified and time.time() > t_start + (30*60):
                    log.warn(f"{file.path}: processing taking longer than expected, preorder at {preorder}")
                    t_notified = True
                batch_writes = []

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
                        batch_insert_WSTNode(db, batch_writes)
                        if node_q:
                            node_q.put(len(batch_writes))
                    # NOTE sucessful end of processing
                    # log.debug(f"{file.path} added {preorder} nodes")
                    if node_q:
                        node_q.put((
                            "cache_stats",
                            {
                                "text_lfu_hit": memoiz_stats[0],
                                "text_lfu_miss": memoiz_stats[1],
                            }
                        ))
                    return file # end process
    except Exception as e:
        file.error = str(e)
        file.update_in_db(db)
        log.err(f"hmm: {e}")
        raise e
    # finally:
    #     log.info(f"file {file.path} done")
