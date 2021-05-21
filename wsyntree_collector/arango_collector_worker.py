
from pathlib import Path
import time
import os
from urllib.parse import urlparse
from contextlib import nullcontext
import functools
import hashlib

import pygit2 as git
import arango.exceptions
from arango import ArangoClient
import enlighten
from pebble import concurrent
import cachetools.func

from wsyntree import log, tree_models
from wsyntree.exceptions import *
from wsyntree.utils import dotdict, strip_url, sha1hex, sha512hex
from wsyntree.tree_models import * # __all__
from wsyntree.wrap_tree_sitter import get_TSABL_for_file

_HASH_CHUNK_READ_SIZE_BYTES = 2 ** 16 # 64 KiB


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
        dedup_stats = {}
        cntr = en_manager.counter(
            desc="writing to db", position=1, unit='docs', autorefresh=True
        )
        # with tqdm(desc="writing documents to db", position=1, unit='docs', unit_scale=True) as tbar:
        while (nc := q.get()) is not None:
            if type(nc) == int:
                n += nc
                cntr.update(nc)
            elif nc[0] == "cache_stats":
                for k, v in nc[1].items():
                    cache_stats[k] += v
            elif nc[0] == "dedup_stats":
                if nc[1] not in dedup_stats:
                    dedup_stats[nc[1]] = 0
                dedup_stats[nc[1]] += nc[2]
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
        log.err(f"process_file error: {type(e)}: {e}")
        raise e

def _process_file(
        file: WSTFile,
        wst_commit: WSTCommit, # the commit the file is a part of
        database_conn_str: str,
        *,
        node_q = None,
        en_manager = None,
        batch_write_size=1000,
    ):
    """Given an incomplete WSTFile,
    Creates a WSTCodeTree, WSTNodes, and WSTTexts for it

    Process working directory should already be within checked out repository

    node_q: push integers for counting number of added syntax nodes
    en_manager: Enlighten Manager compatible API to get Counters from
    batch_write_size: when number of items in memory reaches this, write them all

    Returns the completed AND inserted WSTFile, linked with wst_commit
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
    # edge_fromrepo = db.graph(tree_models._graph_name).edge_collection('wst-fromrepo')

    # always done for every file:
    file_shake_256 = hashlib.shake_256() # WST hashes
    file_git_oid = str(git.hashfile(file.path)) # transforms data with header & filters
    if file.mode in (git.GIT_FILEMODE_BLOB, git.GIT_FILEMODE_BLOB_EXECUTABLE):
        # checking if file has been modified on disk vs git:
        if file_git_oid != file.git_oid:
            # we will NOT insert this file into the db if this happens
            log.warn(f"{file.path} git oid is {file_git_oid} while expected oid is {file.git_oid}")
            log.warn(f"{file.path} mode is {oct(file.mode)}")
            raise LocalCopyOutOfSync(f"file {file.path} sha1 hash does not match git oid")
        # for normal files
        with open(file.path, 'rb') as f:
            while (data := f.read(_HASH_CHUNK_READ_SIZE_BYTES)):
                file_shake_256.update(data)
        lang = get_TSABL_for_file(file.path)
        file.language = lang.lang if lang else None
        file.error = "WST_NO_LANGUAGE" if not lang else None
        file.content_hash = file_shake_256.hexdigest(64) # 128 hex chars
    elif file.mode == git.GIT_FILEMODE_LINK:
        lang = None
        file.language = None
        file.error = "WST_IS_LINK"
        # we will not parse it, instead, store the link
        link = Path(file.path)
        if not link.is_symlink():
            raise LocalCopyOutOfSync(f"{file.path} is not a link but should be!")
        target = Path(os.readlink(link))
        file.symlink = {
            'target': str(target),
        }
        abspath = link.resolve(strict=False)
        try:
            relpath = abspath.relative_to(Path('.').resolve())
            file.symlink['relative'] = str(relpath)
        except ValueError as e:
            # link target probably not within our repo dir
            file.symlink['relative'] = None
        file_shake_256.update(str(target).encode())
        file.content_hash = file_shake_256.hexdigest(64)
    else:
        raise UnhandledGitFileMode(f"{file.path} mode is {oct(file.mode)}")

    try:
        file.insert_in_db(db)
        (wst_commit / file).insert_in_db(db) # commit -> file
    except arango.exceptions.DocumentInsertError as e:
        if e.http_code == 409:
            # already exists: get it
            preexisting_file = WSTFile.get(db, file._key)
            if preexisting_file != file:
                log.debug(f"existing file: {preexisting_file}")
                log.debug(f"new file: {file}")
                raise PrerequisiteStateInvalid(f"WSTFile {file._key} already exists but has mismatched data")
            (wst_commit / preexisting_file).insert_in_db(db)
            if node_q:
                node_q.put(('dedup_stats', 'WSTFile', 1))
            return preexisting_file
        else:
            raise e
    except Exception as e:
        log.error(f"Failed to insert WSTFile into db!")
        raise e

    if lang is None:
        # no WSTCodeTree will be generated
        return file

    # otherwise, let the parsing begin!
    code_tree = WSTCodeTree(
        language=file.language,
        lang_version=None, # TODO
        content_hash=file.content_hash,
        git_oid=file.git_oid,
        error=None,
    )
    try:
        code_tree.insert_in_db(db)
    except arango.exceptions.DocumentInsertError as e:
        if e.http_code == 409:
            # already exists: check that it's the same, and if so, all done here
            preexisting_ct = WSTCodeTree.get(db, code_tree._key)
            assert preexisting_ct == code_tree, f"constructed WSTCodeTree does not match existing, id {preexisting_ct._id}"
            (file / preexisting_ct).insert_in_db(db)
            if node_q:
                node_q.put(('dedup_stats', 'WSTCodeTree', 1))
            return file
    except Exception as e:
        log.error(f"Failed to insert WSTCodeTree into db: {code_tree}")
        raise e
    (file / code_tree).insert_in_db(db)

    tree = lang.parse_file(file.path)

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
        # definitions: nn = new node, nt = new text, nc = node count
        while cursor.node is not None:
            cur_node = cursor.node
            nn = WSTNode(
                _key=f"{code_tree._key}-{preorder}",
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
                code_tree.error = "UnicodeDecodeError"
                code_tree.update_in_db(db)
                return file # ends process

            # nn.insert_in_db(bdb)
            batch_writes.append(nn)
            order_to_id[nn.preorder] = nn._id
            # edge_fromfile.insert(nn / file)
            # batch_writes.append(nn / file)
            if parentorder is not None:
                # edge_nodeparent.insert(nn / order_to_id[parentorder])
                batch_writes.append(nn / order_to_id[parentorder])
            else:
                # if it is none, this is the root node, link it
                batch_writes.append(code_tree / nn)

            # text storage (deduplication)
            nt = WSTText(
                length=textlength,
                text=text,
            )
            nt._genkey()
            if nt._id not in known_exists_text_ids:
                batch_writes.append(nt)
                known_exists_text_ids.add(nt._id)
                memoiz_stats[1] += 1
            else:
                memoiz_stats[0] += 1
            # link node -> text
            batch_writes.append(nn / nt)

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
    except BrokenPipeError as e:
        log.warn(f"caught {type(e)}: {e}")
        os._exit(1)
    except Exception as e:
        code_tree.error = str(e)
        code_tree.update_in_db(db)
        log.err(f"WSTNode generation failed: {e}")
        raise e
    # finally:
    #     log.info(f"file {file.path} done")
