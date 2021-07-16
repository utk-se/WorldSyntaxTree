
import argparse
from pathlib import Path
import time
import os
from urllib.parse import urlparse
from contextlib import nullcontext
import functools
import hashlib
import traceback

import pygit2 as git
from arango.database import StandardDatabase, BatchDatabase
import arango.exceptions
from arango import ArangoClient
import enlighten
from pebble import concurrent
import cachetools.func
from tenacity import retry
# from tenacity.stop import stop_after_attempt
# from tenacity.wait import wait_random
# from tenacity.retry import retry_if_exception_type

from wsyntree import log, tree_models
from wsyntree.exceptions import *
from wsyntree.utils import dotdict, strip_url, sha1hex, sha512hex
from wsyntree.tree_models import * # __all__
from wsyntree.wrap_tree_sitter import get_TSABL_for_file

from wsyntree_collector.jsonl_writer import WST_FileExporter as WSTFE

_HASH_CHUNK_READ_SIZE_BYTES = 2 ** 16 # 64 KiB


def process_file(*args, **kwargs):
    try:
        return _process_file(*args, **kwargs)
    except Exception as e:
        log.err(f"process_file error: {type(e)}: {e}")
        log.trace(log.debug, traceback.format_exc())
        raise e

def _process_file(
        file: WSTFile,
        export_q,
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

    # always done for every file:
    file_shake_256 = hashlib.shake_256() # WST hashes
    _filepath = Path(file.path)
    if file.mode in (git.GIT_FILEMODE_BLOB, git.GIT_FILEMODE_BLOB_EXECUTABLE):
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
        if not _filepath.is_symlink():
            raise LocalCopyOutOfSync(f"{file.path} is not a link but should be!")
        target = Path(os.readlink(_filepath))
        file.symlink = {
            'target': str(target),
        }
        abspath = _filepath.resolve(strict=False)
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

    # TODO these go at end
    # export_q.put(file)
    # export_q.put(wst_commit / file) # commit -> file

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
    code_tree._genkey()
    # TODO these go at end
    # export_q.put(code_tree)
    # export_q.put(file / code_tree)

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
                export_q.put(code_tree)
                return file # ends process

            export_q.put(nn)
            order_to_id[nn.preorder] = nn._id
            if parentorder is not None:
                # parent node -> child
                export_q.put(WST_Edge(order_to_id[parentorder], nn))
            else:
                # if it is none, this is the root node, link it
                export_q.put(code_tree / nn)

            # text storage (deduplication)
            nt = WSTText(
                length=textlength,
                text=text,
            )
            nt._genkey()
            if nt._id not in known_exists_text_ids:
                export_q.put(nt)
                known_exists_text_ids.add(nt._id)
                memoiz_stats[1] += 1
            else:
                memoiz_stats[0] += 1
            # link node -> text
            export_q.put(nn / nt)

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
                    # NOTE successful end of processing
                    # log.debug(f"{file.path} added {preorder} nodes")
                    if node_q:
                        node_q.put((
                            "cache_stats",
                            {
                                "text_lfu_hit": memoiz_stats[0],
                                "text_lfu_miss": memoiz_stats[1],
                            }
                        ))
                    # unset error: CodeTree is completed successfully
                    code_tree.error = None
                    export_q.put(code_tree)
                    return file # end process / everything went smoothly
    except BrokenPipeError as e:
        log.warn(f"caught {type(e)}: {e}")
        os._exit(1)
    except Exception as e:
        code_tree.error = str(e)
        export_q.put(code_tree)
        log.err(f"WSTNode generation failed: {e}")
        raise e
    # finally:
    #     log.info(f"file {file.path} done")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        type=str,
        help="File to parse",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        help="Output directory",
        default="output-jsonl",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
    )

    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)

    lang = get_TSABL_for_file(args.file)

    nf = WSTFile(
        _key="wst0test0461b1c841f897cbd952354370471a64-1",
        git_oid="testwst1",
        path=args.file,
        language=lang.lang,
        mode=git.GIT_FILEMODE_BLOB,
    )

    fe = WSTFE(args.output_dir, delete_existing=True)

    _process_file(nf, fe)
