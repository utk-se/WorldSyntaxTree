
import os
from pathlib import Path
import argparse
import cProfile
import uuid
from multiprocessing import Queue, Manager

from neomodel import config as neoconfig

from wsyntree import log
from wsyntree.utils import node_as_sexp
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.tree_models import (
    SCM_Host, WSTRepository, WSTFile, WSTNode, WSTText, WSTIndexableText, WSTHugeText
)
from wsyntree_collector import neo4j_collector_worker as wst_n4j_worker

log.setLevel(log.DEBUG)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-l", "--language",
        type=str,
        help="Language to parse",
        required=True
    )
    parser.add_argument(
        "file_path",
        type=str,
        help="File to parse"
    )

    args = parser.parse_args()

    if "NEO4J_BOLT_URL" in os.environ:
        neoconfig.DATABASE_URL = os.environ["NEO4J_BOLT_URL"]

    lang = TreeSitterAutoBuiltLanguage(args.language)

    tree = lang.parse_file(args.file_path)

    cur = tree.walk()
    cur = TreeSitterCursorIterator(cur, nodefilter=lambda x: True)

    log.debug(cur)

    root = cur.peek()

    test_id = str(uuid.uuid4().hex)

    repo = WSTRepository(
        type='test',
        url=f"wst.tests.insertion/{test_id}",
        analyzed_commit="N/A",
        path=f"wst/tests/{test_id}",
    )
    repo.save()

    with Manager() as _mp_manager:
        _node_queue = _mp_manager.Queue()
        node_receiver = wst_n4j_worker._tqdm_node_receiver(_node_queue)

        try:
            cProfile.run(
                f'wst_n4j_worker._process_file(args.file_path, repo, node_q=_node_queue, notify_every=10)',
                "test-insertion.prof",
            )
        # except KeyboardInterrupt as e:
        #     log.warn(f"stopping collection ...")
        finally:
            _node_queue.put(None)

    # repo.delete()
