
import os
from pathlib import Path
import argparse
import cProfile
import uuid
from multiprocessing import Queue, Manager
from urllib.parse import urlparse

from arango import ArangoClient

from wsyntree import log
from wsyntree.utils import node_as_sexp, strip_url
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.tree_models import *
from wsyntree_collector import arango_collector_worker as wst_arango_worker


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
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
    )
    parser.add_argument(
        "--db", "--database",
        type=str,
        help="Database connection string",
        default=os.environ.get('WST_DB_URI', "http://wst:wst@localhost:8529/wst")
    )

    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)

    client = ArangoClient(hosts=strip_url(args.db))
    p = urlparse(args.db)
    db = client.db(p.path[1:], username=p.username, password=p.password)

    lang = TreeSitterAutoBuiltLanguage(args.language)

    tree = lang.parse_file(args.file_path)

    cur = tree.walk()
    cur = TreeSitterCursorIterator(cur, nodefilter=lambda x: True)

    log.debug(cur)

    root = cur.peek()

    test_id = str(uuid.uuid4().hex)

    repo = WSTRepository(
        _key="wst0test0461b1c841f897cbd952354370471a64",
        type='test',
        url=f"wst.tests.insertion/{test_id}",
        commit="wst0test0461b1c841f897cbd952354370471a64",
        path=f"wst/tests/{test_id}",
    )
    repo.insert_in_db(db)
    file = WSTFile(
        _key="wst0test0461b1c841f897cbd952354370471a64-0",
        oid="testwst0",
        path=args.file_path,
        language=args.language,
    )

    with Manager() as _mp_manager:
        _node_queue = _mp_manager.Queue()
        node_receiver = wst_arango_worker._tqdm_node_receiver(_node_queue)

        try:
            r = cProfile.run(
                f'wst_arango_worker._process_file(file, repo, args.db, node_q=_node_queue)',
                "test-insertion.prof",
            )
            log.info(f"{r}")
        # except KeyboardInterrupt as e:
        #     log.warn(f"stopping collection ...")
        finally:
            _node_queue.put(None)

    # repo.delete()
