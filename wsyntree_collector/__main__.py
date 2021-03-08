
import argparse
from multiprocessing import Pool
import sys
import os
from urllib.parse import urlparse

import pygit2 as git
from arango import ArangoClient

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.utils import strip_url

from .arango_collector import WST_ArangoTreeCollector

def analyze(args):
    collector = WST_ArangoTreeCollector(
        args.repo_url,
        workers=args.workers,
        database_conn=args.db,
    )
    collector.setup()

    try:
        collector.collect_all()
    except Exception as e:
        log.crit(f"{collector} run failed.")
        raise e

def database_init(args):
    client = ArangoClient(hosts=strip_url(args.db))
    p = urlparse(args.db)
    db = client.db(p.path[1:], username=p.username, password=p.password)

    colls = {}
    for cn in ['wstfiles', 'wstrepos', 'wstnodes', 'wsttexts']:
        if db.has_collection(cn):
            colls[cn] = db.collection(cn)
        else:
            colls[cn] = db.create_collection(cn, user_keys=True)
    for cn in ['wst-fromfile', 'wst-fromrepo', 'wst-nodeparent', 'wst-nodetext']:
        if db.has_collection(cn):
            colls[cn] = db.collection(cn)
        else:
            colls[cn] = db.create_collection(cn, user_keys=True, edge=True)

    graph = None
    if not db.has_graph('wst'):
        graph = db.create_graph('wst')
    else:
        graph = db.graph('wst')
    edgedefs = {}
    _ngraphs = {
        "wst-repo-files": {
            "edge_collection": 'wst-fromrepo',
            "from_vertex_collections": ['wstfiles'],
            "to_vertex_collections": ['wstrepos'],
        },
        "wst-file-nodes": {
            "edge_collection": 'wst-fromfile',
            "from_vertex_collections": ['wstnodes'],
            "to_vertex_collections": ['wstfiles'],
        },
        "wst-node-parents": {
            "edge_collection": 'wst-nodeparent',
            "from_vertex_collections": ['wstnodes'],
            "to_vertex_collections": ['wstnodes'],
        },
        "wst-node-text": {
            "edge_collection": 'wst-nodetext',
            "from_vertex_collections": ['wstnodes'],
            "to_vertex_collections": ['wsttexts'],
        },
    }
    for gk, gv in _ngraphs.items():
        if not graph.has_edge_definition(gv['edge_collection']):
            log.debug(f"Added graph edges {gv}")
            edgedefs[gk] = graph.create_edge_definition(**gv)

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db", "--database",
        type=str,
        help="Database connection string",
        default=os.environ.get('WST_DB_URI', "http://wst:wst@localhost:8529/wst")
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    subcmds = parser.add_subparsers(
        title="Collector commands"
    )

    # analysis
    cmd_analyze = subcmds.add_parser(
        'analyze', aliases=['add', 'a'], help="Analyze repositories")
    cmd_analyze.set_defaults(func=analyze)
    cmd_analyze.add_argument(
        "repo_url",
        type=str,
        help="URI for cloning the repository"
    )
    cmd_analyze.add_argument(
        "-w", "--workers",
        type=int,
        help="Number of workers to use for processing files, default: os.cpu_count()",
        default=None
    )
    # db setup
    cmd_db = subcmds.add_parser(
        'db', aliases=['database'], help="Manage the database")
    subcmds_db = cmd_db.add_subparsers(title="Manage the database")
    cmd_db_init = subcmds_db.add_parser(
        'initialize', aliases=['init', 'setup'], help="Set up the database")
    cmd_db_init.set_defaults(func=database_init)
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    if 'func' not in args:
        log.warn(f"Please supply a valid subcommand!")
        return

    args.func(args)

if __name__ == '__main__':
    __main__()
