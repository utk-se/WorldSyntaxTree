
import argparse
from multiprocessing import Pool
import sys
import os

import pygit2 as git
from neomodel import config as neoconfig
import neomodel

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

from . import neo4j_db as wst_neo4jdb
from .neo4j_collector import WST_Neo4jTreeCollector

def analyze(args):
    collector = WST_Neo4jTreeCollector(args.repo_url, workers=args.workers)
    collector.setup()

    try:
        collector.collect_all()
    except neomodel.exceptions.UniqueProperty as e:
        log.err(f"{collector} already has data in the db")
        raise e

def database_indexes(args):
    if args.action == "install":
        wst_neo4jdb.setup_indexes()
        log.info(f"creation of indexes complete.")
        return
    else:
        raise NotImplementedError()

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db", "--database",
        type=str,
        help="Neo4j connection string"
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    # parser.add_argument(
    #     "--delete",
    #     help="Delete the repo from the database before running",
    #     action="store_true"
    # )
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
        'db', aliases=['database'], help="Manage the Neo4j database")
    subcmds_db = cmd_db.add_subparsers(title="Control WST indexes")
    cmd_db_index = subcmds.add_parser(
        'index', aliases=['indexes', 'idx'], help="Manage Neo4j indexes")
    cmd_db_index.set_defaults(func=database_indexes)
    cmd_db_index.add_argument(
        'action',
        choices=['install', 'drop']
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    if args.db:
        neoconfig.DATABASE_URL = args.db
    elif "NEO4J_BOLT_URL" in os.environ:
        neoconfig.DATABASE_URL = os.environ["NEO4J_BOLT_URL"]

    if 'func' not in args:
        log.warn(f"Please supply a valid subcommand!")
        return

    args.func(args)

if __name__ == '__main__':
    __main__()
