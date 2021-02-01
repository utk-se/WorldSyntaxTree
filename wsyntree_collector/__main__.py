
import argparse
from multiprocessing import Pool
import sys

import pygit2 as git
from neomodel import config as neoconfig
import neomodel

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

from . import neo4j_db as wst_neo4jdb
from .neo4j_collector import WST_Neo4jTreeCollector

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "repo_url",
        type=str,
        help="URI for cloning the repository"
    )
    parser.add_argument(
        "--db", "--database",
        type=str,
        help="Neo4j connection string",
        default="bolt://neo4j:neo4j@localhost:7687"
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    parser.add_argument(
        "--delete",
        help="Delete the repo from the database before running",
        action="store_true"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        help="Number of workers to use for processing files, default: os.cpu_count()",
        default=None
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    neoconfig.DATABASE_URL = args.db

    if '://' not in args.repo_url:
        if args.repo_url == "install_indexes":
            wst_neo4jdb.setup_indexes()
            log.info(f"creation of indexes complete.")
            return

    collector = WST_Neo4jTreeCollector(args.repo_url, workers=args.workers)
    collector.setup()

    if args.delete:
        collector.delete_all_tree_data()
        return

    try:
        collector.collect_all()
    except neomodel.exceptions.UniqueProperty as e:
        log.err(f"{collector} already has data in the db")
        raise e
    except KeyboardInterrupt:
        log.warn(f"cancelling collector...")
        collector.cancel()

if __name__ == '__main__':
    __main__()
