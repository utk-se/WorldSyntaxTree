
import argparse
from multiprocessing import Pool
import sys

import pygit2 as git

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

from wsyntree_collector.neo4j_collector import WST_Neo4jTreeCollector

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
        default="bolt://localhost:7687"
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
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    collector = WST_Neo4jTreeCollector(args.repo_url, args.db)
    collector.setup()

    if args.delete:
        collector.delete_all_tree_data()
        return

    try:
        collector.collect_all()
    except KeyboardInterrupt:
        log.warn(f"cancelling collector...")
        collector.cancel()

if __name__ == '__main__':
    __main__()
