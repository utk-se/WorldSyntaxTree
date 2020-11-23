
import argparse

import pygit2 as git

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

from wsyntree_collector.mongo_collector import WST_MongoTreeCollector

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-f", "--force",
        help="DANGEROUS - Ignore / overwrite existing documents - DANGEROUS",
        required=True,
        action='store_true'
    )
    parser.add_argument(
        "repo_url",
        type=str,
        help="URI for cloning the repository"
    )
    parser.add_argument(
        "--db", "--database",
        type=str,
        help="MongoDB connection string",
        default="mongodb://localhost/wsyntree"
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    collector = WST_MongoTreeCollector(args.repo_url, args.db, force=args.force)
    collector.setup()
    collector.collect_all()

if __name__ == '__main__':
    __main__()
