
import argparse
from multiprocessing import Pool

import pygit2 as git

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

from wsyntree_collector.mongo_collector import WST_MongoTreeCollector

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-f", "--force",
        help="DANGEROUS - Ignore / overwrite existing documents - DANGEROUS",
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
    parser.add_argument(
        "--delete-only",
        help="Delete contents of the database without regenerating",
        action="store_true"
    )
    parser.add_argument(
        "--skip-exists",
        help="Exit without error if the repo document is already present",
        action="store_true"
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    collector = WST_MongoTreeCollector(args.repo_url, args.db)
    collector.setup()

    if args.delete_only:
        collector.delete_all_tree_data()
        return

    try:
        collector.collect_all()
    except FileExistsError as e:
        if args.skip_exists:
            log.info(f"Skipping: document already present.")
            return
        if args.force:
            collector.delete_all_tree_data()
            collector.collect_all()
        else:
            raise e

if __name__ == '__main__':
    __main__()
