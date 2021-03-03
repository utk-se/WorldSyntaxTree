
from pathlib import Path
import argparse

from wsyntree import log
from wsyntree.utils import node_as_sexp
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

def str_tsnode(n):
    return f"tsnode<{n.type}, {n.start_point}>"

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

    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)

    lang = TreeSitterAutoBuiltLanguage(args.language)

    tree = lang.parse_file(args.file_path)

    cur = tree.walk()
    cur = TreeSitterCursorIterator(cur, nodefilter=lambda x: True)

    log.debug(cur)

    root = cur.peek()

    for node in cur:
        print(node)
        log.info(f"{'  ' * cur.depth}{str_tsnode(node)}")
