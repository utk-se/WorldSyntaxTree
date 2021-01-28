
from pathlib import Path
import argparse

from wsyntree import log
from wsyntree.utils import node_as_sexp
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

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

    lang = TreeSitterAutoBuiltLanguage(args.language)

    tree = lang.parse_file(args.file_path)

    cur = tree.walk()
    cur = TreeSitterCursorIterator(cur, nodefilter=lambda x: x.is_named)

    log.debug(cur)

    root = cur.peek()

    for node in cur:
        log.info(node)

    log.info(node_as_sexp(root))
