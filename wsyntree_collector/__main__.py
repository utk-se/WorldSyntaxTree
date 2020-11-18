
import argparse

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

from wsyntree_collector.file.parse_file_treesitter import build_dask_dataframe_for_file

def __main__():
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
        "-o", "--output",
        type=str,
        help="File to write to",
        default=None
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

    lang = TreeSitterAutoBuiltLanguage(args.language)

    df = build_dask_dataframe_for_file(lang, args.file_path)

    print(df)
    print(df.head())

    if args.output:
        df.to_csv(args.output, single_file=True)

if __name__ == '__main__':
    __main__()
