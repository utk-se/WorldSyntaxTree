
import argparse

from dask import dataframe as dd

from wsyntree import log

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file_path",
        type=str,
        help="Input file"
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

    df = dd.read_csv(args.file_path)

    print(df)
    print(df.head())

if __name__ == '__main__':
    __main__()
