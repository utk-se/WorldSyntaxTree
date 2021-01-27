
import argparse

from wsyntree import log

from .sexpParser import sexp

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "query",
        type=str,
        help="S-exp query to execute"
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

    parsed_s_query = sexp.parseString(args.query)

    log.debug(parsed_s_query)

if __name__ == '__main__':
    __main__()
