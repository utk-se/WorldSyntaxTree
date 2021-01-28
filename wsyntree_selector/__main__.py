
import argparse

from wsyntree import log
from wsyntree.utils import node_as_sexp

from .sexpParser import sexp
from .mongotree_matcher import find_nodes_by_query

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

    r = find_nodes_by_query(parsed_s_query)
    rl = []
    for n in r:
        log.info(f"{n} in {n.file.fetch()}")
        n_sexp = node_as_sexp(n, maxdepth=3, indent=2, show_start_coords=True)
        log.info(f"{n_sexp}")
        rl.append(n)

    log.info(f"{len(rl)} results returned")

if __name__ == '__main__':
    __main__()
