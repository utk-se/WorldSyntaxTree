
from pathlib import Path

from networkx.readwrite import json_graph
import orjson

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

from ..file.parse_file_treesitter import build_networkx_graph

def set_args(parser):
    parser.set_defaults(func=run)
    parser.add_argument("which_file", type=Path, help="Path to input file")
    parser.add_argument("-l", "--lang", help="Language parser to use")
    parser.add_argument("-o", "--output", help="File to write result to", type=Path)

def run(args):
    log.info(f"Running for file {args.which_file}")

    if not args.lang:
        raise NotImplementedError(f"Automatic detection of file language not supported. Please specify a TreeSitter parser to use.")

    lang = TreeSitterAutoBuiltLanguage(args.lang)

    graph = build_networkx_graph(lang, args.which_file)

    log.info(f"Graph result: {graph}")

    # node_link_data = json_graph.node_link_data(graph)
    # log.debug(f"Graph node_link_data: {node_link_data}")
    #
    # adjacency_data = json_graph.adjacency_data(graph)
    # log.debug(f"Graph adjacency_data: {adjacency_data}")

    # cytoscape_data = json_graph.cytoscape_data(graph)
    # log.debug(f"Graph cytoscape_data: {cytoscape_data}")

    tree_data = json_graph.tree_data(graph, 0)
    log.debug(f"Graph tree_data: {tree_data}")

    # jit_data = json_graph.jit_data(graph)
    # log.debug(f"Graph jit_data: {jit_data}")

    if args.output:
        if str(args.output) == "-":
            raise NotImplementedError(f"output to stdout not yet supported")

        log.info(f"Writing to {args.output} ...")
        with args.output.open('wb') as f:
            f.write(orjson.dumps(
                tree_data, option=orjson.OPT_APPEND_NEWLINE
            ))
