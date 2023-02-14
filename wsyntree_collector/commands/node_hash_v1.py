
from pathlib import Path

import networkx as nx
from networkx.readwrite import json_graph
import orjson

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.hashtypes import WSTNodeHashV1

from ..file.parse_file_treesitter import build_networkx_graph

def set_args(parser):
    parser.set_defaults(func=run)
    parser.add_argument("which_file", type=Path, help="Path to input file")
    parser.add_argument("what_node_type", type=str, help="What kind of nodes to hash")
    parser.add_argument("-l", "--lang", help="Language parser to use")
    parser.add_argument("-o", "--output", help="File to write result to", type=Path)

def run(args):
    log.info(f"Running for file {args.which_file}")

    if not args.lang:
        raise NotImplementedError(f"Automatic detection of file language not supported. Please specify a TreeSitter parser to use.")

    lang = TreeSitterAutoBuiltLanguage(args.lang)

    graph = build_networkx_graph(lang, args.which_file, include_text=True)

    log.info(f"Graph result: {graph}")

    log.info(f"Searching for nodes of type {args.what_node_type}")
    hashed_nodes = []
    for node in nx.dfs_preorder_nodes(graph):
        if graph.nodes[node]['type'] == args.what_node_type:
            log.debug(f"Node {node} (x1:{graph.nodes[node]['x1']}) matched type {args.what_node_type}, hashing...")
            hashed_nodes.append(WSTNodeHashV1(graph, node))

    # tree_data = json_graph.tree_data(graph, 0)
    # log.debug(f"Graph tree_data: {tree_data}")

    if args.output:
        if str(args.output) == "-":
            raise NotImplementedError(f"output to stdout not yet supported")

        log.info(f"Writing to {args.output} ...")
        with args.output.open('wb') as f:
            for nh in hashed_nodes:
                f.write(orjson.dumps({
                    "sha512": nh._get_sha512_hex(),
                    "file": str(args.which_file),
                    # obviously we want coords to allow humans to quickly find the referenced code
                    "x1": nh._node_props['x1'],
                    "y1": nh._node_props['y1'],
                    # TODO: "objectid": git-object-id, (when run in batch mode)
                }, option=orjson.OPT_APPEND_NEWLINE))
        # if str(args.output).endswith(".graphml"):
        #     log.info("Writing GraphML")
        #     nx.write_graphml(graph, args.output)
        # else:
        #     with args.output.open('wb') as f:
        #         f.write(orjson.dumps(
        #             tree_data, option=orjson.OPT_APPEND_NEWLINE
        #         ))
