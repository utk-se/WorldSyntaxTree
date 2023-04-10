"""
This "command" is only to be run as a job on the DA cluster, not used as a subcommand.
"""

import argparse
import subprocess
from typing import List, Optional
from pathlib import Path

import networkx as nx
from networkx.readwrite import json_graph
import orjson

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.hashtypes import WSTNodeHashV1

from wsyntree_collector.file.parse_file_treesitter import build_networkx_graph
from wsyntree_collector.wociterators import all_blobs as all_blobs_iterator

#import oscar


output_root_dir = Path("/da7_data/WorldSyntaxTree/nhv1/blobfuncs_v0")
assert output_root_dir.is_dir(), f"Output directory does not exist."


def get_filenames_for_blob(b: str) -> List[Path]:
    """Inefficient version: calls lookup b2f because I am lazy to rewrite it in python"""
    c = subprocess.run(
        ["bash", getValuesScript, "b2f"],
        stdin=f"{b}\n",
        text=True,
        capture_output=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser("WST Collector NHV1 BlobFuncs v0")

    args = parser.parser_args()

    print(get_filenames_for_blob(0194e44034e001afd2bdb3b54e6c11a288e25806))
