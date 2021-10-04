
# WorldSyntaxTree

WorldSyntaxTree is a library, a collection of CLI utilities, and a research-oriented tool for exploring code across language boundaries and at incredibly large scale.

Works across languages: the basis for generating WST tree (graph) data is [Tree-Sitter](https://tree-sitter.github.io/tree-sitter/). It's key advantages include it's ability to output the same style of graph for any language, and the fact it's trees are concrete, meaning we can explore all components of the observed software and not just the abstract ones.

Scales to incredible size: the goal is to build a system capable of storing, parsing, and searching the entirety of the open source ecosystem that is tracked in the Git VCS. (Including as much of GitHub, GitLab, etc that we can gather on our hardware.)

## What's it built on?

WorldSyntaxTree is built upon the following technologies:

- Python: we chose this as it is quick to develop and has a large ecosystem of scientific libraries that we aim to be able to support integration with. In our field of research Python is the most popular to use for quickly wrangling large and complex datasets.
- [Tree-Sitter](https://tree-sitter.github.io/tree-sitter/): this is the integral component to enable us to generate the concrete syntax trees from code quickly while still generating a somewhat useful result even on broken code.
- [ArangoDB](https://www.arangodb.com/): our choice of database stemmed from the following requirements:
  - Must be open source (free for use and improvement by all)
  - Must support our incredibly large data size (many terabytes)
  - Must have native/serverside graph processing capabilities
- Git: the outer / top-level structure for the whole tree is based upon Git's structure of repositories, commits, and files, thus we aren't currently exploring other VCS systems (though we might in the far future)

For a full list of libraries used, check the `setup.py` or `requirements.txt`.

## What's included?

### The WST Library

The `wsyntree` library is the interface allowing other Python programs to interface with WST data in various formats.

### The WST Tooling

Included as part of the standard package are the `collector` and the `selector`. Right now only the `collector` part is functional for usage.

In the context of WST, the 'collector' refers to the program which takes git repositories, parses them, and outputs their parsed content to the DB or other formats.

## Tree structure

The WorldSyntaxTree is stored in a few parts based on the type of branch or leaf in the tree:

Repositories are the highest level of the tree, they are the most coarse filtering you can do on the tree. Repositories point to their commits, and commits reference each other much akin to how commits in git reference eachother.

Each commit points to a set of Files which include all the files present if you were to checkout that specific commit.

Instead of storing File content within the tree we instead point from a File to a CodeTree, which is the concrete syntax tree for the file. (If it can be parsed, otherwise only the file metadata is stored.)

The CodeTree is the parent node to all of the concrete syntax nodes produced from Tree-Sitter, and thus forms the most useful part of the data. Individual subgraphs or graph patterns can be searched for using ArangoDB as a backend to allow for larger-than-memory datasets to be queried.

To see the actual complete tree structure look at [the tree_models definition](wsyntree/tree_models.py).

## Installing

Requirements:

- Standard development tooling (git, pip, python-dev, setuptools, etc)
- C++ compiler (ubuntu: `libc++-dev libc++abi-dev`, plus any other dependencies needed for WST to auto-compile Tree-Sitter languages)
- Python 3.8+
- Optional: an ArangoDB instance

Install steps:

```
python -m pip install -r requirements.txt
python setup.py install
```
