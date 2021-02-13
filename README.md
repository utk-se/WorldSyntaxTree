# WorldSyntaxTree

Language-agnostic parsing of World of Code repositories

## Terminology / Project Structure

### Collector

In the context of this project, the 'collector' refers to the program which takes code repositories and adds their parsed content to the Tree.

An external tool such as [tree sitter](http://tree-sitter.github.io/tree-sitter/) is used to parse raw (potentially invalid) file input into a syntax tree. Specifically, we retrieve the type of token as well as the location in the file. We may discard or ignore information based on the file type or content at this stage.

The output of the parsing tool is then stored into the Tree, translated to a queryable, partially tabular format.

### Tree structure

The WorldSyntaxTree is stored in a few parts based on the type of branch or leaf in the tree:

Repositories are the highest level of the tree, they are the most coarse filtering you can do on the tree.

Below that are the files contained in the repositories, which are used as the intermediary to access individual Nodes.

Nodes of the tree are individual syntax nodes within actual code. These nodes are the result of parsing the code and so have additional data such as it's location within the File (two 2d coordinates), and it's name (type, e.x. a function call).

To see the actual complete structure look at [the tree_models definition](wsyntree/tree_models.py).

## Installing

Requirements:

 - C++ compiler (ubuntu: `libc++-dev libc++abi-dev`)
 - Python 3.8+

Install steps:

```
python -m pip install -r requirements.txt
python setup.py install
```
