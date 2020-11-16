# WorldSyntaxTree

Language-agnostic parsing of World of Code repositories

## Terminology / Project Structure

### Collector

In the context of this project, the 'collector' refers to:

* Collection of source repositories and metadata from the 'World of Code' for parsing

* An external tool such as [tree sitter](http://tree-sitter.github.io/tree-sitter/) is used to parse raw (potentially invalid) file input into a syntax tree. Specifically, we retrieve the type of token as well as the location in the file. We may discard or ignore information based on the file type or content at this stage.

* The output of the parsing tool is then stored.translated to a queryable, tabular format. We are interested in representing the output as a Dask dataframe. See [dask](https://dask.org/)

### API

Queryable via dask distributed dataframe or via web (fancy) frontend. (?) Subject to change.

## Installing

Requirements:

 - C++ compiler (ubuntu: `libc++-dev libc++abi-dev`)
 - Python 3
 - Dask, pygit2, etc (`pip install -r requirements.txt` or install wsyntree)
