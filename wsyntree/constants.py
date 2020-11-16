
"""
"language": {
    "tsrepo": "repo_clone_url",
    "file_ext": ".lang$"
}
"""

wsyntree_langs = {
    "javascript": {
        "tsrepo": "https://github.com/tree-sitter/tree-sitter-javascript.git",
        "file_ext": "(?<!(\.min))\.js$", # no .min.js
    },
    "python": {
        "tsrepo": "https://github.com/tree-sitter/tree-sitter-python.git",
        "file_ext": "\.py$",
    },
    "rust": {
        "tsrepo": "https://github.com/tree-sitter/tree-sitter-rust.git",
        "file_ext": "\.rs$"
    }
}