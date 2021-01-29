
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
        "file_ext": "\.rs$",
    },
    # "ruby": {
    #     "tsrepo": "https://github.com/tree-sitter/tree-sitter-ruby.git",
    #     "file_ext": "\.rb$",
    # },
    # "csharp": {
    #     "tsrepo": "https://github.com/tree-sitter/tree-sitter-c-sharp.git",
    #     "file_ext": "\.cs$",
    # },
    "c": {
        "tsrepo": "https://github.com/tree-sitter/tree-sitter-c.git",
        "file_ext": "\.(c|h)$",
    },
    "cpp": {
        "tsrepo": "https://github.com/tree-sitter/tree-sitter-cpp.git",
        "file_ext": "\.(cpp|hpp|c\+\+|h\+\+|cc|hh|cxx|hxx)$"
    },
    "go": {
        "tsrepo": "https://github.com/tree-sitter/tree-sitter-go.git",
        "file_ext": "\.go$",
    }
}

wsyntree_file_to_lang = {}

for k,v in wsyntree_langs.items():
    wsyntree_file_to_lang[v['file_ext']] = k
