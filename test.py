
from pathlib import Path

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage

log.setLevel(log.DEBUG)

python = TreeSitterAutoBuiltLanguage('python')
javascript = TreeSitterAutoBuiltLanguage('javascript')

print(javascript.parse_file(Path('../CodeRibbon/lib/cr-base.js')))
print(python.parse_file(Path('./wsyntree/log.py')))
