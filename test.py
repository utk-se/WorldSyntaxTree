
from pathlib import Path

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

log.setLevel(log.DEBUG)

python = TreeSitterAutoBuiltLanguage('python')
javascript = TreeSitterAutoBuiltLanguage('javascript')

pi = python.parse_file('./test.py')
ji = javascript.parse_file(Path('./tests/stuff.js'))

cur = ji.walk()

cur = TreeSitterCursorIterator(cur, nodefilter=lambda x: x.is_named)
print(cur)

for node in cur:
    print(node)
