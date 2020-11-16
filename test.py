
from pathlib import Path

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator

log.setLevel(log.DEBUG)

python = TreeSitterAutoBuiltLanguage('python')
javascript = TreeSitterAutoBuiltLanguage('javascript')

ji = javascript.parse_file(Path('./tests/stuff.js'))

print(ji)

cur = ji.walk()
print(cur)

cur = TreeSitterCursorIterator(cur)
print(cur)

for node in cur:
    print(node)
