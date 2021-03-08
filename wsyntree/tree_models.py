
__all__ = [
    'WSTRepository', 'WSTFile', 'WSTNode', 'WSTText'
]


class WST_Document():
    __slots__ = ['_key']

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def __dict__(self):
        return {s: getattr(self, s, None) for s in self.__slots__}

class WSTRepository(WST_Document):
    __slots__ = [
        "type",
        "url",
        "path",
        "commit",
        "analyzed_time",
    ]

    # host = RelationshipTo(SCM_Host, 'HOSTED_ON', cardinality=One)

    # files = RelationshipFrom("WSTFile", 'IN_REPO')

class WSTFile(WST_Document):
    __slots__ = [
        "path",
        "language",
        "error",
    ]

    # repo = RelationshipTo(WSTRepository, 'IN_REPO', cardinality=One)

    # wstnodes = RelationshipFrom("WSTNode", 'IN_FILE')

class WSTText(WST_Document):
    __slots__ = [
        "length",
        "text",
    ]

    # used_by = RelationshipFrom("WSTNode", 'CONTENT')

class WSTNode(WST_Document):
    __slots__ = [
        # x: line, y: character (2d coords begin->end)
        "x1",
        "y1",
        "x2",
        "y2",

        # preorder: depth-first traversal order, unique within file
        # aka: topologically sorted
        # root node is zero and has no parent
        "preorder",

        "named",
        "type",
    ]

    # file =   RelationshipTo(WSTFile, 'IN_FILE', cardinality=One)
    # parent = RelationshipTo("WSTNode", 'PARENT', cardinality=One)
    # text =   RelationshipTo(WSTText, 'CONTENT', cardinality=One)

    # children = RelationshipFrom("WSTNode", 'PARENT')
