
from neomodel import (
    config, StructuredNode,
    StringProperty, IntegerProperty, UniqueIdProperty, DateTimeProperty,
    BooleanProperty,
    RelationshipTo, RelationshipFrom,
)
from neomodel.cardinality import ZeroOrOne, ZeroOrMore, One, OneOrMore

__all__ = [
    'SCM_Host', 'WSTRepository', 'WSTFile', 'WSTNode',
    'WSTIndexableText', 'WSTHugeText', 'WSTText'
]


class SCM_Host(StructuredNode):
    """e.g. GitHub"""
    name = StringProperty()
    host = StringProperty(unique_index=True, required=True)

    repos = RelationshipFrom("WSTRepository", 'HOSTED_ON')

class WSTRepository(StructuredNode):
    type = StringProperty() # e.g. git
    url = StringProperty(unique_index=True, required=True)
    path = StringProperty(required=True)
    analyzed_commit = StringProperty()
    analyzed_time = DateTimeProperty()

    host = RelationshipTo(SCM_Host, 'HOSTED_ON', cardinality=One)

    files = RelationshipFrom("WSTFile", 'IN_REPO')

class WSTFile(StructuredNode):
    path = StringProperty(required=True)
    error = StringProperty() # storage of parse failures, etc.
    language = StringProperty()

    repo = RelationshipTo(WSTRepository, 'IN_REPO', cardinality=One)

    wstnodes = RelationshipFrom("WSTNode", 'IN_FILE')

class WSTText(StructuredNode):
    length = IntegerProperty(index=True, required=True)

    used_by = RelationshipFrom("WSTNode", 'CONTENT')

class WSTIndexableText(WSTText):
    text = StringProperty(index=True, required=True)

class WSTHugeText(WSTText):
    """Node for storing text content too large to be indexed."""
    text = StringProperty(index=False, required=True)

class WSTNode(StructuredNode):
    x1 = IntegerProperty(required=True)
    y1 = IntegerProperty(required=True)
    x2 = IntegerProperty(required=True)
    y2 = IntegerProperty(required=True)

    named = BooleanProperty(required=True)
    type = StringProperty(index=True)

    file =   RelationshipTo(WSTFile, 'IN_FILE', cardinality=One)
    parent = RelationshipTo("WSTNode", 'PARENT', cardinality=One)
    text =   RelationshipTo(WSTText, 'CONTENT', cardinality=One)

    children = RelationshipFrom("WSTNode", 'PARENT')
