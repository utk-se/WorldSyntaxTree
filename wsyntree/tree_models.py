
from neomodel import (
    config, StructuredNode,
    StringProperty, IntegerProperty, UniqueIdProperty, DateTimeProperty,
    BooleanProperty,
    RelationshipTo, RelationshipFrom,
)
from neomodel.cardinality import ZeroOrOne, ZeroOrMore, One, OneOrMore


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

    files = RelationshipFrom("File", 'IN_REPO')

class File(StructuredNode):
    path = StringProperty(required=True)
    error = StringProperty() # storage of parse failures, etc.
    language = StringProperty()

    repo = RelationshipTo(WSTRepository, 'IN_REPO', cardinality=One)

    wstnodes = RelationshipFrom("WSTNode", 'IN_FILE')

class WSTText(StructuredNode):
    text = StringProperty(unique_index=True, required=True)

    used_by = RelationshipFrom("WSTNode", 'CONTENT')

class WSTNode(StructuredNode):
    x1 = IntegerProperty(index=True, required=True)
    y1 = IntegerProperty(index=True, required=True)
    x2 = IntegerProperty(index=True, required=True)
    y2 = IntegerProperty(index=True, required=True)

    named = BooleanProperty(index=True, required=True)
    type = StringProperty(index=True)

    file =   RelationshipTo(File, 'IN_FILE')
    parent = RelationshipTo("WSTNode", 'PARENT')
    text =   RelationshipTo(WSTText, 'CONTENT')

    children = RelationshipFrom("WSTNode", 'PARENT')
