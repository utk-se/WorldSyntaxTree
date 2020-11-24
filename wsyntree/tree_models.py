
from mongoengine import Document
from mongoengine.fields import *


class Repository(Document):
    # url/hash combo is unique
    clone_url = URLField(required=True)
    analyzed_commit = StringField(required=True, unique_with='clone_url')
    added_time = DateTimeField(required=True)


class File(Document):
    repo = LazyReferenceField(Repository, required=True, passthrough=True)
    path = StringField(required=True, unique_with='repo')
    first_node = LazyReferenceField('Node', passthrough=True)

    def __repr__(self):
        return f"{self.repo.clone_url}@{self.repo.analyzed_commit}//{self.path}"
    __str__ = __repr__


class Node(Document):
    file = LazyReferenceField(File, required=True, passthrough=True)
    name = StringField(required=True) # TODO IntField encoding for space?
    parent = LazyReferenceField('Node', passthrough=True)
    children = ListField(LazyReferenceField('Node', passthrough=True))
    text = LazyReferenceField('NodeText', passthrough=True)

    x1 = IntField(required=True)
    x2 = IntField(required=True)
    y1 = IntField(required=True)
    y2 = IntField(required=True)

    meta = {
        'indexes': [
            'x1', 'x2', 'y1', 'y2',
        ]
    }


class NodeText(Document):
    text = StringField(required=True, unique=True)
