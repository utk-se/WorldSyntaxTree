
from mongoengine import Document
from mongoengine.fields import *


class Repository(Document):
    # url/hash combo is unique
    clone_url = URLField(required=True)
    analyzed_commit = StringField(required=True, unique_with='clone_url')
    added_time = DateTimeField(required=True)


class File(Document):
    repo = LazyReferenceField(Repository, required=True)
    path = StringField(required=True)
    first_node = LazyReferenceField('Node')


class Node(Document):
    file = LazyReferenceField(File, required=True)
    name = StringField(required=True) # TODO IntField encoding for space?
    parent = LazyReferenceField('Node')
    children = LazyReferenceField('Node')


class NodeText(Document):
    text = StringField(required=True, unique=True)
