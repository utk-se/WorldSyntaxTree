
from mongoengine import Document
from mongoengine.fields import *


class Repository(Document):
    # url/hash combo is unique
    clone_url = URLField(required=True)
    analyzed_commit = StringField(required=True, unique_with='clone_url')
    added_time = DateTimeField(required=True)

# dedup type
class NodeText(Document):
    text = StringField(required=True)

    @classmethod
    def get_or_create(cls, text):
        try:
            return cls.objects.get(text=text)
        except cls.DoesNotExist:
            n = cls(text=text)
            n.save()
            return n

    def __str__(self):
        return self.text

    meta = {
        'indexes': [
            '#text',
        ],
        'index_background': True,
    }

# dedup type
class LineInFile(Document):
    text = StringField(required=True, unique=True)

    @classmethod
    def get_or_create(cls, text):
        try:
            return cls.get(text=text)
        except cls.DoesNotExist:
            n = cls(text=text)
            n.save()
            return n

class File(Document):
    repo = LazyReferenceField(Repository, required=True, passthrough=True)
    path = StringField(required=True, unique_with='repo')
    first_node = LazyReferenceField('Node', passthrough=True)

    def __repr__(self):
        return f"File<{self.repo.clone_url}@{self.repo.analyzed_commit}//{self.path}, {self.id}>"
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
            'name',
        ],
        'index_background': True,
    }

    def __repr__(self):
        return f"Node(id={self.id}, name={self.name}, ({self.x1}, {self.y1} -> {self.x2}, {self.y2}), {len(self.children)} children)"
    __str__ = __repr__
