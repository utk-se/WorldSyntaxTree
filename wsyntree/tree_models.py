
from py2neo import ogm
from py2neo.ogm import Model, Property, Label, RelatedFrom, RelatedTo


class SCM_Host(Model):
    """e.g. GitHub"""
    __primarykey__ = 'host'
    name = Property("name")
    host = Property("host")

    repos = RelatedFrom("WSTRepository")

class WSTRepository(Model):
    __primarykey__ = 'url'
    type = Property("type") # e.g. git
    url = Property("url")
    path = Property("path")
    analyzed_commit = Property("hash")
    analyzed_time = Property("analysis_timestamp")

    host = RelatedTo(SCM_Host)

    files = RelatedFrom("File")

class File(Model):
    path = Property("path")
    error = Property("error") # storage of parse failures, etc.

    repo = RelatedTo(WSTRepository)

    nodes = RelatedFrom("WSTNode")

class WSTText(Model):
    text = Property("text")

    used_by = RelatedFrom("WSTNode")

    @classmethod
    def get_or_create(cls, text):
        # TODO
        raise NotImplementedError()

class WSTNode(Model):
    start_row = Property("x1")
    start_col = Property("y1")
    end_row = Property("x2")
    end_col = Property("y2")

    named = Label()
    type = Property("type")

    file = RelatedTo(File)
    parent = RelatedTo("WSTNode")
    text = RelatedTo(WSTText)

    children = RelatedFrom("WSTNode")
