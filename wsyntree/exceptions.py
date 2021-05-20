
class WSTBaseError(Exception):
    pass

class RepoExistsError(FileExistsError, WSTBaseError):
    pass

class EdgeDefinitionDuplicateError(ValueError, WSTBaseError):
    pass

class LocalCopyOutOfSync(ValueError, WSTBaseError):
    pass
