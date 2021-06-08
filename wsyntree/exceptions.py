
class WSTBaseError(Exception):
    pass

class RepoExistsError(FileExistsError, WSTBaseError):
    pass

class EdgeDefinitionDuplicateError(ValueError, WSTBaseError):
    pass

class LocalCopyOutOfSync(ValueError, WSTBaseError):
    pass

class PrerequisiteStateInvalid(ValueError, WSTBaseError):
    pass

class UnhandledGitFileMode(ValueError, WSTBaseError):
    pass

class DeduplicatedObjectMismatch(ValueError, WSTBaseError):
    pass
