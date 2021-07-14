
from typing import Callable

import tenacity
from tenacity.retry import retry_if_exception
from arango.exceptions import DocumentInsertError as ArangoDocumentInsertError
import arango.errno

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

def isArangoWriteWriteConflict(e: ArangoDocumentInsertError) -> bool:
    """Is an exception a Write-Write conflict?"""
    if isinstance(e, ArangoDocumentInsertError):
        if e.error_code == arango.errno.CONFLICT:
            return True
    return False

def auto_writewrite_retry(f: Callable) -> Callable:
    """Decorator to apply Arango Write-Write conflict retry preset"""
    return tenacity.retry(
        stop=tenacity.stop.stop_after_attempt(20),
        wait=tenacity.wait.wait_random_exponential(
            multiplier=0.1,
            max=6,
        ),
        retry=retry_if_exception(isArangoWriteWriteConflict),
        reraise=True,
    )(f)
