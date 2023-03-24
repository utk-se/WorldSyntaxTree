
from typing import Callable

import tenacity
from tenacity.retry import retry_if_exception
from arango.exceptions import (
    DocumentInsertError as ArangoDocumentInsertError,
    AsyncJobResultError as ArangoAsyncJobResultError,
    BatchJobResultError as ArangoBatchJobResultError,
)
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

class RootTreeSitterNodeIsError(ValueError, WSTBaseError):
    pass

def isArangoWriteWriteConflict(e: ArangoDocumentInsertError) -> bool:
    """Is an exception a Write-Write conflict?"""
    if isinstance(e, ArangoDocumentInsertError):
        if e.error_code == arango.errno.CONFLICT:
            return True
    return False

def isArangoAsyncJobNotDone(e: ArangoAsyncJobResultError) -> bool:
    """Is an AsyncJobResultError because the job is not yet done?"""
    if isinstance(e, ArangoAsyncJobResultError):
        if e.http_code == 204:
            return True
    return False

def isArangoBatchJobNotDone(e: ArangoBatchJobResultError) -> bool:
    """Is an AsyncJobResultError because the job is not yet done?"""
    if isinstance(e, ArangoBatchJobResultError):
        if e.http_code == 204:
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

def auto_asyncjobdone_retry(f: Callable) -> Callable:
    return tenacity.retry(
        # a 30-minute timeout should be plenty in case the database
        # decides to do a checkpoint in the middle of a batch run
        stop=tenacity.stop.stop_after_delay(1800),
        wait=tenacity.wait.wait_random_exponential(
            multiplier=0.01,
            max=5, # check at least every N seconds
        ),
        retry=retry_if_exception(isArangoAsyncJobNotDone),
        reraise=True,
    )(f)

def auto_batchjobdone_retry(f: Callable) -> Callable:
    return tenacity.retry(
        # a 30-minute timeout should be plenty in case the database
        # decides to do a checkpoint in the middle of a batch run
        stop=tenacity.stop.stop_after_delay(1800),
        wait=tenacity.wait.wait_random_exponential(
            multiplier=0.01,
            max=5, # check at least every N seconds
        ),
        retry=retry_if_exception(isArangoBatchJobNotDone),
        reraise=True,
    )(f)
