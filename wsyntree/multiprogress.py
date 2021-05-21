
import multiprocessing
from multiprocessing import Pipe
from multiprocessing.managers import BaseManager, SyncManager, State
from multiprocessing.connection import Connection
import threading
from traceback import format_exc

from pebble import concurrent
import enlighten
import enlighten._manager
# enlighten._manager.RESIZE_SUPPORTED = False

from . import log

_main_proc_en_manager = None
_en_manager_proxy = None
_mp_manager = None
_mp_manager_server = None
_mp_manager_server_thread = None
_proxy_generator_thread = None
_req_queue = None


def main_proc_setup():
    global _main_proc_en_manager
    _main_proc_en_manager = enlighten.get_manager()

class EnlightenMultiprocessManager(SyncManager):
    pass
def _mpcem_call_director(func, args, kwargs):
    try:
        func = getattr(_main_proc_en_manager, func)
        if not callable(func):
            raise RuntimeError(f"requested function not a callable")
        return func(*args, **kwargs)
    except Exception as e:
        log.error(f"{type(e)}: {e}")
        raise e

EnlightenMultiprocessManager.register(
    'CreateEnlightenProxy',
    _mpcem_call_director
)

@concurrent.thread
def enlighten_proxy_generator(mp_manager_instance, request_queue):
    """
    request_queue contains Connections: args are read then proxy is sent

    send `None` into the queue instead of `Connection` to exit this thread
    """
    while (conn := request_queue.get()) is not None:
        try:
            request = conn.recv()
            # use same req format as Python's native multiprocessing:
            ignore, funcname, args, kwds = request
            new_proxy = mp_manager_instance.CreateEnlightenProxy(funcname, args, kwds)
            msg = ('#RESULT', new_proxy)
        except Exception:
            formatted_trace = format_exc()
            msg = ('#TRACEBACK', formatted_trace)
            log.warn(f"failed to generate a proxy:")
            log.trace(log.warn, formatted_trace)
        try:
            # log.debug(f"sending {msg}")
            conn.send(msg)
        except Exception as e:
            log.error(f"{type(e)}: {e}")
            raise e

class EnlightenManagerProxy():
    def __init__(self, conn_queue):
        """
        conn_queue: Queue to send requests over via Pipes
        """
        self._conn_q = conn_queue

    def _add_request_pipe(self) -> Connection:
        requester, giver = Pipe()
        self._conn_q.put(giver)
        return requester

    def counter(self, **kwargs):
        if 'replace' in kwargs:
            raise NotImplementedError(f"Replacement of Counters not yet supported.")
        requester, giver = Pipe()
        self._conn_q.put(giver)
        request = (None, 'counter', [], kwargs)
        requester.send(request)
        # log.debug(f"awaiting a counter...")
        status, new_c = requester.recv()
        if status != '#RESULT':
            raise RuntimeError(f"got bad result: {status}: {new_c}")
        requester.close()
        # log.debug(f"EnlightenManagerProxy instance received: {repr(new_c)}")
        return new_c

def start_server_thread():
    # Setting up the server to run in a thread in this process
    global _mp_manager
    _mp_manager = EnlightenMultiprocessManager()
    _mp_manager_server = _mp_manager.get_server()
    _mp_manager_server_thread = threading.Thread(target=_mp_manager_server.serve_forever)
    _mp_manager_server_thread.daemon = True
    _mp_manager_server_thread.start()
    # slightly a hack:
    _mp_manager._address = _mp_manager_server.address
    _mp_manager._state.value = State.STARTED

    global _req_queue, _proxy_generator_thread
    _req_queue = _mp_manager.Queue()
    _proxy_generator_thread = enlighten_proxy_generator(_mp_manager, _req_queue)

    global _en_manager_proxy
    _en_manager_proxy = EnlightenManagerProxy(_req_queue)

def setup_if_needed() -> bool:
    """returns True if the setup was performed"""
    if _proxy_generator_thread is not None:
        return False
    if multiprocessing.parent_process() == None:
        main_proc_setup()
        start_server_thread()
        return True
    return False

def is_proxy(thing):
    return isinstance(thing, EnlightenManagerProxy)

def get_manager():
    if multiprocessing.parent_process() == None:
        return _main_proc_en_manager
    else:
        if _en_manager_proxy:
            return _en_manager_proxy
        raise RuntimeError(f"was the Enlighten Manager constructed already?")

def get_manager_proxy():
    if not _mp_manager:
        raise RuntimeError(f"you need to start_server_thread() first")
    if _en_manager_proxy:
        return _en_manager_proxy
    raise RuntimeError(f"did not set up correctly?")
