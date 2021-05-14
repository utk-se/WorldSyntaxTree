
import time
import os
import argparse
import threading
import random
import concurrent.futures as futures

from pebble import concurrent, ProcessPool, ThreadPool
from multiprocessing import Pipe
from multiprocessing.managers import BaseManager, SyncManager, State
import enlighten
from enlighten import _manager, Counter

from wsyntree import log


class EnlightenMultiprocessManager(SyncManager):
    pass

main_proc_en_manager = enlighten.get_manager()

def create_counter(**kwargs):
    return main_proc_en_manager.counter(**kwargs)
EnlightenMultiprocessManager.register(
    'CreateCounter',
    create_counter
)
class CounterGenerator():
    def __init__(self, conn_queue):
        self.conn_queue = conn_queue
    def __call__(self, **kwargs):
        requester, giver = Pipe()
        self.conn_queue.put(giver)
        requester.send(kwargs)
        new_c = requester.recv()
        requester.close()
        log.debug(f"a CounterGenerator instance received: {repr(new_c)}")
        return new_c

@concurrent.thread
def counter_giver(counter_creation_func, conn_queue):
    """
    counter_creation_func: EnlightenMultiprocessManager.CreateCounter(**kwargs)

    this function sends Counter Proxies over connections added to the queue
    """
    log.debug(f"counter_giver up and running!")
    while (conn := conn_queue.get()) is not None:
        log.debug(f"counter_giver making a new counter!")
        new_c = counter_creation_func(**conn.recv())
        log.debug(f"counter_giver giving: {repr(new_c)}")
        conn.send(new_c)
        conn.close()

# @concurrent.process
def slow_worker(name, orig_jobitems, itemtime, counter_proxy_generator_proxy):
    """I WANT:
    A function I can call to retrieve a proxy for a counter
    """
    jobitems = random.randrange(orig_jobitems//2, orig_jobitems*2)
    try:
        cntr = counter_proxy_generator_proxy(
            desc=f"job {name}", total=jobitems, leave=False
        )
    except Exception as e:
        log.err(f"{type(e)}: {e}")
        raise

    log.info(f"job {name} started")

    for _ in range(jobitems):
        time.sleep(random.uniform(itemtime*0.1, itemtime*1.5))
        cntr.update()

    cntr.close()

    log.info(f"job {name} completed")
    return jobitems

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--nworkers",
        type=int,
        help="number of workers",
        default=os.cpu_count()
    )
    parser.add_argument(
        "--jobitems",
        type=int,
        help="items in a single job",
        default=200
    )
    parser.add_argument(
        "--njobs",
        type=int,
        help="number of total jobs to complete",
        default=200
    )
    parser.add_argument(
        "--itemtime",
        type=float,
        help="time taken per item in a job",
        default=0.1
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    with ProcessPool(max_workers=args.nworkers) as executor:

        # Setting up the server to run in a thread in this process
        mp_manager = EnlightenMultiprocessManager()
        mp_manager_server = mp_manager.get_server()
        mp_manager_server_thread = threading.Thread(target=mp_manager_server.serve_forever)
        mp_manager_server_thread.daemon = True
        mp_manager_server_thread.start()
        # slightly a hack:
        mp_manager._address = mp_manager_server.address
        mp_manager._state.value = State.STARTED

        counter_generator_queue = mp_manager.Queue()
        counter_generator = CounterGenerator(counter_generator_queue)
        counter_giver(mp_manager.CreateCounter, counter_generator_queue)

        ret_futures = []
        log.debug(f"counter_generator: {repr(counter_generator)}")
        log.info(f"Starting jobs...")
        for i in range(args.njobs):
            ret_futures.append(executor.schedule(
                slow_worker,
                (i, args.jobitems, args.itemtime, counter_generator)
            ))
        log.info(f"Waiting for jobs to complete...")
        cntr_all_jobs = main_proc_en_manager.counter(
            desc="all jobs", total=args.njobs, color='blue'
        )
        log.debug(f"cntr_all_jobs: {repr(cntr_all_jobs)}")
        for f in futures.as_completed(ret_futures):
            f.result()
            cntr_all_jobs.update()
        log.info(f"All jobs completed!")

if __name__ == '__main__':
    __main__()
