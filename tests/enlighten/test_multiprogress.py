"""
just for testing the multiprogress Enlighten wrapper, run directly:

python tests/enlighten/idea.py -v
"""

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
from wsyntree import multiprogress


# @concurrent.process
def slow_worker(name, orig_jobitems, itemtime, en_manager):
    """I WANT:
    A function I can call to retrieve a proxy for a counter
    """
    jobitems = random.randrange(orig_jobitems//2, orig_jobitems*2)
    try:
        cntr = en_manager.counter(
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
        multiprogress.main_proc_setup()
        multiprogress.start_server_thread()

        en_manager_proxy = multiprogress.get_manager_proxy()
        en_manager = multiprogress.get_manager()

        ret_futures = []
        # log.debug(f"counter_generator: {repr(counter_generator)}")
        log.info(f"Starting jobs...")
        for i in range(args.njobs):
            ret_futures.append(executor.schedule(
                slow_worker,
                (i, args.jobitems, args.itemtime, en_manager_proxy)
            ))
        log.info(f"Waiting for jobs to complete...")
        cntr_all_jobs = en_manager.counter(
            desc="all jobs", total=args.njobs, color='blue'
        )
        log.debug(f"cntr_all_jobs: {repr(cntr_all_jobs)}")
        for f in futures.as_completed(ret_futures):
            f.result()
            log.debug(f"finished a job!")
            cntr_all_jobs.update()
        log.info(f"All jobs completed!")

if __name__ == '__main__':
    __main__()
