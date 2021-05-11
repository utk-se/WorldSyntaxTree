
import argparse
from multiprocessing import Pool
import sys
import os
from urllib.parse import urlparse
import time

import pygit2 as git
from arango import ArangoClient

from wsyntree import log
from wsyntree.wrap_tree_sitter import TreeSitterAutoBuiltLanguage, TreeSitterCursorIterator
from wsyntree.utils import strip_url, desensitize_url
from wsyntree.tree_models import WSTRepository

from .arango_collector import WST_ArangoTreeCollector
from .batch_analyzer import set_batch_analyze_args, RepoExistsError


def analyze(args):
    collector = WST_ArangoTreeCollector(
        args.repo_url,
        workers=args.workers,
        database_conn=args.db,
        commit_sha=args.target_commit,
    )
    collector.setup()
    log.debug(f"Set up collector: {collector}")

    # check if exists already
    if repo := WSTRepository.get(collector._db, collector._current_commit_hash):
        if args.skip_exists:
            log.warn(f"Skipping collection since repo document already present for commit {collector._current_commit_hash}")
            return
        else:
            raise RepoExistsError(f"Repo document already exists: {repo.__dict__}")

    try:
        collector.collect_all()
    except Exception as e:
        log.crit(f"{collector} run failed.")
        raise e

def delete(args):
    if '/' in args.which_repo:
        # it's a URL/URI
        collector = WST_ArangoTreeCollector(
            args.which_repo,
            database_conn=args.db,
        )
    else:
        # find by commit
        collector = WST_ArangoTreeCollector(
            None, # this collector only used to delete
            commit_sha=args.which_repo,
        )
    collector.delete_all_tree_data()

def database_init(args):
    client = ArangoClient(hosts=strip_url(args.db))
    p = urlparse(args.db)
    odb = client.db(p.path[1:], username=p.username, password=p.password)

    newdcls = ['wstfiles', 'wstrepos', 'wstnodes', 'wsttexts']
    newecls = ['wst-fromfile', 'wst-fromrepo', 'wst-nodeparent', 'wst-nodetext']
    _ngraphs = {
        "wst-repo-files": {
            "edge_collection": 'wst-fromrepo',
            "from_vertex_collections": ['wstfiles'],
            "to_vertex_collections": ['wstrepos'],
        },
        "wst-file-nodes": {
            "edge_collection": 'wst-fromfile',
            "from_vertex_collections": ['wstnodes'],
            "to_vertex_collections": ['wstfiles'],
        },
        "wst-node-parents": {
            "edge_collection": 'wst-nodeparent',
            "from_vertex_collections": ['wstnodes'],
            "to_vertex_collections": ['wstnodes'],
        },
        "wst-node-text": {
            "edge_collection": 'wst-nodetext',
            "from_vertex_collections": ['wstnodes'],
            "to_vertex_collections": ['wsttexts'],
        },
    }

    if args.delete:
        log.warn(f"deleting all data ...")
        # deleting old stuff could take awhile
        jobs = []
        db = odb.begin_async_execution()

        jobs.append(db.delete_graph('wst', ignore_missing=True))
        for c in newdcls:
            jobs.append(db.delete_collection(c, ignore_missing=True))
        for c in newecls:
            jobs.append(db.delete_collection(c, ignore_missing=True))

        jt_wait = len(jobs)
        while len(jobs) > 0:
            time.sleep(1)
            for j in jobs:
                if j.status() == 'done':
                    jobs.remove(j)
            if jt_wait != len(jobs):
                log.debug(f"delete: waiting on {len(jobs)} jobs to finish ...")
                jt_wait = len(jobs)

    # back to non-async
    db = odb

    log.info(f"Creating collections ...")

    colls = {}
    for cn in newdcls:
        if db.has_collection(cn):
            colls[cn] = db.collection(cn)
        else:
            colls[cn] = db.create_collection(cn, user_keys=True)
    for cn in newecls:
        if db.has_collection(cn):
            colls[cn] = db.collection(cn)
        else:
            colls[cn] = db.create_collection(cn, user_keys=True, edge=True)

    graph = None
    if not db.has_graph('wst'):
        graph = db.create_graph('wst')
    else:
        graph = db.graph('wst')
    edgedefs = {}

    for gk, gv in _ngraphs.items():
        if not graph.has_edge_definition(gv['edge_collection']):
            log.debug(f"Added graph edges {gv}")
            edgedefs[gk] = graph.create_edge_definition(**gv)

def __main__():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db", "--database",
        type=str,
        help="Database connection string",
        default=os.environ.get('WST_DB_URI', "http://wst:wst@localhost:8529/wst")
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    subcmds = parser.add_subparsers(
        title="Collector commands"
    )

    # analysis
    cmd_analyze = subcmds.add_parser(
        'analyze', aliases=['add', 'a'], help="Analyze repositories")
    cmd_analyze.set_defaults(func=analyze)
    cmd_analyze.add_argument(
        "repo_url",
        type=str,
        help="URI for cloning the repository"
    )
    cmd_analyze.add_argument(
        "-w", "--workers",
        type=int,
        help="Number of workers to use for processing files, default: os.cpu_count()",
        default=None
    )
    cmd_analyze.add_argument(
        "--skip-exists", "--skip-existing",
        action="store_true",
        help="Skip the analysis if the repo document already exists in the database"
    )
    cmd_analyze.add_argument(
        "-t", "--target-commit",
        type=str,
        help="Checkout and analyze a specific commit from the repo",
        default=None
    )
    # batch analysis
    cmd_batch = subcmds.add_parser(
        'batch', aliases=['addbatch', 'addmulti'],
        help="Analyze multiple repos from a JSON specification list"
    )
    set_batch_analyze_args(cmd_batch)
    # delete data selectively
    cmd_delete = subcmds.add_parser(
        'delete', aliases=['del'], help="Delete tree data selectively")
    cmd_delete.set_defaults(func=delete)
    cmd_delete.add_argument(
        "which_repo",
        type=str,
        help="URI or commit SHA for which repo's data to delete"
    )
    # db setup
    cmd_db = subcmds.add_parser(
        'db', aliases=['database'], help="Manage the database")
    subcmds_db = cmd_db.add_subparsers(title="Manage the database")
    cmd_db_init = subcmds_db.add_parser(
        'initialize', aliases=['init', 'setup'], help="Set up the database")
    cmd_db_init.set_defaults(func=database_init)
    cmd_db_init.add_argument(
        "-d", "--delete",
        help="Delete any existing data in the database",
        action="store_true",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    log.info(f"DB connection: {desensitize_url(args.db)}")

    if 'func' not in args:
        log.warn(f"Please supply a valid subcommand!")
        return

    args.func(args)

if __name__ == '__main__':
    __main__()
