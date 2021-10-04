#!/usr/bin/env python3

import os
import argparse
import subprocess
import shutil
import shlex
from pathlib import Path
from urllib.parse import urlparse

from wsyntree import log
from wsyntree.utils import strip_url, desensitize_url


cmdfmt_arangoimport = "arangoimport --progress true --file '{filepath}' --type jsonl --collection '{collname}' --server.database '{database}' --on-duplicate ignore --server.username '{username}' --server.password '{password}'"

def run_arangoimport(dir: Path, db_uri: str):
    collfiles = list(reversed(sorted(dir.glob("*.jsonl"))))
    log.debug(f"Collection files to import: {collfiles}")
    db = urlparse(db_uri)

    for collfile in collfiles:
        fname = collfile.name
        collname = collfile.stem.split('.')[0]
        # log.debug(f"Importing to {collname} from {fname}")
        run_cmd = cmdfmt_arangoimport.format(
            filepath=collfile,
            collname=collname,
            database=db.path.lstrip('/'),
            username=db.username,
            password=db.password,
        )
        # log.debug(f"Import command: `{run_cmd}`")
        with subprocess.Popen(shlex.split(run_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            for line in p.stdout:
                line = line.decode().strip()
                log.debug(line)
            for line in p.stderr:
                log.warn(line.decode().strip())
        retval = p.returncode
        if retval != 0:
            raise RuntimeError(f"Run of arangoimport failed.")
        log.info(f"Import to {collname} completed...")

if __name__ == "__main__":
    if not shutil.which("arangoimport"):
        log.error(f"Running this importer requires the `arangoimport` to be available in the path.")
        raise RuntimeError(f"Missing arangoimport")

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "output_dir",
        nargs="+",
        help="Output directories to import into ArangoDB",
        type=Path,
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )
    parser.add_argument(
        "--db", "--database",
        type=str,
        help="Database connection string",
        default=os.environ.get('WST_DB_URI', "http://wst:wst@localhost:8529/wst")
    )

    args = parser.parse_args()
    if args.verbose:
        log.setLevel(log.DEBUG)
        log.debug("Verbose logging enabled.")

    log.info(f"DB connection: {desensitize_url(args.db)}")

    for dir in args.output_dir:
        log.info(f"Processing {dir} ...")
        run_arangoimport(dir, args.db)
