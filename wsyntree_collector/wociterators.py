import sys
import os
import gzip
from pathlib import Path, PurePosixPath
from collections import namedtuple

import oscar

blob_sections = 128
all_blob_sections = range(0, blob_sections)

blob_fbase = "/da5_data/All.blobs/blob_{section}.{ext}"

blobresult = namedtuple("BlobResult", ["hash", "content", "filenames"])

def iter_blobs(section, blobfilter=lambda b: True, filefilter=lambda fnames: True):
    """
    blobfilter(str) -> bool
    filefilter(List[PurePosixPath]) -> bool

    all provided filters must pass
    """
    p_idx = Path(blob_fbase.format(section=section, ext="idxf"))
    p_bin = Path(blob_fbase.format(section=section, ext="bin"))

    with gzip.open(p_idx, "rt") as idx_f, p_bin.open("rb") as bin_f:
        for idx_line in idx_f:
            fields = idx_line.rstrip().split(";")
            _hash = fields[3]
            filenames = tuple(PurePosixPath(x) for x in fields[4:])
            offset = int(fields[1])
            length = int(fields[2])
            if not blobfilter(_hash):
                continue
            if not filefilter(filenames):
                continue
            bin_f.seek(offset, os.SEEK_SET)
            val = oscar.decomp(bin_f.read(length))
            yield blobresult(_hash, val, filenames)

def all_blobs(**kwargs):
    for sec in all_blob_sections:
        for i in iter_blobs(sec, **kwargs):
            yield i

if __name__ == "__main__":
    it = all_blobs()

    for _ in range(5):
        n = next(it)
        print(f"Blob {n[0]}: content length {len(n[1])}")
