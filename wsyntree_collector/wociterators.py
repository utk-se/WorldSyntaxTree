import sys
import os
from pathlib import Path

import oscar

blob_sections = 128
all_blob_sections = range(0, blob_sections)

blob_fbase = "/da5_data/All.blobs/blob_{section}.{ext}"

def iter_blobs(section):
    p_idx = Path(blob_fbase.format(section=section, ext="idx"))
    p_bin = Path(blob_fbase.format(section=section, ext="bin"))

    with p_idx.open("rt") as idx_f, p_bin.open("rb") as bin_f:
        for idx_line in idx_f:
            fields = idx_line.rstrip().split(";")
            _hash = fields[3]
            if len(fields) > 4:
                _hash = fields[4]
            offset = int(fields[1])
            length = int(fields[2])
            bin_f.seek(offset, os.SEEK_SET)
            val = oscar.decomp(bin_f.read(length))
            yield (_hash, val)

def all_blobs():
    for sec in all_blob_sections:
        for i in iter_blobs(sec):
            yield i

if __name__ == "__main__":
    it = all_blobs()

    for _ in range(5):
        n = next(it)
        print(f"Blob {n[0]}: content length {len(n[1])}")
