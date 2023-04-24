import sys
import os
import gzip
import enum
from pathlib import Path, PurePosixPath
from collections import namedtuple

from tqdm import tqdm

from wsyntree import log
import oscar

blob_sections = 128
all_blob_sections = range(0, blob_sections)

blob_fbase = "/da5_data/All.blobs/blob_{section}.{ext}"

blobresult = namedtuple("BlobResult", ["hash", "content", "filenames"])

KIBIBYTE = 2**10
MEBIBYTE = 2**20
GIBIBYTE = 2**30

class BlobStatus(str, enum.Enum): # enum.StrEnum added in 3.11
    done = "done"
    not_supported = "not_supported"
    too_large = "too_large"
    errored = "errored"
    skipped = "skipped"

def binary_line_iterator(open_file, max_buffer_size=GIBIBYTE):
    """only yields lines that are decodable"""
    # magic performance number: wanting the average buffer.split() size
    # idxf_1: 10849129529/97563749 = 111.2 avg bytes per line
    chunksize = 256
    lineno = 0
    buffer = bytearray()
    for block in iter(lambda: open_file.read(chunksize), b""):
        buffer += block
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            lineno += 1
            try:
                yield line.decode()
            except UnicodeDecodeError:
                log.warn(f"cannot decode line {lineno} of {open_file.name}")
        if len(buffer) >= max_buffer_size:
            # skip this line, it's too large
            log.warn(f"line {lineno} greater than {max_buffer_size} bytes long, skipping to next line...")
            buffer = bytearray()
            for block in iter(lambda: open_file.read(chunksize), b""):
                if b"\n" in block:
                    # found next line
                    _, new_line_start = block.split(b"\n", 1)
                    lineno += 1
                    buffer.extend(new_line_start)
                    break
            else: # no newline before end of file:
                log.warn(f"line {lineno} was last in file")
                # loop will exit naturally
    if buffer:
        for line in buffer.split(b"\n"):
            try:
                yield line.decode()
            except UnicodeDecodeError:
                log.warn(f"cannot decode line {lineno} of {open_file.name}")
            lineno += 1 # does this go before or after? not too important?

def iter_blobs(
        section,
        blobfilter=lambda b: True,
        filefilter=lambda fnames: True,
        max_blob_size=(MEBIBYTE * 32),
        tqdm_position=None,
        redis_cl=None,
    ):
    """
    blobfilter(str) -> bool
    filefilter(List[PurePosixPath]) -> bool

    all provided filters must pass
    """
    p_idx = Path(blob_fbase.format(section=section, ext="idxf"))
    p_bin = Path(blob_fbase.format(section=section, ext="bin"))

    # open the index in binary as well, encoding not specified
    with gzip.open(p_idx, "rb") as idx_f, p_bin.open("rb") as bin_f:
        if tqdm_position is None:
            bin_line_it = binary_line_iterator(idx_f)
        else:
            bin_line_it = tqdm(
                binary_line_iterator(idx_f),
                position=tqdm_position,
                desc="index lines",
                unit="lines",
                unit_scale=True,
                smoothing=0.01,
            )
        for idx_line in bin_line_it:
            fields = idx_line.rstrip().split(";")
            _hash = fields[3]
            if not blobfilter(_hash):
                continue
            offset = int(fields[1])
            length = int(fields[2])
            if length >= max_blob_size:
                log.warn(f"compressed blob too large: skip {_hash} of lzf size {length}")
                if redis_cl:
                    redis_cl.set(_hash, BlobStatus.too_large)
                continue
            filenames = tuple(PurePosixPath(x) for x in fields[4:])
            if not filefilter(filenames):
                if redis_cl:
                    redis_cl.set(_hash, BlobStatus.not_supported)
                continue
            bin_f.seek(offset, os.SEEK_SET)
            lzf_data = bin_f.read(length)
            if not lzf_data:
                log.warn(f"no data in blob: skip {_hash}")
                continue
            if lzf_data[0] == 0:
                # data not compressed, will be handled by oscar.decomp
                pass
            else:
                header_size, uncompressed_content_length = oscar.lzf_length(lzf_data)
                if uncompressed_content_length >= max_blob_size:
                    log.warn(f"uncompressed blob too large: skip {_hash} of size {uncompressed_content_length}")
                    if redis_cl:
                        redis_cl.set(_hash, BlobStatus.too_large)
                    continue
            val = oscar.decomp(lzf_data)
            yield blobresult(_hash, val, filenames)

def all_blobs(**kwargs):
    for sec in all_blob_sections:
        log.info(f"iter all blob sections: start section {sec}")
        for i in iter_blobs(sec, **kwargs):
            yield i

if __name__ == "__main__":
    it = all_blobs()

    for _ in range(5):
        n = next(it)
        print(f"Blob {n[0]}: content length {len(n[1])}")
