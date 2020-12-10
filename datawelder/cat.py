"""Concatenate data."""


import io

import datawelder.readwrite

from typing import (
    List,
)


def cat(sources: List[str], destination: str) -> None:
    #
    # If destination is S3, may be able to do a multipart upload instead of
    # streaming
    #
    with datawelder.readwrite.open(destination, 'wb') as fout:
        for src in sources:
            with datawelder.readwrite.open(src, 'rb') as fin:
                while True:
                    buf = fin.read(io.DEFAULT_BUFFER_SIZE)
                    if not buf:
                        break
                    fout.write(buf)
