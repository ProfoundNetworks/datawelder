"""Concatenate data."""


import io

import smart_open  # type: ignore

from typing import (
    List,
)


def cat(sources: List[str], destination: str) -> None:
    #
    # If destination is S3, may be able to do a multipart upload instead of
    # streaming
    #
    with smart_open.open(destination, 'wb') as fout:
        for src in sources:
            with smart_open.open(src, 'rb') as fin:
                while True:
                    buf = fin.read(io.DEFAULT_BUFFER_SIZE)
                    if not buf:
                        break
                    fout.write(buf)
