import functools
import gc
import io
import logging
import os
import tempfile
import time

from typing import (
    IO,
    List,
    Optional,
)

import botocore  # type: ignore
import boto3  # type: ignore

_LOGGER = logging.getLogger(__name__)

DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for S3 multipart uploads"""
MIN_MIN_PART_SIZE = 5 * 1024 ** 2
"""The absolute minimum permitted by Amazon."""

_UPLOAD_ATTEMPTS = 10
_SLEEP_SECONDS = 10


class LightweightWriter(io.BufferedIOBase):
    """Writes bytes to S3 using the multi part API.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
        self,
        bucket: str,
        key: str,
        min_part_size: int = DEFAULT_MIN_PART_SIZE,
    ):
        assert min_part_size >= MIN_MIN_PART_SIZE

        self._bucket: str = bucket
        self._key: str = key
        self._min_part_size: int = min_part_size

        unused_parent, filename = os.path.split(self._key)
        name, extension = os.path.splitext(filename)
        self._buf: IO[bytes] = tempfile.NamedTemporaryFile(prefix='datawelder-%s-' % name)
        self._mpid: Optional[str] = None
        self._etags: List[str] = []
        self._closed: bool = False
        self._total_bytes: int = 0

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None  # type: ignore

    def flush(self):
        pass

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        if self.closed:
            return

        _LOGGER.debug("closing")
        if self._buf.tell():
            self._upload_next_part()

        assert self._total_bytes > 0
        assert self._mpid is not None

        s3 = boto3.resource('s3')
        multipart_upload = s3.MultipartUpload(self._bucket, self._key, self._mpid)
        partinfo = {
            'Parts': [
                {'PartNumber': partnum, 'ETag': etag}
                for partnum, etag in enumerate(self._etags, 1)
            ]
        }
        _LOGGER.debug('partinfo: %r', partinfo)

        #
        # TODO: check that this thing succeeded
        #
        multipart_upload.complete(MultipartUpload=partinfo)

        self._mpid = None
        self._etags = []

        del s3
        del multipart_upload
        del partinfo
        gc.collect()

    @property
    def closed(self):
        return self._closed

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self._total_bytes

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    def write(self, b):
        """Write the given buffer (bytes, bytearray, memoryview or any buffer
        interface implementation) to the S3 file.

        For more information about buffers, see https://docs.python.org/3/c-api/buffer.html

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""

        length = self._buf.write(b)
        self._total_bytes += length

        if self._buf.tell() >= self._min_part_size:
            self._upload_next_part()

        return length

    def terminate(self):
        """Cancel the underlying multipart upload."""
        raise NotImplementedError

    #
    # Internal methods.
    #
    def _upload_next_part(self):
        s3 = boto3.resource('s3')
        if self._mpid:
            multipart_upload = s3.MultipartUpload(self._bucket, self._key, self._mpid)
            _LOGGER.debug('resuming multipart upload %r', multipart_upload.id)
        else:
            object_ = s3.Object(self._bucket, self._key)
            multipart_upload = object_.initiate_multipart_upload()
            _LOGGER.debug('initiated multipart upload %r', multipart_upload.id)
            self._mpid = multipart_upload.id
            del object_

        part_num = len(self._etags) + 1
        _LOGGER.debug(
            "uploading bucket %r key %r part #%i, %i bytes (total %.3fGB)",
            self._bucket,
            self._key,
            part_num,
            self._buf.tell(),
            self._total_bytes / 1024.0 ** 3,
        )
        self._buf.seek(0)
        part = multipart_upload.Part(part_num)

        #
        # Network problems in the middle of an upload are particularly
        # troublesome.  We don't want to abort the entire upload just because
        # of a temporary connection problem, so this part needs to be
        # especially robust.
        #
        message = 'upload bucket %r key %r part %r' % (self._bucket, self._key, part_num)
        uploaded_part = _retry_if_failed(
            functools.partial(part.upload, Body=self._buf),
            message=message,
        )
        _LOGGER.debug('%s finished, ETag: %r', message, uploaded_part['ETag'])

        self._etags.append(uploaded_part['ETag'])
        self._buf.seek(0)
        self._buf.truncate(0)

        del s3
        del multipart_upload
        del part
        del uploaded_part
        gc.collect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()


def _retry_if_failed(
    partial,
    attempts=_UPLOAD_ATTEMPTS,
    sleep_seconds=_SLEEP_SECONDS,
    exceptions=None,
    message=None,
):
    if exceptions is None:
        exceptions = (botocore.exceptions.EndpointConnectionError, )
    for attempt in range(attempts):
        try:
            return partial()
        except exceptions:
            _LOGGER.critical(
                'Unable to connect to the endpoint (%s). '
                'Sleeping and retrying %d more times '
                'before giving up.' % (message, attempts - attempt - 1)
            )
            time.sleep(sleep_seconds)
    else:
        _LOGGER.critical('Unable to connect to the endpoint (%s). Giving up.' % message)
        raise IOError('Unable to connect to the endpoint after %d attempts' % attempts)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('sourcepath')
    parser.add_argument('bucket')
    parser.add_argument('key')
    parser.add_argument('--minpartsize', type=int, default=DEFAULT_MIN_PART_SIZE)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    with open(args.sourcepath, 'rb') as fin:
        with LightweightWriter(args.bucket, args.key, min_part_size=args.minpartsize) as writer:
            while True:
                buf = fin.read(io.DEFAULT_BUFFER_SIZE)
                if not buf:
                    break
                writer.write(buf)


if __name__ == '__main__':
    main()
