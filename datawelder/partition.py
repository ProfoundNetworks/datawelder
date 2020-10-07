"""Implements functionality for partitioning data frames.

Partitioning a data frame::

    >>> partition('/data/foo.csv.gz', '/tmp/partitions/foo')

You can then work with the partitions of each frame, e.g. access records
from the 27th partition::

    >>> foo = PartitionedFrame('/tmp/partitions/foo')
    >>> num_partitions = len(foo)
    >>> for record in foo[27]:
    ...     pass

Each record will be a tuple.  You can convert it to a more helpful format::

    >>> record_tuple = next(foo[27])
    >>> record_dict = dict(zip(foo.field_names, record_tuple))

"""

import contextlib
import logging
import os
import os.path as P
import pickle
import resource
import zlib

from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Union,
)

import smart_open  # type: ignore
import smart_open.s3  # type: ignore
import yaml

import datawelder.io

_LOGGER = logging.getLogger(__name__)


@contextlib.contextmanager
def _update_soft_limit(soft_limit: int, limit_type: int = resource.RLIMIT_NOFILE) -> Iterator[None]:
    """Temporarily update a system limit.

    If the new soft limit is less than the current one, this function has no
    effect.

    :param soft_limit: The new soft limit.
    :param limit_type: One of the limit types, e.g. ``resource.RLIMIT_NOFILE``.

    """
    old_soft_limit, hard_limit = resource.getrlimit(limit_type)

    soft_limit = max(old_soft_limit, min(soft_limit, hard_limit))
    resource.setrlimit(limit_type, (soft_limit, hard_limit))

    yield

    resource.setrlimit(limit_type, (old_soft_limit, hard_limit))


@contextlib.contextmanager
def open_partitions(
    path_format: str,
    num_partitions: int,
    mode: str = "rt",
) -> Iterator[List]:
    """Open partitions based on the provided string pattern.

    Injects the `path_to_file` attribute to the resulting file objects.

    :param path_format: The format to use when determining partition paths.
    :param num_partitions: The number of partitions to open.
    :param mode: The mode in which the partitions must be opened.
    """
    _LOGGER.info("opening partitions: %r %r", path_format, mode)

    #
    # When writing to S3, smart_open buffers parts in memory until the minimum
    # part size is reached, and only then performs the upload.  If we're
    # writing to a large number of partitions simultaneously, the memory usage
    # can become very high.  Reduce the limit to the minimum allowable to
    # keep memory usage down.
    #
    tparams = {}
    if path_format.startswith('s3://'):
        tparams['min_part_size'] = smart_open.s3.MIN_MIN_PART_SIZE
    partition_paths = [path_format % i for i in range(num_partitions)]

    #
    # Temporarily bump the max number of open files to twice the number of
    # partitions, to be safe.  This appears to only be necessary on MacOS.
    #
    with _update_soft_limit(num_partitions * 2):
        gzip_streams = [
            smart_open.open(path, mode=mode, transport_params=tparams)
            for path in partition_paths
        ]
        for path, stream in zip(partition_paths, gzip_streams):
            stream.path_to_file = path  # type: ignore

        yield gzip_streams

        #
        # We want to make sure the files are _really_ closed to avoid running
        # into "Too many open files" error later.
        #
        for fin in gzip_streams:
            fin.close()


def calculate_key(key: str, num_shards: int) -> int:
    """Map an arbitrary string to a shard number.

    :param key: The key to use for hashing.
    :param num_shards: The number of shards to use.
    :returns: The number of the partition.
    """
    #
    # zlib.crc32 is approx. twice as fast as hashlib.sha1, .md5 and friends.
    # adler32 is documented as faster than crc32, but I haven't seen any
    # difference between the two in my benchmarks.
    #
    return zlib.adler32(key.encode(datawelder.io.ENCODING)) % num_shards


class PartitionedFrame:
    def __init__(self, path: str) -> None:
        self.path = path

        with smart_open.open(P.join(path, 'datawelder.yaml')) as fin:
            self.config = yaml.safe_load(fin)

        assert self.config['config_format'] == 1

        self.selected_fields = self.field_names

    def __len__(self):
        return self.config['num_partitions']

    def __getitem__(self, key: Any) -> Any:
        if not isinstance(key, int):
            raise ValueError('key must be an integer indicating the number of the partition')

        partition_number = int(key)
        if partition_number > len(self):
            raise ValueError('key must be less than %d' % self.config['num_partitions'])

        names = list(self.selected_fields)
        if self.key_name not in names:
            names.insert(0, self.key_name)
        indices = [self.field_names.index(f) for f in names]
        keyindex = names.index(self.key_name)

        partition_path = P.join(self.path, self.config['partition_format'] % partition_number)
        return Partition(partition_path, indices, names, keyindex)

    def select(self, field_names: List[str]) -> None:
        for f in field_names:
            if f not in self.field_names:
                raise ValueError('expected %r to be one of %r' % (f, self.field_names))

        self.selected_fields = field_names

    @property
    def field_names(self):
        return self.config['field_names']

    @property
    def key_index(self):
        return self.config['key_index']

    @property
    def key_name(self):
        return self.field_names[self.key_index]


class Partition:
    def __init__(
        self,
        path: str,
        field_indices: List[int],
        field_names: List[str],
        key_index: int,
    ) -> None:
        """Initialize a new partition.

        The partition stores records in a pickle file, where a record is
        an unnamed tuple.

        :param path: The path to the pickle file.
        :param field_indices: The indices of the fields to extract from the tuple.
        :param field_names: The names of the extracted fields.
        :param key_index: The index of the key.
        """
        assert len(field_names) == len(field_indices)

        self.path = path
        self.field_indices = field_indices
        self.field_names = field_names
        self.key_index = key_index

        self._fin = None

    def __iter__(self):
        return self

    def __next__(self):
        #
        # FIXME: maybe optimize this later
        #
        if self._fin is None:
            self._fin = smart_open.open(self.path, 'rb')

        try:
            record = pickle.load(self._fin)
        except EOFError:
            raise StopIteration
        else:
            return [record[i] for i in self.field_indices]


def partition(
    reader: 'datawelder.io.AbstractReader',
    destination_path: str,
    num_partitions: int,
    field_names: Optional[List[str]] = None,
    key_index: int = 0,
    key_function: Callable[[str, int], int] = calculate_key,
) -> 'PartitionedFrame':
    """Partition a data frame."""

    if not destination_path.startswith('s3://'):
        os.makedirs(destination_path, exist_ok=True)

    partition_format = '%04d.pickle.gz'
    abs_partition_format = P.join(destination_path, partition_format)

    with open_partitions(abs_partition_format, num_partitions, mode='wb') as partitions:
        for record in reader:
            key = record[key_index]
            partition_index = calculate_key(key, num_partitions)
            pickle.dump(record, partitions[partition_index])

    config = {
        'field_names': reader.field_names,
        'key_index': key_index,
        'source_path': reader.path,
        'num_partitions': num_partitions,
        'partition_format': partition_format,
        'config_format': 1,
    }

    with smart_open.open(P.join(destination_path, 'datawelder.yaml'), 'w') as fout:
        yaml.dump(config, fout)

    return PartitionedFrame(destination_path)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Split dataframes into partitions')
    parser.add_argument('source')
    parser.add_argument('destination')
    parser.add_argument('numpartitions', type=int)
    parser.add_argument(
        '--fieldnames',
        nargs='+',
        help=(
            'The names of fields to load. If not specified, will attemp to '
            'read from the first line of the source file.'
        ),
    )
    parser.add_argument(
        '--keyindex',
        type=int,
        help='The index of the partition key in the `fieldnames` list',
    )
    parser.add_argument(
        '--keyname',
        type=str,
        help='The name of the partition key',
    )
    parser.add_argument(
        '--format',
        type=str,
        default='auto',
        choices=('auto', datawelder.io.CSV, datawelder.io.JSON),
        help='The format of the source file',
    )
    parser.add_argument(
        '--fmtparams',
        type=str,
        nargs='*',
        help='Additional params to pass to the reader, in key=value format',
    )
    parser.add_argument('--loglevel', default=logging.INFO)
    args = parser.parse_args()

    if args.keyindex and args.keyname:
        parser.error('--keyindex and --keyname are mutually exclusive')

    logging.basicConfig(level=args.loglevel)

    source_format = datawelder.io.CSV
    if args.format in (None, 'auto'):
        source_format = datawelder.io.sniff_format(args.source)

    if source_format == datawelder.io.CSV:
        cls = datawelder.io.CsvReader
    elif source_format == datawelder.io.JSON:
        cls = datawelder.io.JsonReader
    else:
        assert False

    fmtparams = datawelder.io.parse_fmtparams(args.fmtparams)

    key: Union[int, str, None] = None
    if args.keyindex:
        key = args.keyindex
    elif args.keyname:
        key = args.keyname
    else:
        key = 0
    assert key is not None

    with cls(args.source, key, args.fieldnames, fmtparams) as reader:
        partition(reader, args.destination, args.numpartitions)


if __name__ == '__main__':
    main()
