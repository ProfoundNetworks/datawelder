"""Implements functionality for partitioning data frames.

Partitioning a data frame into 32 partitions::

    >>> partition('/data/foo.csv.gz', '/tmp/partitions/foo', 32)

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

import collections
import contextlib
import gzip
import hashlib
import logging
import os
import os.path as P
import resource
import sys

from typing import (
    Any,
    Callable,
    IO,
    Iterator,
    List,
    Optional,
    Union,
)

import smart_open  # type: ignore
import yaml

import datawelder.readwrite

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


def _open(path: str, mode: str) -> IO[bytes]:
    if mode == 'wb' and path.startswith('s3://'):
        import datawelder.s3
        #
        # The default S3 writers in smart_open are too memory-hungry, so use
        # a custom implementation here.
        #
        uri = smart_open.parse_uri(path)
        fileobj = datawelder.s3.LightweightWriter(
            uri.bucket_id,
            uri.key_id,
            min_part_size=datawelder.s3.MIN_MIN_PART_SIZE,
        )
        if path.endswith('.gz'):
            return gzip.GzipFile(fileobj=fileobj, mode=mode)  # type: ignore
        return fileobj  # type: ignore

    return smart_open.open(path, mode)


@contextlib.contextmanager
def open_partitions(
    path_format: str,
    num_partitions: int,
    mode: str = "rt",
) -> Iterator[List]:
    """Open partitions based on the provided string pattern.

    :param path_format: The format to use when determining partition paths.
    :param num_partitions: The number of partitions to open.
    :param mode: The mode in which the partitions must be opened.
    """
    _LOGGER.info("opening partitions: %r %r", path_format, mode)

    partition_paths = [path_format % i for i in range(num_partitions)]

    #
    # Temporarily bump the max number of open files.  If we don't do this,
    # and open a large number of partitions, things like SSL signing will
    # start failing mysteriously.
    #
    # MacOS seems to be a bit stingy, so use more conservative limits.
    #
    soft_limit = num_partitions * (10 if sys.platform == 'darwin' else 100)
    with _update_soft_limit(soft_limit):
        streams = [_open(path, mode=mode) for path in partition_paths]

        yield streams

        #
        # We want to make sure the files are _really_ closed to avoid running
        # into "Too many open files" error later.
        #
        for fin in streams:
            fin.close()


def calculate_key(key: str, num_partitions: int) -> int:
    """Map an arbitrary string to a shard number."""
    h = hashlib.md5()
    h.update(str(key).encode(datawelder.readwrite.ENCODING))
    return int(h.hexdigest(), 16) % num_partitions


class PartitionedFrame:
    def __init__(self, path: str) -> None:
        self.path = path

        with smart_open.open(P.join(path, 'datawelder.yaml')) as fin:
            self.config = yaml.safe_load(fin)

        assert self.config['config_format'] == 1

    def __len__(self):
        """Returns the number of partitions in this partitioned frame."""
        return self.config['num_partitions']

    def __iter__(self):
        def g():
            for i in range(len(self)):
                yield self[i]
        return g()

    def __getitem__(self, key: Any) -> Any:
        """Returns a partition given its ordinal number (zero-based)."""
        if not isinstance(key, int):
            raise ValueError('key must be an integer indicating the number of the partition')

        partition_number = int(key)
        if partition_number >= len(self):
            raise ValueError('key must be less than %d' % self.config['num_partitions'])

        names = list(self.field_names)
        if self.key_name not in names:
            names.insert(0, self.key_name)
        keyindex = names.index(self.key_name)

        partition_path = P.join(self.path, self.config['partition_format'] % partition_number)
        return Partition(partition_path, names, keyindex)

    @property
    def field_names(self) -> List[str]:
        """Returns the names of the fields used by this frame.

        The names will be in the order they are stored on disk.
        """
        return self.config['field_names']

    @property
    def key_index(self) -> int:
        """Returns the index of the partition key."""
        return self.config['key_index']

    @property
    def key_name(self) -> str:
        """Returns the name of the partition key."""
        return self.field_names[self.key_index]


class Partition:
    def __init__(
        self,
        path: str,
        field_names: List[str],
        key_index: int,
    ) -> None:
        """Initialize a new partition.

        The partition stores records in a JSON file, where a record is
        an unnamed tuple.

        :param path: The path to the JSON file.
        :param field_names: The names of the contained fields.
        :param key_index: The index of the key.
        """
        self.path = path
        self.field_names = field_names
        self.key_index = key_index

        self._num_fields = len(self.field_names)
        self._fin = None

    def __iter__(self):
        return self

    def __next__(self):
        #
        # FIXME: maybe optimize this later
        #
        if self._fin is None:
            self._fin = smart_open.open(self.path, 'rb')

        while True:
            try:
                record = datawelder.readwrite.load(self._fin)
            except EOFError:
                raise StopIteration
            else:
                if len(record) != self._num_fields:
                    #
                    # FIXME: Malformed record!  Prevent these from appearing
                    # in the partition in the first place.
                    #
                    continue
                return record


class MemoryFrame(PartitionedFrame):
    """A partitioned data frame that holds all of its data in memory.

    Useful for ad-hoc work.
    """
    def __init__(
        self,
        fieldnames: List[str],
        data: List[List],
        numparts: int,
        keyindex: int = 0,
    ) -> None:
        self.path = 'memory:///datawelder/memoryframe'
        self._numparts = numparts
        self._keyindex = keyindex

        partitioned_data = collections.defaultdict(list)
        numfields = len(fieldnames)
        for d in data:
            assert len(d) == numfields
            partnum = datawelder.partition.calculate_key(d[keyindex], numparts)
            partitioned_data[partnum].append(d)

        self._parts = []
        for partnum in range(numparts):
            mempart = MemoryPartition(fieldnames, partitioned_data[partnum], keyindex)
            self._parts.append(mempart)

        #
        # NB The parent class expects this dict to exist.
        #
        self.config = {
            'field_names': fieldnames,
            'key_index': keyindex,
            'key_name': fieldnames[keyindex],
            'num_partitions': numparts,
            'partition_format': '%d.json.gz',
        }

    def __getitem__(self, key):
        partnum = int(key)
        if partnum >= len(self):
            raise ValueError('key must be less than %d' % len(self))
        return self._parts[partnum]

    def __iter__(self):
        return iter(self._parts)


class MemoryPartition(Partition):
    """A partition that holds all of its data in memory.

    Useful for ad-hoc work.
    """
    def __init__(
        self,
        field_names: List[str],
        data: List[Any],
        key_index: int = 0,
    ) -> None:
        self.path = 'memory:///datawelder/memorypartition'
        self.field_names = field_names
        self.key_index = key_index

        self._num_fields = len(self.field_names)
        self._data = data
        self._iter = iter(data)

    def __iter__(self):
        return self._iter

    def __next__(self):
        return next(self._iter)


def sort_partition(path: str, key_index: int) -> None:
    """Sorts the records in this partition by the value of the partition key.

    Loads the entire partition into memory to perform the sort.
    Modifies the partition's data on disk.

    Sorting partitions simplifies joining, as long as all the partitions are
    sorted in the same way.
    """
    #
    # TODO: more memory-efficient sorting?  Is it worth it?  The partitions
    # are already quite small (by design).
    #
    def g():
        with smart_open.open(path, 'rb') as fin:
            while True:
                try:
                    yield datawelder.readwrite.load(fin)
                except EOFError:
                    break

    records = sorted(g(), key=lambda r: r[key_index])

    with smart_open.open(path, 'wb') as fout:
        for r in records:
            datawelder.readwrite.dump(r, fout)


def partition(
    reader: 'datawelder.readwrite.AbstractReader',
    destination_path: str,
    num_partitions: int,
    field_names: Optional[List[str]] = None,
    key_function: Callable[[str, int], int] = calculate_key,
) -> 'PartitionedFrame':
    """Partition a data frame."""

    if not destination_path.startswith('s3://'):
        os.makedirs(destination_path, exist_ok=True)

    partition_format = '%04d.json.gz'
    abs_partition_format = P.join(destination_path, partition_format)

    wrote = 0
    with open_partitions(abs_partition_format, num_partitions, mode='wb') as partitions:
        for i, record in enumerate(reader, 1):
            if i % 1000000 == 0:
                _LOGGER.info('processed record #%d', i)

            try:
                key = record[reader.key_index]
            except IndexError:
                _LOGGER.error('bad record on line %r: %r, skipping', i, record)
                continue

            assert key is not None
            partition_index = key_function(key, num_partitions)
            datawelder.readwrite.dump(record, partitions[partition_index])
            wrote += 1

    _LOGGER.info('wrote %d records to %d partitions', wrote, num_partitions)

    #
    # NB This can happen only after the first record has been read, because
    # at this stage we can be sure the reader has been initialized completely,
    # e.g. it knows what the field_names and key_index are.
    #
    config = {
        'field_names': reader.field_names,
        'key_index': reader.key_index,
        'source_path': str(reader.path),  # FIXME:
        'num_partitions': num_partitions,
        'partition_format': partition_format,
        'config_format': 1,
    }

    with smart_open.open(P.join(destination_path, 'datawelder.yaml'), 'w') as fout:
        yaml.dump(config, fout)

    #
    # N.B. Parallelize the sorting?  Need to be careful of EOM because the
    # sort loads each partition into memory in its entirety.
    #
    frame = PartitionedFrame(destination_path)
    for part in frame:
        sort_partition(part.path, part.key_index)

    return frame


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
            'The names of fields to load. If not specified, will attempt to '
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
        choices=(datawelder.readwrite.CSV, datawelder.readwrite.JSON),
        help='The format of the source file',
    )
    parser.add_argument(
        '--fmtparams',
        type=str,
        nargs='*',
        help='Additional params to pass to the reader, in key=value format',
    )
    parser.add_argument(
        '--fieldtypes',
        type=str,
        nargs='+',
        help='The data types for each column (CSV only)',
    )
    parser.add_argument('--loglevel', default=logging.INFO)
    args = parser.parse_args()

    if args.keyindex and args.keyname:
        parser.error('--keyindex and --keyname are mutually exclusive')

    if args.fieldtypes and args.fieldnames and len(args.fieldtypes) != len(args.fieldnames):
        parser.error('fieldtypes and fieldnames must have the same length if specified')

    logging.basicConfig(level=args.loglevel)

    key: Union[int, str, None] = None
    if args.keyindex:
        key = args.keyindex
    elif args.keyname:
        key = args.keyname
    else:
        key = 0
    assert key is not None

    fmtparams = datawelder.readwrite.parse_fmtparams(args.fmtparams)
    if args.fieldtypes:
        fieldtypes = list(datawelder.readwrite.parse_types(args.fieldtypes))
    else:
        fieldtypes = None

    with datawelder.readwrite.open_reader(
        None if args.source == '-' else args.source,
        key,
        args.fieldnames,
        args.format,
        fmtparams,
        fieldtypes,
    ) as reader:
        partition(reader, args.destination, args.numpartitions)


if __name__ == '__main__':
    main()
