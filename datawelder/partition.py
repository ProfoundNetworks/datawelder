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
import csv
import json
import logging
import os
import os.path as P
import pickle
import resource
import zlib

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
)

import smart_open  # type: ignore
import yaml

_LOGGER = logging.getLogger(__name__)
_TEXT_ENCODING = 'utf-8'


def _sniff(path: str) -> str:
    #
    # FIXME: improve this
    #
    if '.csv' in path:
        return 'csv'
    elif '.json' in path:
        return 'json'

    assert False


class AbstractReader:
    def __init__(
        self,
        path: str,
        field_names: Optional[List[str]] = None,
        key_index: int = 0
    ) -> None:
        self._path = path

    def __enter__(self):
        self._fin = smart_open.open(self._path, 'r')
        return self

    def __exit__(self, *exc):
        pass

    def next(self) -> Iterator[List]:
        raise NotImplementedError


def _read_csv(
    source_path: str,
    key_index: int,
    field_names: List[str],
    params: Optional[Dict[str, Any]] = None,
) -> Iterator[Tuple]:
    if params is None:
        params = {}
    with smart_open.open(source_path) as fin:
        reader = csv.reader(fin, **params)
        if not field_names:
            field_names.extend(next(reader))
            _LOGGER.info('assuming that the key is %r', field_names[key_index])
        for record in reader:
            yield tuple(record)


def _read_json(
    source_path: str,
    key_index: int,
    field_names: List[str],
) -> Iterator[Tuple]:
    with smart_open.open(source_path) as fin:
        for line in fin:
            record_dict = json.loads(line)
            if not field_names:
                field_names.extend(sorted(record_dict))
                _LOGGER.info('assuming that the key is %r', field_names[key_index])

            #
            # NB We're potentially introducing null values here...
            #
            record_tuple = tuple([record_dict.get(f) for f in field_names])
            yield record_tuple


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
    partition_paths = [path_format % i for i in range(num_partitions)]

    #
    # Temporarily bump the max number of open files to twice the number of
    # partitions, to be safe.  This appears to only be necessary on MacOS.
    #
    with _update_soft_limit(num_partitions * 2):
        gzip_streams = [smart_open.open(path, mode=mode) for path in partition_paths]
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
    return zlib.adler32(key.encode(_TEXT_ENCODING)) % num_shards


class PartitionedFrame:
    def __init__(self, path: str) -> None:
        self.path = path

        with smart_open.open(P.join(path, 'datawelder.yaml')) as fin:
            self.config = yaml.safe_load(fin)

        assert self.config['config_format'] == 1

    def __len__(self):
        return self.config['num_partitions']

    def __getitem__(self, key: Any) -> Any:
        if not isinstance(key, int):
            raise ValueError('key must be an integer indicating the number of the partition')

        partition_number = int(key)
        if partition_number > self.config['num_partitions']:
            raise ValueError('key must be less than %d' % self.config['num_partitions'])

        partition_path = P.join(self.path, self.config['partition_format'] % partition_number)
        return Partition(partition_path, self.config['field_names'], self.config['key_index'])


class Partition:
    def __init__(self, path: str, field_names: List[str], key_index: int) -> None:
        self.path = path
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
            return pickle.load(self._fin)
        except EOFError:
            raise StopIteration


def partition(
    source_path: str,
    destination_path: str,
    num_partitions: int,
    field_names: Optional[List[str]] = None,
    key_index: int = 0,
    source_format: Optional[str] = None,
    csv_params: Optional[Dict[str, Any]] = None,
    key_function: Callable[[str, int], int] = calculate_key,
) -> 'PartitionedFrame':
    """Partition a data frame."""
    if source_format is None:
        source_format = _sniff(source_path)

    if field_names is None:
        field_names = []

    if source_format == 'csv':
        reader = _read_csv(source_path, key_index, field_names, csv_params)
    elif source_format == 'json':
        reader = _read_json(source_path, key_index, field_names)
    else:
        assert False

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
        'field_names': field_names,
        'key_index': key_index,
        'source_path': source_path,
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
        default=0,
        help='The index of the partition key in the `fieldnames` list',
    )
    parser.add_argument('--delimiter', default='|')
    parser.add_argument('--quoting', type=int, default=csv.QUOTE_NONE)
    parser.add_argument('--quotechar', default='')
    parser.add_argument('--loglevel', default=logging.INFO)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    csv_params = {
        'delimiter': args.delimiter,
        'quoting': args.quoting,
        'quotechar': args.quotechar,
    }
    partition(args.source, args.destination, args.numpartitions, csv_params=csv_params)


if __name__ == '__main__':
    main()
