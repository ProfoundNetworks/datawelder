"""Joins multiple partitioned data frames together.

Example usage::

    >>> foo = partition('/data/foo.csv.gz', '/tmp/partitions/foo')
    >>> bar = partition('/data/bar.csv.gz', '/tmp/partitions/bar')
    >>> baz = partition('/data/baz.csv.gz', '/tmp/partitions/baz')

"""

import csv
import functools
import json
import multiprocessing
import os.path as P
import pickle
import tempfile

from typing import (
    Any,
    Dict,
    List,
    Optional,
    TYPE_CHECKING,
)

import smart_open  # type: ignore

from . import cat

if TYPE_CHECKING:
    from . import partition


#
# TODO: Consider using abc for actual abstract classes.
#
class AbstractWriter:
    def __init__(self, path: str, partition_num: int):
        self._path = path
        self._partition_num = partition_num

    def __enter__(self):
        self._fout = smart_open.open(self._path, 'wb')
        return self

    def __exit__(self, *exc):
        pass

    def write(self, record: List[Any]) -> None:
        raise NotImplementedError


class PickleWriter(AbstractWriter):
    def write(self, record):
        pickle.dump(record, self._fout)


class JsonWriter(AbstractWriter):
    def __init__(self, path: str, partition_num: int, field_names: str):
        super().__init__(path, partition_num)
        self._field_names = field_names

    def write(self, record):
        assert len(self._field_names) == len(record)
        record_dict = dict(zip(self._field_names, record))
        self._fout.write(json.dumps(record_dict).encode('utf-8'))
        self._fout.write(b'\n')


class CsvWriter(AbstractWriter):
    def __init__(
        self,
        path,
        partition_num: int,
        columns: List[Optional[str]] = None,
        write_header: bool = False,
        **kwargs,
    ):
        super().__init__(path, partition_num)
        self._columns = columns
        self._column_indices = [i for (i, colname) in enumerate(columns) if colname]
        self._kwargs = kwargs
        self._write_header = write_header

    def __enter__(self):
        self._fout = smart_open.open(self._path, 'wt')
        self._writer = csv.writer(self._fout, **self._kwargs)

        header = [colname for colname in self._columns if colname]
        if self._write_header and self._partition_num == 0:
            self._writer.writerow(header)

        return self

    def write(self, record):
        assert len(record) == len(self._columns)
        row = [record[i] for i in self._column_indices]
        self._writer.writerow(row)


def _join_partitions(
    partition_num: int,
    partitions: List['partition.Partition'],
    output_path: str,
    writer_class: Any = PickleWriter,
) -> None:
    #
    # Load all partitions into memory, except for the first one, because we
    # will be iterating over it.
    #
    lookup: List[Dict] = [dict() for _ in partitions]
    left_partition = partitions[0]
    partitions = partitions[1:]

    with writer_class(output_path, partition_num) as writer:
        for left_record in left_partition:
            left_key = left_record[left_partition.key_index]
            joined_record = list(left_record)

            for i, right_partition in enumerate(partitions, 1):
                try:
                    right_record = lookup[i][left_key]
                except KeyError:
                    right_record = [None for _ in right_partition.field_names]

                right_record.pop(right_partition.key_index)
                joined_record.extend(right_record)

            writer.write(joined_record)


def join_field_names(frames: List['partition.PartitionedFrame']) -> List[str]:
    num_partitions = frames[0].config['num_partitions']
    for f in frames:
        assert f.config['num_partitions'] == num_partitions

    fields = []
    for i, frame in enumerate(frames):
        field_frames = list(frame.config['field_names'])
        if i != 0:
            field_frames.pop(frame.config['key_index'])
        fields.extend(field_frames)

    return fields


def join(
    frames: List['partition.PartitionedFrame'],
    destination: str,
    writer_class: Any = PickleWriter,
    num_subprocesses: Optional[int] = None,
) -> Any:
    #
    # NB. The following fails when writer_class is a partial
    #
    # assert issubclass(writer_class, AbstractWriter)

    #
    # If the number of partitions is different, then joining is impossible.
    #
    num_partitions = frames[0].config['num_partitions']
    for f in frames:
        assert f.config['num_partitions'] == num_partitions

    if num_subprocesses is None:
        num_subprocesses = multiprocessing.cpu_count()

    def generate_work(temp_paths):
        assert len(temp_paths) == num_partitions
        for partition_num, temp_path in enumerate(temp_paths):
            yield partition_num, [f[partition_num] for f in frames], temp_path, writer_class

    with tempfile.TemporaryDirectory(prefix='datawelder-') as temp_dir:
        temp_paths = [P.join(temp_dir, str(i)) for i in range(num_partitions)]

        if num_subprocesses == 1:
            for args in generate_work(temp_paths):
                _join_partitions(*args)
        else:
            pool = multiprocessing.Pool(num_subprocesses)
            pool.starmap(_join_partitions, generate_work(temp_paths))
        cat.cat(temp_paths, destination)


def main():
    import sys
    import datawelder.partition

    destination = sys.argv[1]
    dataframes = [datawelder.partition.PartitionedFrame(x) for x in sys.argv[2:]]
    columns = join_field_names(dataframes)
    # writer_class = functools.partial(CsvWriter, write_header=True, delimiter='|', columns=columns)
    writer_class = functools.partial(JsonWriter, field_names=columns)
    join(dataframes, destination, writer_class=writer_class)


if __name__ == '__main__':
    main()
