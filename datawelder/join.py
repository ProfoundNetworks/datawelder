"""Joins multiple partitioned data frames together.

Example usage::

    >>> foo = partition('/data/foo.csv.gz', '/tmp/partitions/foo')
    >>> bar = partition('/data/bar.csv.gz', '/tmp/partitions/bar')
    >>> baz = partition('/data/baz.csv.gz', '/tmp/partitions/baz')

"""

import functools
import multiprocessing
import os.path as P
import tempfile

from typing import (
    Any,
    Dict,
    List,
    Optional,
    TYPE_CHECKING,
)

import datawelder.cat
import datawelder.io

if TYPE_CHECKING:
    from . import partition


def _join_partitions(
    partition_num: int,
    partitions: List['partition.Partition'],
    output_path: str,
    writer_class: Any = 'datawelder.io.PickleWriter',
) -> None:
    #
    # Load all partitions into memory, except for the first one, because we
    # will be iterating over it.
    #
    lookup: List[Dict] = [dict() for _ in partitions]
    left_partition = partitions[0]
    partitions = partitions[1:]

    for i, part in enumerate(partitions, 1):
        for record in part:
            lookup[i][record[part.key_index]] = record

    with writer_class(output_path, partition_num) as writer:
        for left_record in left_partition:
            left_key = left_record[left_partition.key_index]
            joined_record = list(left_record)

            for i, right_partition in enumerate(partitions, 1):
                try:
                    #
                    # Convert the tuple to a list here, as opposed to when
                    # creating the lookup, because tuples are more compact.
                    # We'll be throwing the list away shortly anyway.
                    #
                    right_record = list(lookup[i][left_key])
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
    writer_class: Any = 'datawelder.io.PickleWriter',
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
        datawelder.cat.cat(temp_paths, destination)


def main():
    import argparse
    import datawelder.partition

    parser = argparse.ArgumentParser(description='Join partitioned dataframes together')
    parser.add_argument('destination', help='Where to save the result of the join')
    parser.add_argument('sources', nargs='+', help='Which partitioned dataframes (subdirectories) to join')
    parser.add_argument(
        '--format',
        default=datawelder.io.PICKLE,
        choices=(
            datawelder.io.CSV,
            datawelder.io.JSON,
            datawelder.io.PICKLE,
        ),
    )
    parser.add_argument(
        '--fmtparams',
        type=str,
        nargs='*',
        help='Additional params to pass to the writer, in key=value format',
    )
    parser.add_argument(
        '--select',
        type=str,
        nargs='*',
        help='Select a subset of output fields to keep',
    )
    parser.add_argument('--subprocesses', type=int)
    args = parser.parse_args()

    dataframes = [datawelder.partition.PartitionedFrame(x) for x in args.sources]
    fieldnames = join_field_names(dataframes)

    fmtparams = datawelder.io.parse_fmtparams(args.fmtparams)

    if args.format == datawelder.io.PICKLE:
        writer_class = datawelder.io.PickleWriter
    elif args.format == datawelder.io.JSON:
        writer_class = datawelder.io.JsonWriter
    elif args.format == datawelder.io.CSV:
        writer_class = datawelder.io.CsvWriter
    else:
        assert False

    writer_class = functools.partial(
        writer_class,
        fieldnames=fieldnames,
        fmtparams=fmtparams,
        select=args.select,
    )
    join(
        dataframes,
        args.destination,
        writer_class=writer_class,
        num_subprocesses=args.subprocesses,
    )


if __name__ == '__main__':
    main()
