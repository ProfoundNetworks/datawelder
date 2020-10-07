"""Joins multiple partitioned data frames together.

Example usage::

    >>> foo = partition('/data/foo.csv.gz', '/tmp/partitions/foo')
    >>> bar = partition('/data/bar.csv.gz', '/tmp/partitions/bar')
    >>> baz = partition('/data/baz.csv.gz', '/tmp/partitions/baz')

"""

import functools
import logging
import multiprocessing
import os.path as P
import tempfile

from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

import datawelder.cat
import datawelder.io

if TYPE_CHECKING:
    from . import partition

_LOGGER = logging.getLogger(__name__)


def _join_partitions(
    partition_num: int,
    partitions: List['partition.Partition'],
    output_path: str,
    selected_fields: Optional[List[str]] = None,
    aliases: Optional[List[str]] = None,
    writer_class: Any = 'datawelder.io.PickleWriter',
) -> None:
    #
    # Load all partitions into memory, except for the first one, because we
    # will be iterating over it.
    #
    lookup: List[Dict] = [dict() for _ in partitions]
    left_partition = partitions[0]
    partitions = partitions[1:]
    field_names = ['0.%s' % f for f in left_partition.field_names]

    for i, part in enumerate(partitions, 1):
        field_names.extend(['%d.%s' % (i, n) for n in part.field_names])
        for record in part:
            lookup[i][record[part.key_index]] = record

    #
    # Do not output the join key multiple times, because it is redundant.
    #
    if selected_fields is None:
        selected_fields = aliases = list(field_names)
        for i, part in enumerate(partitions, 1):
            selected_fields.remove('%d.%s' % (i, part.field_names[part.key_index]))

    field_indices = [field_names.index(f) for f in selected_fields]
    with writer_class(output_path, partition_num, field_indices, aliases) as writer:
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
                joined_record.extend(right_record)

            writer.write(joined_record)


def join_field_names(frames: List['partition.PartitionedFrame']) -> List[str]:
    num_partitions = frames[0].config['num_partitions']
    for f in frames:
        assert f.config['num_partitions'] == num_partitions

    fields = []
    for i, frame in enumerate(frames):
        field_frames = list(frame.selected_fields)
        fields.extend(['%d.%s' % (i, f) for f in field_frames])

    return fields


def join(
    frames: List['partition.PartitionedFrame'],
    destination: str,
    selected_fields: Optional[List[str]] = None,
    aliases: Optional[List[str]] = None,
    writer_class: Any = 'datawelder.io.PickleWriter',
    subs: Optional[int] = None,
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

    if subs is None:
        subs = multiprocessing.cpu_count()

    def generate_work(temp_paths):
        assert len(temp_paths) == num_partitions
        for partition_num, temp_path in enumerate(temp_paths):
            yield (
                partition_num,
                [f[partition_num] for f in frames],
                temp_path,
                selected_fields,
                aliases,
                writer_class,
            )

    with tempfile.TemporaryDirectory(prefix='datawelder-') as temp_dir:
        temp_paths = [P.join(temp_dir, str(i)) for i in range(num_partitions)]

        if subs == 1:
            for args in generate_work(temp_paths):
                _join_partitions(*args)
        else:
            pool = multiprocessing.Pool(subs)
            pool.starmap(_join_partitions, generate_work(temp_paths))
        datawelder.cat.cat(temp_paths, destination)


def _parse_select(query: str) -> Iterator[Tuple[int, str, str, str]]:
    for clause in query.split(','):
        words = clause.strip().split(' ')
        if len(words) == 3 and words[1].lower() == 'as':
            prefix, suffix = words[0].split('.', 1)
            yield int(prefix), suffix, words[0], words[2]
        elif len(words) == 1:
            prefix, suffix = words[0].split('.', 1)
            yield int(prefix), suffix, words[0], words[0]
        else:
            raise ValueError('bad SELECT query: %r' % query)


def _select(
    dataframes: List['partition.PartitionedFrame'],
    query: str
) -> Tuple[List[str], List[str]]:
    parsed_query = list(_parse_select(query))
    _, _, selected_names, aliases = zip(*parsed_query)

    field_names = join_field_names(dataframes)
    for field in selected_names:
        if field not in field_names:
            raise ValueError('expected %r to be one of %r' % (field, field_names))

    if len(set(aliases)) != len(aliases):
        raise ValueError('field aliases are not unique')

    for i, df in enumerate(dataframes):
        fields_to_keep = [
            fieldname
            for (datasetidx, fieldname, prefixed_fieldname, alias) in parsed_query
            if datasetidx == i
        ]
        if not fields_to_keep:
            raise ValueError('not keeping any fields from dataframe %d' % i)

        df.select(fields_to_keep)

    return list(selected_names), list(aliases)


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
        help='Select a subset of output fields to keep',
    )
    parser.add_argument('--subs', type=int)
    parser.add_argument('--loglevel', default=logging.INFO)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    dataframes = [datawelder.partition.PartitionedFrame(x) for x in args.sources]

    if args.select:
        selected_names, aliases = _select(dataframes, args.select)
    else:
        selected_names = aliases = None

    if args.format == datawelder.io.PICKLE:
        writer_class = datawelder.io.PickleWriter
    elif args.format == datawelder.io.JSON:
        writer_class = datawelder.io.JsonWriter
    elif args.format == datawelder.io.CSV:
        writer_class = datawelder.io.CsvWriter
    else:
        assert False

    fmtparams = datawelder.io.parse_fmtparams(args.fmtparams)
    writer_class = functools.partial(writer_class, fmtparams=fmtparams)

    join(
        dataframes,
        args.destination,
        selected_names,
        aliases,
        writer_class=writer_class,
        subs=args.subs,
    )


if __name__ == '__main__':
    main()
