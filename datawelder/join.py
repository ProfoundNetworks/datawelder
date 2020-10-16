"""Joins multiple partitioned data frames together.

Example usage::

    >>> foo = partition('/data/foo.csv.gz', '/tmp/partitions/foo')
    >>> bar = partition('/data/bar.csv.gz', '/tmp/partitions/bar')
    >>> baz = partition('/data/baz.csv.gz', '/tmp/partitions/baz')

"""

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
import datawelder.readwrite
import datawelder.partition

if TYPE_CHECKING:
    from . import partition

_LOGGER = logging.getLogger(__name__)


def _name_fields(partitions_or_frames: List[Any]) -> List[str]:
    """Assign names to fields in preparation for a join."""
    leftpart, others = partitions_or_frames[0], partitions_or_frames[1:]
    field_names = ['0.%s' % f for f in leftpart.field_names]
    for i, part in enumerate(others, 1):
        field_names.extend(['%d.%s' % (i, n) for n in part.field_names])
    return field_names


def _join_partitions(partitions: List[datawelder.partition.Partition]) -> Iterator[Tuple]:

    def mkdefault(p):
        return [None for _ in p.field_names]

    def getnext(p):
        try:
            return next(p)
        except StopIteration:
            return None

    leftpart = partitions.pop(0)
    peek = [getnext(p) for p in partitions]
    defaults = [mkdefault(p) for p in partitions]

    for leftrecord in leftpart:
        leftkey = leftrecord[leftpart.key_index]
        joinedrecord = list(leftrecord)
        for i, rightpart in enumerate(partitions):
            while peek[i] is not None and peek[i][rightpart.key_index] < leftkey:
                peek[i] = getnext(rightpart)

            if peek[i] is None:
                joinedrecord.extend(defaults[i])
            else:
                joinedrecord.extend(peek[i])

            yield tuple(joinedrecord)


def join_partitions(
    partition_num: int,
    frame_paths: List[str],
    output_path: str,
    output_format: str = datawelder.readwrite.PICKLE,
    fmtparams: Optional[Dict[str, str]] = None,
    selected_fields: Optional[List[str]] = None,
    aliases: Optional[List[str]] = None,
) -> None:
    if selected_fields and aliases and len(selected_fields) != len(aliases):
        raise ValueError('selected_fields and aliases must have same length if specified')

    frames = [datawelder.partition.PartitionedFrame(fp) for fp in frame_paths]
    partitions = [frame[partition_num] for frame in frames]
    field_names = _name_fields(partitions)

    #
    # Do not output the join key multiple times, unless the caller explicitly
    # asked for it.
    #
    if selected_fields is None:
        selected_fields = aliases = list(field_names)
        for i, part in enumerate(partitions):
            if i > 0:
                name = '%d.%s' % (i, part.field_names[part.key_index])
                selected_fields.remove(name)

    if aliases is None:
        aliases = list(selected_fields)

    field_indices = [field_names.index(f) for f in selected_fields]

    with datawelder.readwrite.open_writer(
        output_path,
        output_format,
        partition_num,
        field_indices=field_indices,
        field_names=aliases,
        fmtparams=fmtparams,
    ) as writer:
        for record in _join_partitions(partitions):
            writer.write(record)


def join(
    frames: List['partition.PartitionedFrame'],
    destination: str,
    output_format: str = datawelder.readwrite.PICKLE,
    fmtparams: Optional[Dict[str, str]] = None,
    selected_fields: Optional[List[str]] = None,
    aliases: Optional[List[str]] = None,
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
                [f.path for f in frames],
                temp_path,
                output_format,
                fmtparams,
                selected_fields,
                aliases,
            )

    with tempfile.TemporaryDirectory(prefix='datawelder-') as temp_dir:
        temp_paths = [P.join(temp_dir, str(i)) for i in range(num_partitions)]

        if subs == 1:
            for args in generate_work(temp_paths):
                join_partitions(*args)
        else:
            pool = multiprocessing.Pool(subs)
            pool.starmap(join_partitions, generate_work(temp_paths))
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
    query: str,
) -> Tuple[List[str], List[str]]:
    parsed_query = list(_parse_select(query))
    _, _, selected_names, aliases = zip(*parsed_query)

    field_names = _name_fields(dataframes)
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

    return list(selected_names), list(aliases)


def main():
    import argparse
    import datawelder.partition

    parser = argparse.ArgumentParser(description='Join partitioned dataframes together')
    parser.add_argument('destination', help='Where to save the result of the join')
    parser.add_argument('sources', nargs='+', help='Which partitioned dataframes (subdirectories) to join')
    parser.add_argument(
        '--format',
        default=datawelder.readwrite.PICKLE,
        choices=(
            datawelder.readwrite.CSV,
            datawelder.readwrite.JSON,
            datawelder.readwrite.PICKLE,
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

    fmtparams = datawelder.readwrite.parse_fmtparams(args.fmtparams)
    join(
        dataframes,
        args.destination,
        args.format,
        fmtparams=fmtparams,
        selected_fields=selected_names,
        aliases=aliases,
        subs=args.subs,
    )


if __name__ == '__main__':
    main()
