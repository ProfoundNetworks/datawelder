"""Joins multiple partitioned data frames together.

Example usage::

    >>> foo = partition('/data/foo.csv.gz', '/tmp/partitions/foo')
    >>> bar = partition('/data/bar.csv.gz', '/tmp/partitions/bar')
    >>> baz = partition('/data/baz.csv.gz', '/tmp/partitions/baz')

"""

import collections
import logging
import multiprocessing
import os.path as P
import tempfile
import sys

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

Field = Tuple[int, int, str]
"""A field definition for a join operation.

The first element is an integer that indicates the ordinal number of the
dataset the field comes from.

The second element is an integer than indicates the ordinal number of the
field within that dataset.

The third element is the name of the field as it will appear in the joined record.
"""

if TYPE_CHECKING:
    from . import partition

_LOGGER = logging.getLogger(__name__)


def _join_partitions(partitions: List[datawelder.partition.Partition]) -> Iterator[Tuple]:
    """Join partitions assuming that they are sorted by the partition key."""

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
    leftkey = None

    for leftrecord in leftpart:
        if leftkey is not None and leftkey > leftrecord[leftpart.key_index]:
            raise RuntimeError('%r is not properly sorted' % leftpart)
        leftkey = leftrecord[leftpart.key_index]

        joinedrecord = list(leftrecord)
        for i, rightpart in enumerate(partitions):
            while peek[i] is not None and peek[i][rightpart.key_index] < leftkey:
                nextrecord = getnext(rightpart)
                if nextrecord[rightpart.key_index] < peek[i][rightpart.key_index]:
                    raise RuntimeError('%r is not properly sorted' % rightpart)
                peek[i] = nextrecord

            if peek[i] is None or peek[i][rightpart.key_index] != leftkey:
                joinedrecord.extend(defaults[i])
            else:
                joinedrecord.extend(peek[i])

            yield tuple(joinedrecord)


def _calculate_indices(
    frame_headers: List[List[str]],
    selected_fields: List[Field],
) -> List[int]:
    """Calculate the required indices into the joined record."""
    joined_fields: List[Tuple[int, int]] = []
    for framenum, header in enumerate(frame_headers):
        for fieldnum, fieldname in enumerate(header):
            joined_fields.append((framenum, fieldnum))
    indices = [
        joined_fields.index((framenum, fieldnum))
        for (framenum, fieldnum, unused_alias) in selected_fields
    ]
    return indices


def join_partitions(
    partition_num: int,
    frame_paths: List[str],
    output_path: Optional[str],
    output_format: str = datawelder.readwrite.PICKLE,
    fmtparams: Optional[Dict[str, str]] = None,
    fields: Optional[List[Field]] = None,
) -> None:
    if output_path is None:
        #
        # NB. smart_open can handle this mess
        #
        output_path = sys.stdout.buffer  # type: ignore

    frames = [datawelder.partition.PartitionedFrame(fp) for fp in frame_paths]
    headers = [f.field_names for f in frames]

    if fields is None:
        fields = _scrub_fields(headers, None)

    field_indices = _calculate_indices(headers, fields)
    field_names = [alias for (unused_framenum, unused_fieldnum, alias) in fields]

    partitions = [frame[partition_num] for frame in frames]
    with datawelder.readwrite.open_writer(
        output_path,
        output_format,
        partition_num,
        field_indices=field_indices,
        field_names=field_names,
        fmtparams=fmtparams,
    ) as writer:
        for record in _join_partitions(partitions):
            writer.write(record)


def join(
    frames: List['partition.PartitionedFrame'],
    destination: str,
    output_format: str = datawelder.readwrite.PICKLE,
    fields: Optional[List[Field]] = None,
    fmtparams: Optional[Dict[str, str]] = None,
    subs: Optional[int] = None,
) -> Any:
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
                fields,
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


def _split_compound(compound):
    try:
        framenum, fieldname = compound.split('.', 1)
    except ValueError:
        return None, compound
    else:
        return int(framenum), fieldname


def _parse_select(query: str) -> Iterator[Tuple]:
    for clause in query.split(','):
        words = clause.strip().split(' ')
        if len(words) == 3 and words[1].lower() == 'as':
            framenum, fieldname = _split_compound(words[0])
            yield framenum, fieldname, words[2]
        elif len(words) == 1:
            framenum, fieldname = _split_compound(words[0])
            yield framenum, fieldname, None
        else:
            raise ValueError('malformed SELECT query: %r' % query)


def _scrub_fields(
    frame_headers: List[List[str]],
    dirty_fields: Optional[List[Tuple]],
) -> List[Field]:
    lut = collections.defaultdict(list)
    for framenum, header in enumerate(frame_headers):
        for fieldname in header:
            lut[fieldname].append(framenum)

    #
    # Select all the fields.
    #
    if dirty_fields is None:
        dirty_fields = []
        for framenum, header in enumerate(frame_headers):
            for fieldname in header:
                dirty_fields.append((framenum, fieldname, None))

    assert dirty_fields

    selected: List[Field] = []
    used_aliases = set()

    for framenum, fieldname, alias in dirty_fields:
        if fieldname not in lut:
            raise ValueError('expected %r to be one of %r' % (fieldname, sorted(lut)))

        if framenum is None:
            candidate_frames = lut[fieldname]
            if len(candidate_frames) > 1:
                alt = ['%d.%s' % (fn, fieldname) for fn in candidate_frames]
                raise ValueError(
                    '%r is an ambiguous name, '
                    'try the following instead: %r' % (fieldname, alt),
                )
            framenum = candidate_frames[0]

        assert framenum is not None
        fieldnum = frame_headers[framenum].index(fieldname)

        if alias and alias in used_aliases:
            raise ValueError('%r is a non-unique alias' % alias)
        elif alias is None and fieldname not in used_aliases:
            alias = fieldname
        elif alias is None:
            alias = '%s_%d' % (fieldname, framenum)

        assert fieldnum is not None
        assert alias
        selected.append((framenum, fieldnum, alias))

        used_aliases.add(alias)

    return selected


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Join partitioned dataframes together')
    parser.add_argument('destination', help='Where to save the result of the join')
    parser.add_argument('sources', nargs='+', help='Which partitioned dataframes to join')
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
    headers = [df.field_names for df in dataframes]

    fields = None
    if args.select:
        dirty_fields = list(_parse_select(args.select))
        fields = _scrub_fields(headers, dirty_fields)
    else:
        fields = _scrub_fields(headers, None)

    fmtparams = datawelder.readwrite.parse_fmtparams(args.fmtparams)
    join(
        dataframes,
        args.destination,
        args.format,
        fields=fields,
        fmtparams=fmtparams,
        subs=args.subs,
    )


if __name__ == '__main__':
    main()
