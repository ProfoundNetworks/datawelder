import pytest

import datawelder.partition
import datawelder.join


def test_parse_select_simple():
    query = 'foo, bar, baz'
    expected = [(None, 'foo', None), (None, 'bar', None), (None, 'baz', None)]
    actual = list(datawelder.join._parse_select(query))
    assert actual == expected


def test_parse_select_qualified():
    query = '1.foo, 1.bar, 2.baz'
    expected = [(1, 'foo', None), (1, 'bar', None), (2, 'baz', None)]
    actual = list(datawelder.join._parse_select(query))
    assert actual == expected


def test_parse_select_aliased():
    query = 'foo as FOO, bar as BAR, baz as BAZ'
    expected = [(None, 'foo', 'FOO'), (None, 'bar', 'BAR'), (None, 'baz', 'BAZ')]
    actual = list(datawelder.join._parse_select(query))
    assert actual == expected


def test_parse_select_qualified_aliased():
    query = '1.foo as FOO, 1.bar AS BaR, 2.baz aS bAZ'
    expected = [(1, 'foo', 'FOO'), (1, 'bar', 'BaR'), (2, 'baz', 'bAZ')]
    actual = list(datawelder.join._parse_select(query))
    assert actual == expected


def test_parse_select_malformed():
    with pytest.raises(ValueError):
        list(datawelder.join._parse_select('foo az fu, bar iz ba'))


def test_scrub_fields_simple():
    headers = [['foo', 'bar'], ['baz', 'boz']]
    fields = [(0, 'foo', None), (1, 'boz', None)]
    expected = [(0, 0, 'foo'), (1, 1, 'boz')]
    actual = datawelder.join._scrub_fields(headers, fields)
    assert actual == expected


def test_scrub_fields_auto():
    headers = [['foo', 'bar'], ['baz', 'boz']]
    expected = [(0, 0, 'foo'), (0, 1, 'bar'), (1, 0, 'baz'), (1, 1, 'boz')]
    actual = datawelder.join._scrub_fields(headers, None)
    assert actual == expected


def test_scrub_fields_ambiguous():
    headers = [['foo', 'bar'], ['baz', 'foo']]
    fields = [(None, 'foo', 'FOO'), (None, 'baz', 'BAZ')]
    with pytest.raises(ValueError):
        datawelder.join._scrub_fields(headers, fields)


def test_scrub_fields_unknown():
    headers = [['foo', 'bar'], ['baz', 'foo']]
    fields = [(0, 'foo', 'FOO'), (1, 'buzz', 'BAZ')]
    with pytest.raises(ValueError):
        datawelder.join._scrub_fields(headers, fields)


def test_unique_aliases():
    headers = [['foo', 'bar'], ['baz', 'foo']]
    fields = [(0, 'foo', 'foo'), (1, 'baz', 'foo')]
    with pytest.raises(ValueError):
        datawelder.join._scrub_fields(headers, fields)


def test_auto_alias():
    headers = [['foo', 'bar'], ['baz', 'foo']]
    fields = [(0, 'foo', None), (1, 'foo', None)]
    expected = [(0, 0, 'foo'), (1, 1, 'foo_1')]
    actual = datawelder.join._scrub_fields(headers, fields)
    assert actual == expected


def test_join_partitions():
    left = datawelder.partition.MemoryPartition(
        ('iso3', 'name'),
        [('AU', 'Australia'), ('RU', 'Russia')],
    )
    right = datawelder.partition.MemoryPartition(
        ('iso', 'currency'),
        [('AU', 'Dollar'), ('RU', 'Rouble')],
    )
    expected = [
        ('AU', 'Australia', 'AU', 'Dollar'),
        ('RU', 'Russia', 'RU', 'Rouble'),
    ]
    actual = list(datawelder.join._join_partitions([left, right]))
    assert actual == expected


def test_join_threeway():
    name = datawelder.partition.MemoryPartition(
        ('iso3', 'name'),
        [('AU', 'Australia'), ('RU', 'Russia')],
    )
    currency = datawelder.partition.MemoryPartition(
        ('iso', 'currency'),
        [('AU', 'Dollar'), ('RU', 'Rouble')],
    )
    capital = datawelder.partition.MemoryPartition(
        ('iso', 'capital'),
        [('AU', 'Canberra'), ('RU', 'Moscow')],
    )
    expected = [
        ('AU', 'Australia', 'AU', 'Dollar', 'AU', 'Canberra'),
        ('RU', 'Russia', 'RU', 'Rouble', 'RU', 'Moscow'),
    ]
    actual = list(datawelder.join._join_partitions([name, currency, capital]))
    assert actual == expected


def test_join_partitions_missing_left():
    left = datawelder.partition.MemoryPartition(
        ('iso', 'name'),
        [('AU', 'Australia'), ('KP', 'Kraplakistan'), ('RU', 'Russia')],
    )
    right = datawelder.partition.MemoryPartition(
        ('iso3', 'currency'),
        [('AU', 'Dollar'), ('RU', 'Rouble')],
    )
    expected = [
        ('AU', 'Australia', 'AU', 'Dollar'),
        ('KP', 'Kraplakistan', None, None),
        ('RU', 'Russia', 'RU', 'Rouble'),
    ]
    actual = list(datawelder.join._join_partitions([left, right]))
    assert actual == expected


def test_join_partitions_missing_right():
    left = datawelder.partition.MemoryPartition(
        ('iso', 'name'),
        [('AU', 'Australia'), ('RU', 'Russia')],
    )
    right = datawelder.partition.MemoryPartition(
        ('iso3', 'currency'),
        [('AU', 'Dollar'), ('KPL', '???'), ('RU', 'Rouble')],
    )
    expected = [
        ('AU', 'Australia', 'AU', 'Dollar'),
        ('RU', 'Russia', 'RU', 'Rouble'),
    ]
    actual = list(datawelder.join._join_partitions([left, right]))
    assert actual == expected


def test_join_partitions_unsorted_left():
    left = datawelder.partition.MemoryPartition(
        ('iso', 'name'),
        [('RU', 'Russia'), ('AU', 'Australia')],
    )
    right = datawelder.partition.MemoryPartition(
        ('iso3', 'currency'),
        [('AU', 'Dollar'), ('RU', 'Rouble')],
    )
    with pytest.raises(RuntimeError):
        list(datawelder.join._join_partitions([left, right]))


@pytest.mark.skip('not sure how to test this particular edge case')
def test_join_partitions_unsorted_right():
    left = datawelder.partition.MemoryPartition(
        ('iso', 'name'),
        [('AU', 'Australia'), ('RU', 'Russia')]
    )
    right = datawelder.partition.MemoryPartition(
        ('iso3', 'currency'),
        [('RU', 'Rouble'), ('AU', 'Dollar')],
    )
    with pytest.raises(RuntimeError):
        list(datawelder.join._join_partitions([left, right]))


def test_calculate_indices():
    headers = [['iso', 'name'], ['iso', 'currency']]
    fields = [(0, 0, 'iso'), (0, 1, 'name'), (1, 0, 'iso_1'), (1, 1, 'currency')]
    expected = [0, 1, 2, 3]
    actual = datawelder.join._calculate_indices(headers, fields)
    assert actual == expected


def test_calculate_indices_jumbled():
    headers = [['iso', 'name'], ['iso', 'currency']]
    fields = [(0, 0, 'iso'), (1, 1, 'currency'), (0, 0, 'ISO'), (0, 1, 'name')]
    expected = [0, 3, 0, 1]
    actual = datawelder.join._calculate_indices(headers, fields)
    assert actual == expected


def test_fastforward_found():
    part = datawelder.partition.MemoryPartition(
        ('iso', 'name'),
        [('AU', 'Australia'), ('RU', 'Russia')]
    )
    nextrecord = datawelder.join._fastforward(part, ('AU', 'Australia'), 'RU')
    assert nextrecord is ('RU', 'Russia')


def test_fastforward_missing():
    part = datawelder.partition.MemoryPartition(
        ('iso', 'name'),
        [('AU', 'Australia'), ('RU', 'Russia')]
    )
    nextrecord = datawelder.join._fastforward(part, ('AU', 'Australia'), 'ZA')
    assert nextrecord is None


def test_fastforward_end_of_partition():
    part = datawelder.partition.MemoryPartition(
        ('iso', 'name'),
        [('RU', 'Russia')]
    )
    nextrecord = datawelder.join._fastforward(part, ('JP', 'Japan'), 'ZA')
    assert nextrecord is None
