import pytest

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


class Partition:
    def __init__(self, field_names, data):
        self.path = '/does/not/exist'
        self.field_names = field_names
        self.key_index = 0
        self._data = data
        self._iter = iter(self._data)

    def __iter__(self):
        return self._iter

    def __next__(self):
        return next(self._iter)


def test_join_partitions():
    left = Partition(('iso3', 'name'), [('AU', 'Australia'), ('RU', 'Russia')])
    right = Partition(('iso', 'currency'), [('AU', 'Dollar'), ('RU', 'Rouble')])
    expected = [
        ('AU', 'Australia', 'AU', 'Dollar'),
        ('RU', 'Russia', 'RU', 'Rouble'),
    ]
    actual = list(datawelder.join._join_partitions([left, right]))
    assert actual == expected


def test_join_partitions_missing_left():
    left = Partition(
        ('iso', 'name'),
        [('AU', 'Australia'), ('KP', 'Kraplakistan'), ('RU', 'Russia')],
    )
    right = Partition(('iso3', 'currency'), [('AU', 'Dollar'), ('RU', 'Rouble')])
    expected = [
        ('AU', 'Australia', 'AU', 'Dollar'),
        ('KP', 'Kraplakistan', None, None),
        ('RU', 'Russia', 'RU', 'Rouble'),
    ]
    actual = list(datawelder.join._join_partitions([left, right]))
    assert actual == expected


def test_join_partitions_missing_right():
    left = Partition(
        ('iso', 'name'),
        [('AU', 'Australia'), ('RU', 'Russia')],
    )
    right = Partition(
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
    left = Partition(('iso', 'name'), [('RU', 'Russia'), ('AU', 'Australia')])
    right = Partition(('iso3', 'currency'), [('AU', 'Dollar'), ('RU', 'Rouble')])
    with pytest.raises(RuntimeError):
        list(datawelder.join._join_partitions([left, right]))


@pytest.mark.skip('not sure how to test this particular edge case')
def test_join_partitions_unsorted_right():
    left = Partition(('iso', 'name'), [('AU', 'Australia'), ('RU', 'Russia')])
    right = Partition(('iso3', 'currency'), [('RU', 'Rouble'), ('AU', 'Dollar')])
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
