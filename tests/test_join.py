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
