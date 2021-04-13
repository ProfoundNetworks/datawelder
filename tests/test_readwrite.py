import csv
import io
import os
import tempfile

import boto3  # type: ignore
import mock
import pytest

import datawelder.readwrite


def test_read_csv():
    expected = [('AU', 'Australia'), ('JP', 'Japan'), ('RU', 'Russia')]
    buf = io.BytesIO(
        b'iso,name\n'
        b'AU,Australia\n'
        b'JP,Japan\n'
        b'RU,Russia\n'
    )
    with datawelder.readwrite.CsvReader(buf) as reader:
        actual = list(reader)

    assert reader.field_names == ['iso', 'name']
    assert actual == expected


def test_read_csv_jagged():
    expected = [('AU', 'Australia'), ('JP', 'Japan'), ('RU', 'Russia')]
    buf = io.BytesIO(
        b'iso,name\n'
        b'AU,Australia\n'
        b'JP,Japan\n'
        b'KP,Kraplakistan,whoops,not,a,country\n'
        b'RU,Russia\n'
        b'XX\n'
    )
    with datawelder.readwrite.CsvReader(buf) as reader:
        actual = list(reader)

    assert reader.field_names == ['iso', 'name']
    assert actual == expected


def test_read_csv_no_header():
    buf = io.BytesIO(b'AU,Australia')
    expected = [('AU', 'Australia')]
    fmtparams = {'header': 'none'}
    with datawelder.readwrite.CsvReader(buf, fmtparams=fmtparams) as reader:
        actual = list(reader)
    assert actual == expected
    assert reader.field_names == ['f0', 'f1']


def test_read_csv_drop_header():
    buf = io.BytesIO(b'iso,name\nAU,Australia')
    expected = [('AU', 'Australia')]
    fmtparams = {'header': 'drop'}
    with datawelder.readwrite.CsvReader(buf, fmtparams=fmtparams) as reader:
        actual = list(reader)
    assert actual == expected
    assert reader.field_names == ['f0', 'f1']


def test_read_csv_ignores_bad_fmtparams():
    buf = io.BytesIO(b'iso,name\nAU,Australia')
    expected = [('AU', 'Australia')]
    fmtparams = {'foo': 'bar'}
    with datawelder.readwrite.CsvReader(buf, fmtparams=fmtparams) as reader:
        actual = list(reader)
    assert actual == expected
    assert reader.field_names == ['iso', 'name']


def scrub(val):
    return str(val).replace(',', ' ')


def test_scrub_delimiter():
    fmtparams = {'quoting': csv.QUOTE_NONE, 'quotechar': ''}
    with tempfile.NamedTemporaryFile() as temp:
        with datawelder.readwrite.CsvWriter(
            temp.name,
            1,
            [0, 1, 2],
            ['f1', 'f2', 'f3'],
            fmtparams,
            scrubbers={0: scrub, 1: scrub, 2: scrub},
        ) as writer:
            writer.write(['hello', 123, 'world, how you doin?'])

        assert open(temp.name, 'rb').read() == b'hello,123,world  how you doin?\r\n'


def test_dump_and_load():
    buf = io.BytesIO()
    buf.close = lambda: None
    record = ['AU', 'Australia', 'Dollar', 'Canberra']

    datawelder.readwrite.dump(record, buf)

    expected = b'["AU", "Australia", "Dollar", "Canberra"]\n'
    actual = buf.getvalue()
    assert actual == expected

    buf.seek(0)
    actual_record = datawelder.readwrite.load(buf)
    assert actual_record == record


@pytest.mark.parametrize(
    ('fmtparams', 'expected'),
    [
        ({'doublequote': True}, {'doublequote': True}),
        ({'doublequote': 'true'}, {'doublequote': True}),
        ({'doublequote': 'false'}, {'doublequote': False}),
        ({'delimiter': '|'}, {'delimiter': '|'}),
    ]
)
def test_csv_params(fmtparams, expected):
    assert datawelder.readwrite.csv_fmtparams(fmtparams) == expected


@pytest.mark.parametrize(
    ('kwargs', 'expected'),
    [
        ({}, 's3(http://localhost:1234)'),
        ({'transport_params': None}, 's3(http://localhost:1234)'),
        ({'transport_params': {}}, 's3(http://localhost:1234)'),
        (
            {
                'transport_params': {
                    'client': boto3.client('s3', endpoint_url='http://localhost:4567'),
                }
            },
            's3(http://localhost:4567)'
        ),
    ]
)
def test_inject_parameters(kwargs, expected):
    datawelder.readwrite._inject_parameters('http://localhost:1234', kwargs)
    assert kwargs['transport_params']['client']
    assert str(kwargs['transport_params']['client']._endpoint) == expected


@mock.patch('smart_open.open')
def test_open_endpoint_url(mock_open):
    try:
        os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:1234'
        datawelder.readwrite.open('s3://mybucket/key')
        args, kwargs = mock_open.call_args_list[0]
        client = kwargs['transport_params']['client']

        assert args == ('s3://mybucket/key', )
        assert str(client._endpoint) == 's3(http://localhost:1234)'
    finally:
        del os.environ['AWS_ENDPOINT_URL']
