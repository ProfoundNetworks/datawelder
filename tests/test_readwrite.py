import io

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
