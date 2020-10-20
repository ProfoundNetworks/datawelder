import datawelder.partition


def test_calculate_key_str():
    key = datawelder.partition.calculate_key('hello world', 1000)
    assert key == 291

    assert datawelder.partition.calculate_key('AU', 5) == 3
    assert datawelder.partition.calculate_key('JP', 5) == 4
    assert datawelder.partition.calculate_key('RU', 5) == 0


def test_calculate_key_int():
    key = datawelder.partition.calculate_key(123, 1000)
    assert key == 808


def test_memory_frame():
    fieldnames = ('iso', 'name')
    data = [('AU', 'Australia'), ('JP', 'Japan'), ('RU', 'Russia')]
    numpartitions = 5
    frame = datawelder.partition.MemoryFrame(fieldnames, data, numpartitions)

    assert len(frame) == numpartitions
    assert list(frame[3]) == [('AU', 'Australia')]
    assert list(frame[4]) == [('JP', 'Japan')]
    assert list(frame[0]) == [('RU', 'Russia')]


def test_memory_partition_iteration():
    expected = [('AU', 'Australia'), ('RU', 'Russia')]
    part = datawelder.partition.MemoryPartition(('iso3', 'name'), expected)
    actual = list(part)
    assert actual == expected
