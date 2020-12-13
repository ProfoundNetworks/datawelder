import boto3
import moto

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


@moto.mock_s3
def test_open_partitions():
    s3 = boto3.resource('s3', region_name='us-east-1')
    s3.Bucket('testbucket').create()
    with datawelder.partition.open_partitions('s3://testbucket/%d', 3, 'wt') as parts:
        print(0, file=parts[0])
        print(1, file=parts[1])
        print(2, file=parts[2])

    assert s3.Object('testbucket', '0').get()['Body'].read() == b'0\n'
    assert s3.Object('testbucket', '1').get()['Body'].read() == b'1\n'
    assert s3.Object('testbucket', '2').get()['Body'].read() == b'2\n'
