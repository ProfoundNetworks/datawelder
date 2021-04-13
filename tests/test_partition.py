import json
import os
import tempfile

import unittest.mock as mock

import boto3  # type: ignore
import moto  # type: ignore
import pytest
import smart_open  # type: ignore

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
    with datawelder.partition.open_partitions('s3://testbucket/%d', 3, 'wb') as parts:
        parts[0].write(b'0\n')
        parts[1].write(b'1\n')
        parts[2].write(b'2\n')

    assert s3.Object('testbucket', '0').get()['Body'].read() == b'0\n'
    assert s3.Object('testbucket', '1').get()['Body'].read() == b'1\n'
    assert s3.Object('testbucket', '2').get()['Body'].read() == b'2\n'


@pytest.mark.skipif(
    not os.environ.get('AWS_ENDPOINT_URL'),
    reason='this test expects a working localstack',
)
def test_open_partitions_localstack():
    endpoint_url = os.environ.get('AWS_ENDPOINT_URL')
    s3 = boto3.resource('s3', region_name='us-east-1', endpoint_url=endpoint_url)

    with datawelder.partition.open_partitions('s3://mybucket/%d', 3, 'wt') as parts:
        print(0, file=parts[0])
        print(1, file=parts[1])
        print(2, file=parts[2])

    assert s3.Object('mybucket', '0').get()['Body'].read() == b'0\n'
    assert s3.Object('mybucket', '1').get()['Body'].read() == b'1\n'
    assert s3.Object('mybucket', '2').get()['Body'].read() == b'2\n'


def test_partition():
    curr_dir = os.path.dirname(__file__)
    data_path = os.path.join(curr_dir, '../sampledata/names.csv')
    callback = mock.Mock()
    with datawelder.readwrite.open_reader(data_path, 'iso3') as reader:
        with tempfile.TemporaryDirectory() as tmpdir:
            datawelder.partition.partition(reader, tmpdir, 5, callback=callback, modulo=50)

            frame = datawelder.partition.PartitionedFrame(tmpdir)
            partition = frame[0]
            records = list(partition)
            assert records == sorted(records)

    assert callback.call_count == 5
    assert callback.call_args_list == [mock.call(x) for x in (50, 100, 150, 200, 250)]


def test_partition_without_sort():
    curr_dir = os.path.dirname(__file__)
    data_path = os.path.join(curr_dir, '../sampledata/names.csv')
    with datawelder.readwrite.open_reader(data_path, 'iso3') as reader:
        with tempfile.TemporaryDirectory() as tmpdir:
            datawelder.partition.partition(reader, tmpdir, 5, sort_partitions=False)

            frame = datawelder.partition.PartitionedFrame(tmpdir)
            partition = frame[0]
            records = list(partition)
            assert records != sorted(records)


def test_sort_partition_overwrite():
    curr_dir = os.path.dirname(__file__)
    data_path = os.path.join(curr_dir, '../sampledata/names.csv')
    with datawelder.readwrite.open_reader(data_path, 'iso3') as reader:
        with tempfile.TemporaryDirectory() as tmpdir:
            datawelder.partition.partition(reader, tmpdir, 5, sort_partitions=False)
            frame = datawelder.partition.PartitionedFrame(tmpdir)
            datawelder.partition.sort_partition(frame[0].path, key_index=frame.key_index)

            frame = datawelder.partition.PartitionedFrame(tmpdir)
            partition = frame[0]
            records = list(partition)
            assert records == sorted(records)


def test_sort_partition_no_overwrite():
    curr_dir = os.path.dirname(__file__)
    data_path = os.path.join(curr_dir, '../sampledata/names.csv')
    with datawelder.readwrite.open_reader(data_path, 'iso3') as reader:
        with tempfile.TemporaryDirectory() as tmpdir:
            datawelder.partition.partition(reader, tmpdir, 5, sort_partitions=False)
            frame = datawelder.partition.PartitionedFrame(tmpdir)
            sorted_path = frame[0].path.replace('0000.json.gz', '0000.sorted.json.gz')

            datawelder.partition.sort_partition(
                frame[0].path,
                key_index=frame.key_index,
                output_path=sorted_path,
            )

            with smart_open.open(sorted_path) as fin:
                records = [json.loads(line) for line in fin]

            assert records == sorted(records)
