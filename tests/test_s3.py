import os

import boto3
import moto
import pytest

import datawelder.s3


@moto.mock_s3()
def test_writer():
    """Does the writer work with a mock S3 bucket?"""
    resource = boto3.resource('s3', region_name='us-east-1')
    bucket = resource.create_bucket(Bucket='mybucket')
    bucket.wait_until_exists()

    with datawelder.s3.LightweightWriter('mybucket', 'mykey') as fout:
        fout.write(b'hello world!')

    obj = resource.Object('mybucket', 'mykey')
    assert obj.get()['Body'].read() == b'hello world!'


@pytest.mark.skipif(
    os.environ.get('LOCALSTACK_ENDPOINT') is None,
    reason='set LOCALSTACK_ENDPOINT to e.g. http://localhost:4566 to enable this test',
)
def test_writer_localstack():
    """Does the writer work with a localstack S3 bucket?"""
    session = boto3.Session(region_name='us-east-1')

    endpoint_url = os.environ['LOCALSTACK_ENDPOINT']
    resource = session.resource('s3', endpoint_url=endpoint_url)
    bucket = resource.create_bucket(Bucket='mybucket')
    bucket.wait_until_exists()

    client = session.client('s3', endpoint_url=endpoint_url)
    with datawelder.s3.LightweightWriter('mybucket', 'mykey', client=client) as fout:
        fout.write(b'hello world!')

    obj = resource.Object('mybucket', 'mykey')
    assert obj.get()['Body'].read() == b'hello world!'
