import datawelder.partition


def test_calculate_key_str():
    key = datawelder.partition.calculate_key('hello world', 1000)
    assert key == 291


def test_calculate_key_int():
    key = datawelder.partition.calculate_key(123, 1000)
    assert key == 808
