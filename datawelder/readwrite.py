"""Implements functions for reading and writing from/to files."""
import csv
import functools
import json
import logging
import pickle

import smart_open  # type: ignore

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

_LOGGER = logging.getLogger(__name__)
ENCODING = 'utf-8'

CSV = 'csv'
JSON = 'json'
PICKLE = 'pickle'


def sniff_format(path: str) -> str:
    if '.csv' in path:
        return CSV
    if '.json' in path:
        return JSON
    if '.pickle' in path:
        return PICKLE
    assert False, 'uknown format: %r' % path


class AbstractReader:
    def __init__(
        self,
        path: str,
        key: Union[int, str] = 0,
        field_names: Optional[List[str]] = None,
        fmtparams: Optional[Dict[str, str]] = None,
    ) -> None:
        self.path = path
        self._key = key
        self.field_names = field_names
        self.fmtparams = fmtparams

        self.key_index: Optional[int] = None
        if isinstance(self._key, int):
            self.key_index = self._key
        elif isinstance(self._key, str) and self.field_names is not None:
            self.key_index = self.field_names.index(self._key)

    def __enter__(self):
        self._fin = smart_open.open(self.path, 'r')
        return self

    def __exit__(self, *exc):
        pass

    def __iter__(self):
        return self

    def __next__(self) -> Tuple:
        raise NotImplementedError


class CsvReader(AbstractReader):
    def __enter__(self):
        fmtparams = csv_fmtparams(self.fmtparams)
        self._fin = smart_open.open(self.path, 'r')
        self._reader = csv.reader(self._fin, **fmtparams)

        if not self.field_names:
            self.field_names = next(self._reader)
            if isinstance(self._key, str):
                self.key_index = self.field_names.index(self._key)

        _LOGGER.info('partition key: %r', self.field_names[self.key_index])
        return self

    def __next__(self):
        return tuple(next(self._reader))


class JsonReader(AbstractReader):
    def __enter__(self):
        #
        # Better to read in binary mode, because of unicode line ending weirdness.
        #
        self._fin = smart_open.open(self.path, 'rb')
        return self

    def __next__(self):
        line = next(self._fin)
        record_dict = json.loads(line)

        if not self.field_names:
            self.field_names = sorted(record_dict)
            if isinstance(self._key, str):
                self.key_index = self.field_names.index(self._key)

            _LOGGER.info('partition key: %r', self.field_names[self.key_index])

        #
        # NB We're potentially introducing null values here...
        #
        record_tuple = tuple([record_dict.get(f) for f in self.field_names])
        return record_tuple


def parse_fmtparams(params: List[str]) -> Dict[str, str]:
    if not params:
        return {}
    fmtparams: Dict[str, str] = {}
    for pair in params:
        key, value = pair.split('=', 1)
        fmtparams[key] = value
    return fmtparams


def csv_fmtparams(fmtparams: Dict[str, str]) -> Dict[str, Any]:
    #
    # https://docs.python.org/3/library/csv.html
    #
    types = {
        'delimiter': str,
        'doublequote': bool,
        'escapechar': str,
        'lineterminator': str,
        'quotechar': str,
        'quoting': int,
        'skipinitialspace': bool,
        'strict': bool,
    }
    scrubbed = {}
    for key, value in fmtparams.items():
        t = types[key]
        if t == bool:
            scrubbed[key] = value.lower() == 'true'
        else:
            scrubbed[key] = t(value)
    return scrubbed


class AbstractWriter:
    def __init__(
        self,
        path: str,
        partition_num: int,
        field_indices: List[int],
        field_names: List[str],
        fmtparams: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        :param path: Where to write to.
        :param partition_num: The number of the partition being written.
        :param field_indices: What fields to pick from the record when writing.
        :param field_names: How to name the picked fields.
        :param fmtparams: Options for the CSV writer.
        """
        assert len(field_indices) == len(field_names)

        self._path = path
        self._partition_num = partition_num
        self._field_indices = field_indices
        self._field_names = field_names

        if fmtparams:
            self._fmtparams = fmtparams
        else:
            self._fmtparams = {}

    def __enter__(self):
        self._fout = smart_open.open(self._path, 'wb')
        return self

    def __exit__(self, *exc):
        pass

    def write(self, record: List[Any]) -> None:
        raise NotImplementedError


class PickleWriter(AbstractWriter):
    """Simply dumps the record as an unnamed tuple (list) to pickle.

    Ignores most of the initializer parameters.
    """
    def write(self, record):
        pickle.dump(record, self._fout)


class JsonWriter(AbstractWriter):
    """Writes records as JSON, one record per line."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mapping = list(zip(self._field_indices, self._field_names))
        assert self._mapping, 'nothing to output'

    def write(self, record):
        record_dict = {
            fieldname: record[fieldindex]
            for fieldindex, fieldname in self._mapping
        }
        self._fout.write(json.dumps(record_dict).encode(ENCODING))
        self._fout.write(b'\n')


class CsvWriter(AbstractWriter):
    """Writes record as CSV."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._write_header = self._fmtparams.pop('write_header', 'true').lower() == 'true'

    def __enter__(self):
        fmtparams = csv_fmtparams(self._fmtparams)
        self._fout = smart_open.open(self._path, 'wt')
        self._writer = csv.writer(self._fout, **fmtparams)

        if self._write_header and self._partition_num == 0:
            self._writer.writerow(self._field_names)

        return self

    def write(self, record):
        row = [record[i] for i in self._field_indices]
        self._writer.writerow(row)


def open_reader(
    path: str,
    key: Union[int, str] = 0,
    field_names: Optional[List[str]] = None,
    fmt: Optional[str] = None,
    fmtparams: Optional[Dict[str, str]] = None,
) -> AbstractReader:
    if fmt is None:
        fmt = sniff_format(path)

    cls: Type[AbstractReader] = JsonReader
    if fmt == CSV:
        cls = CsvReader
    elif fmt == JSON:
        cls = JsonReader
    else:
        assert False

    return cls(path, key, field_names, fmtparams)


def partial_writer(fmt: str, fmtparams: Optional[Dict[str, str]] = None) -> Any:
    cls: Type[AbstractWriter] = PickleWriter
    if fmt == PICKLE:
        cls = PickleWriter
    elif fmt == JSON:
        cls = JsonWriter
    elif fmt == CSV:
        cls = CsvWriter
    else:
        assert False

    return functools.partial(cls, fmtparams=fmtparams)