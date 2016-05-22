# -*- coding: utf-8 -*-
"""Fast Avro - faster reading and writing Avro files.

To read an Avro file, a simple API is exposed: `fastavro.Reader`

Example usage:
>>> import fastavro
>>>
>>> with open('some_file.avro', 'rb') as input:
>>>     # Create a `Reader` object
>>>     reader = fastavro.Reader(input)
>>>
>>>     # Obtain the writer's schema if required
>>>     schema = reader.schema
>>>
>>>     # Iteratively read the records:
>>>     for record in reader:
>>>         process_record(record)
>>>
>>>     # Read the records in one shot:
>>>     records = list(reader)

To write an Avro file, a simple API is exposed: `fastavro.write`

Example usage:
>>> import fastavro
>>>
>>> # Define an Avro Schema as a dict:
>>> schema = {
>>>     'doc': 'A weather reading.',
>>>     'name': 'Weather',
>>>     'namespace': 'test',
>>>     'type': 'record',
>>>     'fields': [
>>>         {'name': 'station', 'type': 'string'},
>>>         {'name': 'time', 'type': 'long'},
>>>         {'name': 'temp', 'type': 'int'},
>>>     ],
>>> }
>>>
>>> # Create some records:
>>> records = [
>>>     {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
>>>     {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
>>>     {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
>>>     {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
>>> ]
>>>
>>> # Open a file, and write the records:
>>> with open('weather.avro', 'wb') as out:
>>>     fastavro.write(out, schema, records)
"""

from __future__ import absolute_import


__version_info__ = (0, 10, 0)

__version__ = '.'.join(map(str, __version_info__))


try:
    from . import c_reader as _reader
    from . import c_writer as _writer
    from . import c_schema as _schema

    Reader = _reader.PyReader
    load = _reader.py_read_data

    writer = _writer.py_writer
    dump = _writer.py_write_data

except ImportError:
    from . import reader as _reader
    from . import writer as _writer
    from . import schema as _schema

    Reader = _reader.Reader
    load = _reader.read_data

    writer = _writer.writer
    dump = _writer.write_data


def _acquaint_schema(schema):
    """Add a new schema to the schema repo.

    Parameters
    ----------
    schema: dict
        Schema to add to repo
    """
    _reader.acquaint_schema(schema)
    _writer.acquaint_schema(schema)


acquaint_schema = _schema.acquaint_schema = _acquaint_schema


reader = iter_avro = Reader
schemaless_reader = _reader.schemaless_reader

write = writer
schemaless_writer = _writer.schemaless_writer

# Exceptions
UnknownType = _schema.UnknownType
SchemaError = _schema.SchemaError
SchemaResolutionError = _reader.SchemaResolutionError
ReadError = _reader.ReadError

# Some useful constants
PRIMITIVE_TYPES = _schema.PRIMITIVE_TYPES
COMPLEX_TYPES = _schema.COMPLEX_TYPES
AVRO_TYPES = _schema.AVRO_TYPES
SYNC_INTERVAL = _schema.SYNC_INTERVAL


__all__ = [
    _n for _n in locals().keys() if not _n.startswith('_')
] + ['__version__', '__version_info__']
