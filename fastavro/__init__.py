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


__version__ = '0.9.10'


try:
    from . import _reader
    from . import _writer
    from . import _schema
except ImportError:
    from . import reader as _reader
    from . import writer as _writer
    from . import schema as _schema


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

# Reader API
Reader = reader = iter_avro = _reader.Reader
schemaless_reader = _reader.schemaless_reader
load = _reader.read_data

# Writer API
write = writer = _writer.writer
schemaless_writer = _writer.schemaless_writer
dump = _writer.write_data

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
    n for n in locals().keys() if not n.startswith('_')
] + ['__version__']
