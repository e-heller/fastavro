# -*- coding: utf-8 -*-
"""fastavro - faster reading and writing Avro files.

To read from an Avro file, a simple API is exposed: `fastavro.Reader`

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

To write to a file in Avro format, a simple API is exposed: `fastavro.write`

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

# Some of the code in `fastavro` is derived from the Apache Avro python source:
#    https://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/
#    Copyright 2010-2015 The Apache Software Foundation
#    Licensed under the Apache License, Version 2.0:
#       https://www.apache.org/licenses/LICENSE-2.0
# Please refer to NOTICE.txt
# Apache Avro, Avro, Apache are trademarks of The Apache Software Foundation.


# `fastavro` is licensed under the MIT License. See LICENSE.txt


from __future__ import absolute_import


from . import schema as _schema

try:
    from . import c_reader as _reader
    from . import c_writer as _writer

    Reader = _reader.PyReader
    load = _reader.py_read_data
    writer = _writer.py_writer
    dump = _writer.py_write_data

except ImportError:
    from . import reader as _reader
    from . import writer as _writer

    Reader = _reader.Reader
    load = _reader.read_data
    writer = _writer.writer
    dump = _writer.write_data


__title__ = 'fastavro'
__summary__ = 'Faster reading and writing Apache Avro files.'
__uri__ = 'https://github.com/e-heller/fastavro'
__author__ = 'Eric Heller, Miki Tebeka, and contributors'
__license__ = 'MIT'
__copyright__ = 'Copyright %s' % __author__

__version_info__ = (0, 10, 1)
__version__ = '.'.join(map(str, __version_info__))


def acquaint_schema(schema):
    """Add a new schema to the schema repo.

    Parameters
    ----------
    schema: dict
        Schema to add to repo
    """
    schema = _schema.normalize_schema(schema)
    _reader.acquaint_schema(schema)
    _writer.acquaint_schema(schema)


def reset_schema_repository():
    """Reset schema repositories to their original state.
    This can be useful if the repository has become polluted after reading or
    writing Avro files which use the same 'Names' in different Schemas.
    """
    _reader.reset_reader_funcs()
    _reader.reset_schema_defs()
    _writer.reset_writer_funcs()
    _writer.reset_schema_defs()


_schema.acquaint_schema = acquaint_schema

normalize_schema = _schema.normalize_schema

reader = iter_avro = Reader
schemaless_reader = _reader.schemaless_reader

write = writer
schemaless_writer = _writer.schemaless_writer

# Exceptions
SchemaError = _schema.SchemaError
SchemaAttributeError = _schema.SchemaAttributeError
UnknownTypeError = UnknownType = _schema.UnknownTypeError
InvalidTypeError = _schema.InvalidTypeError
SchemaResolutionError = _reader.SchemaResolutionError
ReadError = _reader.ReadError

# Some useful constants
PRIMITIVE_TYPES = _schema.PRIMITIVE_TYPES
COMPLEX_TYPES = _schema.COMPLEX_TYPES
AVRO_TYPES = _schema.AVRO_TYPES
SYNC_INTERVAL = _schema.SYNC_INTERVAL


__all__ = [_n for _n in locals().keys() if not _n.startswith('_')] + [
    '__version__', '__version_info__', '__title__', '__summary__', '__uri__',
    '__author__', '__license__', '__copyright__',
]
