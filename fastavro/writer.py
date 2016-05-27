# -*- coding: utf-8 -*-
"""Python implementation for writing Apache Avro files"""


# This code is based on the Apache 'avro' pacakge, found at:
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/
# Under the Apache 2.0 License (https://www.apache.org/licenses/LICENSE-2.0)

# Please refer to the Avro specification page for details on data encoding:
#   https://avro.apache.org/docs/current/spec.html


from __future__ import absolute_import

from binascii import crc32
from collections import Iterable, Mapping
from os import urandom
from struct import pack
from zlib import compress

try:
    import simplejson as json
except ImportError:
    import json

try:
    import snappy
except ImportError:
    snappy = None

from fastavro.compat import (
    BytesIO, iteritems, _int_types, _number_types, _unicode_type,
    _bytes_type, _string_types,
)
from fastavro.schema import (
    extract_named_schemas_into_repo, HEADER_SCHEMA, MAGIC, SYNC_SIZE,
    SYNC_INTERVAL, PRIMITIVE_TYPES,
)


INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1


class _NoValue(object):
    def __repr__(self):
        return '<NoValue>'

    def __bool__(self):
        raise TypeError('%r has no bool value' % self)

    # Python 2 uses '__nonzero__' rather than '__bool__'
    __nonzero__ = __bool__

NoValue = _NoValue()


# ---- Writing Avro primitives -----------------------------------------------#

def write_null(stream, datum, schema=None):
    """A `null` value is not written at all."""
    pass


def write_boolean(stream, datum, schema=None):
    """A `boolean` value is written as a single byte: b'0x00' for False,
    b'0x01' for True.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    stream.write(b'\x01' if datum else b'\x00')


def write_long(stream, datum, schema=None):
    """An `int` or `long` value is written as 32-bit or 64-bit integer,
    respectively, using a variable-length zig-zag encoding. This is the same
    encoding used in Google protobufs.

    A good explanation of this encoding can be found at:
        https://developers.google.com/protocol-buffers/docs/encoding#signed-integers

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    datum = (datum << 1) ^ (datum >> 63)
    while (datum & ~0x7F) != 0:
        stream.write(pack('B', (datum & 0x7f) | 0x80))
        datum >>= 7
    stream.write(pack('B', datum))


# Alias `write_int` to `write_long`
write_int = write_long


def write_float(stream, datum, schema=None):
    """A `float` value is written as a single precision 32-bit IEEE 754
    floating-point value in little-endian format.

    See the implementation of `_PyFloat_Pack4()` in Python's floatobject.c

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    stream.write(pack('<f', datum))


def write_double(stream, datum, schema=None):
    """A `double` value is written as a double precision 64-bit IEEE 754
    floating-point value in little-endian format.

    See the implementation of `_PyFloat_Pack8()` in Python's floatobject.c

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    stream.write(pack('<d', datum))


def write_bytes(stream, datum, schema=None):
    """A `bytes` value is written as a `long` (length of the byte string),
    immediately followed by the raw byte data.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    write_long(stream, len(datum))
    stream.write(datum)


def write_string(stream, datum, schema=None):
    """A `string` value is written as a `long` (length of the UTF-8 encoded
    string), immediately followed by the UTF-8 encoded byte data.

    Note: Avro `string` values *must* be encoded to UTF-8 byte strings.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    byte_str = (
        datum.encode('utf-8') if isinstance(datum, _unicode_type) else datum
    )
    write_long(stream, len(byte_str))
    stream.write(byte_str)


# ---- Writing Avro complex types --------------------------------------------#

def write_fixed(stream, datum, schema=None):
    """A `fixed` value is written as raw bytes. The length of the byte data
    is declared in the schema.

    Note: we do not verify that the length of `datum` matches the length
    declared in the `schema` (although perhaps we should). For now, this
    responsibility is left to the user.

    Reference: https://avro.apache.org/docs/current/spec.html#Fixed
    """
    stream.write(datum)


def write_enum(stream, datum, schema):
    """An `enum` value is written as an `int` representing the zero-based
    position of the symbol in the schema.

    Note: calling `.index(datum)` repeatedly is a bit inefficient. Instead, we
    could precompute the index-offsets for each symbol and store it in a dict.

    Reference: https://avro.apache.org/docs/current/spec.html#Enums
    """
    index = schema['symbols'].index(datum)
    write_int(stream, index)


def write_array(stream, datum, schema):
    """An `array` value is written as a series of blocks.

    Each block consists of a `long` `count` value, followed by that many array
    items. A block with `count` zero indicates the end of the array. Each item
    is encoded per the array's item schema.

    If a block's `count` is negative, then the `count` is followed immediately
    by a `long` block size, indicating the number of bytes in the block. The
    actual `count` in this case is the absolute value of the `count` written.

    Note: this implementation does not support writing negative block counts.

    Reference: https://avro.apache.org/docs/current/spec.html#Arrays
    """
    datum_len = len(datum)
    if datum_len:
        write_long(stream, datum_len)
        dtype = schema['items']
        for item in datum:
            write_data(stream, item, dtype)
    write_long(stream, 0)


def write_map(stream, datum, schema):
    """A `map` value is written as a series of blocks.

    Each block consists of a `long` `count` value, followed by that many
    key / value pairs. A block with `count` zero indicates the end of the map.

    If a block's `count` is negative, then the `count` is followed immediately
    by a `long` block size, indicating the number of bytes in the block. The
    actual `count` in this case is the absolute value of the `count` written.

    For writing the key / value pairs:
        A map `key` is assumed to be string
        A map `value` is written per the map's 'value' schema

    Note: this implementation does not support writing negative block counts.

    Reference: https://avro.apache.org/docs/current/spec.html#Maps
    """
    datum_len = len(datum)
    if datum_len:
        write_long(stream, datum_len)
        vtype = schema['values']
        for key, val in iteritems(datum):
            write_string(stream, key)
            write_data(stream, val, vtype)
    write_long(stream, 0)


def write_union(stream, datum, schema):
    """A `union` value is written as a `long` indicating the zero-based
    position of the `value` in the union's schema, immediately followed by
    the `value`, written per the indicated schema within the union.

    Reference: https://avro.apache.org/docs/current/spec.html#Unions
    """
    for index, candidate in enumerate(schema):
        if validate(datum, candidate):
            break
    else:
        raise ValueError(
            '%r (type %s) does not match the schema: %s'
            % (datum, type(datum), schema)
        )
    write_long(stream, index)
    write_data(stream, datum, schema[index])


def write_record(stream, datum, schema):
    """A `record` value is written by encoding the values of its fields in the
    order in which they are declared. In other words, a `record` is written
    as just the concatenation of the encodings of its fields. Field values
    are encoded per their schema.

    Reference: https://avro.apache.org/docs/current/spec.html#schema_record
    """
    for field in schema['fields']:
        value = datum.get(field['name'], field.get('default', NoValue))
        write_data(stream, value, field['type'])


# ---- Validation of data with Schema ----------------------------------------#

def validate(datum, schema):
    """
    Determine if a python datum is an instance of a schema.

    This function is only called by `write_union`. Unfortunately, this function
    creates significant overhead for writing `union` types. Any ideas to
    improve this function would be especially welcomed.
    """

    if isinstance(schema, dict):
        record_type = schema['type']
    elif isinstance(schema, list):
        record_type = 'union'
    else:
        record_type = schema

    if record_type == 'null':
        return datum is None

    elif record_type == 'boolean':
        return isinstance(datum, bool)

    elif record_type == 'string':
        return isinstance(datum, _string_types)

    elif record_type == 'bytes':
        return isinstance(datum, _bytes_type)

    elif record_type == 'int':
        return (
            isinstance(datum, _int_types) and
            INT_MIN_VALUE <= datum <= INT_MAX_VALUE
        )

    elif record_type == 'long':
        return (
            isinstance(datum, _int_types) and
            LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
        )

    elif record_type in ('float', 'double',):
        return isinstance(datum, _number_types)

    elif record_type == 'fixed':
        return isinstance(datum, bytes) and len(datum) == schema['size']

    elif record_type in ('union', 'error_union',):
        return any(validate(datum, s) for s in schema)

    # dict-y types from here on.
    elif record_type == 'enum':
        return datum in schema['symbols']

    elif record_type == 'array':
        return (
            isinstance(datum, Iterable) and
            all(validate(d, schema['items']) for d in datum)
        )

    elif record_type == 'map':
        return (
            isinstance(datum, Mapping) and
            all(isinstance(k, _string_types) for k in datum.keys()) and
            all(validate(v, schema['values']) for v in datum.values())
        )

    elif record_type in ('record', 'error'):
        return (
            isinstance(datum, Mapping) and
            all(
                validate(datum.get(f['name'], f.get('default', NoValue)),
                         f['type'])
                for f in schema['fields']
            )
        )

    else:
        record_type = SCHEMA_DEFS.get(record_type)
        if record_type:
            return validate(datum, record_type)

    raise ValueError('Unknown record type: %s' % record_type)


# ---- Writer function lookup ------------------------------------------------#

WRITERS = {
    # Primitive types
    'null': write_null,
    'boolean': write_boolean,
    'int': write_int,
    'long': write_long,
    'float': write_float,
    'double': write_double,
    'bytes': write_bytes,
    'string': write_string,

    # Complex types
    'fixed': write_fixed,
    'enum': write_enum,
    'array': write_array,
    'map': write_map,
    'union': write_union,
    'record': write_record,
    'error': write_record,
    'error_union': write_union,
}


def write_data(stream, datum, schema):
    """Write a `datum` of data to the output `stream` using the specified
    Avro `schema`.

    Paramaters
    ----------
    stream: file-like object
        Output file or stream
    datum: object
        Data to write
    schema: dict
        Avro Schema for `datum`
    """
    if isinstance(schema, dict):
        record_type = schema['type']
    elif isinstance(schema, list):
        record_type = 'union'
    else:
        record_type = schema
    WRITERS[record_type](stream, datum, schema)


# ---- Block Encoders --------------------------------------------------------#

def null_write_block(stream, block_bytes):
    """Write a block of bytes with no codec ('null' codec)."""
    write_long(stream, len(block_bytes))
    stream.write(block_bytes)


def deflate_write_block(stream, block_bytes):
    """Write a block of bytes with the 'deflate' codec."""
    # The first two and last characters are zlib wrappers around deflate data
    data = compress(block_bytes)[2:-1]
    write_long(stream, len(data))
    stream.write(data)


def snappy_write_block(stream, block_bytes):
    """Write a block of bytes wih the 'snappy' codec."""
    data = snappy.compress(block_bytes)
    # Add 4 bytes for the CRC32
    write_long(stream, len(data) + 4)
    stream.write(data)
    # Write the 4-byte, big-endian CRC32 checksum
    crc = crc32(block_bytes) & 0xFFFFFFFF
    stream.write(pack('>I', crc))


# ---- Schema Handling -------------------------------------------------------#

SCHEMA_DEFS = dict((typ, typ) for typ in PRIMITIVE_TYPES)


def get_schema_defs():
    """Return the registered schema definitions."""
    return SCHEMA_DEFS


def acquaint_schema(schema, repo=None):
    """Extract `schema` into `repo` (default WRITERS)"""
    repo = WRITERS if repo is None else repo
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: (
            lambda stream, datum, _: write_data(stream, datum, schema)
        ),
    )
    extract_named_schemas_into_repo(
        schema,
        SCHEMA_DEFS,
        lambda schema: schema,
    )


def write_header(stream, metadata, sync_marker):
    """Write the Avro header"""
    # Note: values in the `meta` dict are written as bytes.
    # See the definition of HEADER_SCHEMA in schema.py
    header = {
        'magic': MAGIC,
        'meta': dict(
            (k, v.encode('utf-8') if isinstance(v, _unicode_type) else v)
            for k, v in iteritems(metadata)
        ),
        'sync': sync_marker,
    }
    write_data(stream, header, HEADER_SCHEMA)


# ---- Public API - Writing Avro Files ---------------------------------------#

def writer(
    stream,
    schema,
    records,
    codec='null',
    sync_interval=SYNC_INTERVAL,
    metadata=None
):
    """Write multiple `records` to the output `stream` using the specified
    Avro `schema`.

    Paramaters
    ----------
    stream: file-like object
        Output file or stream
    schema: dict
        An Avro schema, typically a dict
    records: iterable
        Records to write
    codec: string, optional
        Compression codec, can be 'null', 'deflate' or 'snappy' (if installed)
    sync_interval: int, optional
        Size of sync interval. Defaults to fastavro.SYNC_INTERVAL
    metadata: dict, optional
        Additional header metadata

    Example
    -------
    >>> import fastavro
    >>>
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
    >>> records = [
    >>>     {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    >>>     {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    >>>     {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    >>>     {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
    >>> ]
    >>>
    >>> with open('weather.avro', 'wb') as out:
    >>>     fastavro.write(out, schema, records)
    """
    # Default values
    codec = codec or 'null'
    sync_interval = sync_interval or SYNC_INTERVAL
    metadata = metadata or {}

    # Get block writer specified by `codec`
    if codec == 'null':
        block_writer = null_write_block
    elif codec == 'deflate':
        block_writer = deflate_write_block
    elif codec == 'snappy':
        if not snappy:
            raise ValueError(
                "Cannot write 'snappy' codec: 'snappy' module is not available"
            )
        block_writer = snappy_write_block
    else:
        raise ValueError('Unknown codec: %r' % codec)

    # Write Avro header
    sync_marker = urandom(SYNC_SIZE)
    metadata['avro.codec'] = codec
    metadata['avro.schema'] = json.dumps(schema)
    write_header(stream, metadata, sync_marker)

    # Register the schema
    acquaint_schema(schema)

    buf = BytesIO()
    block_count = 0

    for record in records:
        write_data(buf, record, schema)
        block_count += 1
        if buf.tell() >= sync_interval:
            write_long(stream, block_count)
            block_writer(stream, buf.getvalue())
            stream.write(sync_marker)
            buf.truncate(0)
            buf.seek(0)
            block_count = 0

    if buf.tell() or block_count > 0:
        write_long(stream, block_count)
        block_writer(stream, buf.getvalue())
        stream.write(sync_marker)

    stream.flush()
    buf.close()


def schemaless_writer(stream, schema, record):
    """Write a single `record` to the output `stream` without writing the Avro
    schema and header information.

    Paramaters
    ----------
    stream: file-like object
        Output file or stream
    schema: dict
        Schema for `record`. (This is not written to the output stream,
        however it is required for writing the `record`.)
    record: dict
        Record to write
    """
    acquaint_schema(schema)
    write_data(stream, record, schema)
