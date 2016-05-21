# -*- coding: utf-8 -*-
# cython: c_string_type=bytes, wraparound=False
# cython: optimize.use_switch=True, always_allow_keywords=False
"""Cython implementation for writing Apache Avro files"""


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

from libc.stdint cimport int32_t, uint32_t, int64_t, uint64_t
from libc.string cimport memcpy

from . c_utils import INT32_MIN, INT32_MAX, INT64_MIN, INT64_MAX
from . c_utils cimport (
    Endianness, little, big, unknown, error, get_float_format,
    get_double_format,
)
from . c_schema import (
    HEADER_SCHEMA, MAGIC, SYNC_SIZE, SYNC_INTERVAL, PRIMITIVE_TYPES,
)
from . c_schema cimport extract_named_schemas_into_repo
from . c_buffer cimport Stream, StreamWrapper, ByteBuffer, SSize_t, uchar

include 'c_compat.pxi'


cdef Endianness double_format = get_double_format()
cdef Endianness float_format = get_float_format()


# ---- Writing Avro primitives -----------------------------------------------#

cdef inline int write_null(Stream fo, datum, schema) except -1:
    """A `null` value is not written at all."""
    pass


cdef inline int write_boolean(Stream fo, datum, schema) except -1:
    """A `boolean` value is written as a single byte: b'0x00' for False,
    b'0x01' for True.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    cdef uchar buf[1]
    buf[0] = 1 if datum else 0
    fo.write_chars(buf, 1)


cdef inline int write_int(Stream fo, int32_t datum, schema) except -1:
    """An `int` value is written as 32-bit integer, using a variable-length
    zig-zag encoding. This is the same encoding used in Google protobufs.

    A good explanation of this encoding can be found at:
        https://developers.google.com/protocol-buffers/docs/encoding#signed-integers

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    cdef uint32_t d = (datum << 1) ^ (datum >> 31)
    cdef uint32_t mask = ~0x7F
    # The longest possible encoding for an `int` is 5 bytes
    cdef uchar buf[5]
    cdef int ct = 0
    while d & mask:
        buf[ct] = <uchar>((d & 0x7F) | 0x80)
        ct += 1
        d >>= 7
    buf[ct] = <uchar>d
    fo.write_chars(buf, ct+1)


cdef inline int write_long(Stream fo, int64_t datum, schema) except -1:
    """A `long` value is written as 64-bit integer, using a variable-length
    zig-zag encoding. This is the same encoding used in Google protobufs.

    A good explanation of this encoding can be found at:
        https://developers.google.com/protocol-buffers/docs/encoding#signed-integers

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    cdef uint64_t d = (datum << 1) ^ (datum >> 63)
    cdef uint64_t mask = ~0x7F
    # The longest possible encoding for a `long` is 10 bytes
    cdef uchar buf[10]
    cdef int ct = 0
    while d & mask:
        buf[ct] = <uchar>((d & 0x7F) | 0x80)
        ct += 1
        d >>= 7
    buf[ct] = <uchar>d
    fo.write_chars(buf, ct+1)


cdef inline int write_float(Stream fo, float datum, schema) except -1:
    """A `float` value is written as a single precision 32-bit IEEE 754
    floating-point value in little-endian format.

    This is inspired by the implementation of `_PyFloat_Pack4()` in Python's
    floatobject.c

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    cdef uchar *d_cp = <uchar*>&datum
    cdef uchar buf[4]
    cdef int i = 0
    if float_format == little:
        fo.write_chars(d_cp, 4)
    elif float_format == big:
        for i in range(4):
            buf[i] = d_cp[3-i]
        fo.write_chars(buf, 4)
    else:
        # For an unknown float format, fall back to using struct.pack
        fo.write(pack('<f', datum))


cdef inline int write_double(Stream fo, double datum, schema) except -1:
    """A `double` value is written as a double precision 64-bit IEEE 754
    floating-point value in little-endian format.

    This is inspired by the implementation of `_PyFloat_Pack8()` in Python's
    floatobject.c

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    cdef uchar *d_cp = <uchar*>&datum
    cdef uchar buf[8]
    cdef int i = 0
    if double_format == little:
        fo.write_chars(d_cp, 8)
    elif double_format == big:
        for i in range(8):
            buf[i] = d_cp[7-i]
        fo.write_chars(buf, 8)
    else:
        # For an unknown double format, fall back to using struct.pack
        fo.write(pack('<d', datum))


cdef inline int write_bytes(Stream fo, bytes datum, schema) except -1:
    """A `bytes` value is written as a `long` (length of the byte string),
    immediately followed by the raw byte data.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    cdef int64_t datum_len = len(datum)
    write_long(fo, datum_len, None)
    fo.write(datum)


cdef inline int write_string(Stream fo, unicode datum, schema) except -1:
    """A `string` value is written as a `long` (length of the UTF-8 encoded
    string), immediately followed by the UTF-8 encoded byte data.

    Note: Avro `string` values *must* be encoded to UTF-8 byte strings.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    cdef bytes byte_str = datum.encode('utf-8')
    cdef int64_t datum_len = len(byte_str)
    write_long(fo, datum_len, None)
    fo.write(byte_str)


# ---- Writing Avro complex types --------------------------------------------#

cdef inline int write_fixed(Stream fo, bytes datum, schema) except -1:
    """A `fixed` value is written as raw bytes. The length of the byte data
    is declared in the schema.

    Note: we do not verify that the length of `datum` matches the length
    declared in the `schema` (although perhaps we should). For now, this
    responsibility is left to the user.

    Reference: https://avro.apache.org/docs/current/spec.html#Fixed
    """
    fo.write(datum)


cdef inline int write_enum(Stream fo, datum, dict schema) except -1:
    """An `enum` value is written as an `int` representing the zero-based
    position of the symbol in the schema.

    Note: calling `.index(datum)` repeatedly is a bit inefficient. Instead, we
    could precompute the index-offsets for each symbol and store it in a dict.

    Reference: https://avro.apache.org/docs/current/spec.html#Enums
    """
    cdef int32_t index = schema['symbols'].index(datum)
    write_int(fo, index, None)


cdef inline int write_array(Stream fo, list datum, dict schema) except -1:
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
    cdef int64_t datum_len = len(datum)
    if datum_len:
        write_long(fo, datum_len, None)
        dtype = schema['items']
        for item in datum:
            write_data(fo, item, dtype)
    write_long(fo, 0, None)


cdef inline int write_map(Stream fo, dict datum, dict schema) except -1:
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
    cdef int64_t datum_len = len(datum)
    if datum_len:
        write_long(fo, datum_len, None)
        vtype = schema['values']
        for key, val in iteritems(datum):
            if isinstance(key, unicode):
                write_string(fo, key, None)
            else:
                write_bytes(fo, key, None)
            write_data(fo, val, vtype)
    write_long(fo, 0, None)


cdef inline int write_union(Stream fo, datum, list schema) except -1:
    """A `union` value is written as a `long` indicating the zero-based
    position of the `value` in the union's schema, immediately followed by
    the `value`, written per the indicated schema within the union.

    Reference: https://avro.apache.org/docs/current/spec.html#Unions
    """
    cdef int64_t index
    cdef int64_t schema_len = len(schema)
    for index in xrange(schema_len):
        if validate(datum, schema[index]):
            break
    else:
        raise ValueError(
            '%r (type %s) does not match the schema: %s'
            % (datum, type(datum), schema)
        )
    write_long(fo, index, None)
    write_data(fo, datum, schema[index])


cdef inline int write_record(Stream fo, dict datum, dict schema) except -1:
    """A `record` value is written by encoding the values of its fields in the
    order in which they are declared. In other words, a `record` is written
    as just the concatenation of the encodings of its fields. Field values
    are encoded per their schema.

    Reference: https://avro.apache.org/docs/current/spec.html#schema_record
    """
    cdef dict field
    for field in schema['fields']:
        value = datum.get(field['name'], field.get('default'))
        write_data(fo, value, field['type'])


# ---- Validation of data with Schema ----------------------------------------#

cdef Py_hash_t h_null = hash('null')
cdef Py_hash_t h_boolean = hash('boolean')
cdef Py_hash_t h_int = hash('int')
cdef Py_hash_t h_long = hash('long')
cdef Py_hash_t h_float = hash('float')
cdef Py_hash_t h_double = hash('double')
cdef Py_hash_t h_bytes = hash('bytes')
cdef Py_hash_t h_string = hash('string')
cdef Py_hash_t h_fixed = hash('fixed')
cdef Py_hash_t h_enum = hash('enum')
cdef Py_hash_t h_array = hash('array')
cdef Py_hash_t h_map = hash('map')
cdef Py_hash_t h_union = hash('union')
cdef Py_hash_t h_record = hash('record')
cdef Py_hash_t h_error_union = hash('error_union')
cdef Py_hash_t h_error = hash('error')


cdef bint validate(datum, schema) except -1:
    """
    Determine if a python datum is an instance of a schema.

    This function is only called by `write_union`. Unfortunately, this function
    creates significant overhead for writing `union` types.
    I've optimized this function as well as I know how -- but any new ideas
    would be especially welcomed to help reduce this overhead cost.
    """

    if isinstance(schema, dict):
        record_type = schema['type']
    elif isinstance(schema, list):
        record_type = 'union'
    else:
        record_type = schema

    cdef Py_hash_t h_type = hash(record_type)

    if h_type == h_null:
        return datum is None

    elif h_type == h_int:
        return (
            isinstance(datum, _int_types) and INT32_MIN <= datum <= INT32_MAX
        )

    elif h_type == h_long:
        return (
            isinstance(datum, _int_types) and INT64_MIN <= datum <= INT64_MAX
        )

    elif h_type == h_string:
        return isinstance(datum, _string_types)

    elif h_type in (h_float, h_double):
        return isinstance(datum, _number_types)

    elif h_type == h_bytes:
        return isinstance(datum, bytes)

    elif h_type == h_fixed:
        return isinstance(datum, bytes) and len(datum) == schema['size']

    elif h_type == h_boolean:
        return isinstance(datum, bool)

    elif h_type == h_enum:
        return datum in schema['symbols']

    elif h_type in (h_record, h_error):
        if isinstance(datum, Mapping):
            for f in schema['fields']:
                if not validate(datum.get(f['name']), f['type']):
                    return False
            return True
        return False

    elif h_type == h_array:
        if isinstance(datum, Iterable):
            items = schema['items']
            for d in datum:
                if not validate(d, items):
                    return False
            return True
        return False

    elif h_type == h_map:
        if isinstance(datum, Mapping):
            values = schema['values']
            for k, v in iteritems(datum):
                if not (isinstance(k, _string_types) and validate(v, values)):
                    return False
            return True
        return False

    elif h_type in (h_union, h_error_union):
        for s in schema:
            if validate(datum, s):
                return True
        return False

    else:
        record_type = SCHEMA_DEFS.get(record_type)
        if record_type:
            return validate(datum, record_type)

    raise ValueError('Unknown record type: %s' % record_type)


# ---- Writer function lookup ------------------------------------------------#

cdef dict WRITERS = {
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


cpdef py_write_data(stream, datum, schema):
    """Write a `datum` of data to the output stream `stream` using the
    specified Avro `schema`.

    Paramaters
    ----------
    stream: file-like object
        Output file or stream
    datum: object
        Data to write
    schema: dict
        Avro Schema for `datum`
    """
    # This function is callable from Python
    cdef StreamWrapper stream_obj = StreamWrapper(stream)
    write_data(stream_obj, datum, schema)


cdef int write_data(Stream fo, datum, schema) except -1:
    """Write a `datum` of data to the output stream `fo`, using the Avro
    Schema `schema`.
    See full documentation in `py_write_data`
    """
    if isinstance(schema, dict):
        record_type = schema['type']
    elif isinstance(schema, list):
        record_type = 'union'
    else:
        record_type = schema

    cdef Py_hash_t h_type = hash(record_type)

    if h_type == h_long:
        return write_long(fo, datum, None)

    elif h_type in (h_record, h_error):
        return write_record(fo, datum, schema)

    elif h_type == h_string:
        # If the object is already unicode, then call `write_string`
        # If the object is a byte-string, then let's *assume* the user has
        # already encoded to UTF-8 and call `write_bytes`
        if isinstance(datum, unicode):
            return write_string(fo, datum, None)
        else:
            return write_bytes(fo, datum, None)

    elif h_type == h_int:
        return write_int(fo, datum, None)

    elif h_type == h_double:
        return write_double(fo, datum, None)

    elif h_type == h_float:
        return write_float(fo, datum, None)

    elif h_type == h_bytes:
        return write_bytes(fo, datum, None)

    elif h_type == h_boolean:
        return write_boolean(fo, datum, None)

    elif h_type == h_fixed:
        return write_fixed(fo, datum, None)

    elif h_type in (h_union, h_error_union):
        return write_union(fo, datum, schema)

    elif h_type == h_map:
        return write_map(fo, datum, schema)

    elif h_type == h_array:
        return write_array(fo, datum, schema)

    elif h_type == h_enum:
        return write_enum(fo, datum, schema)

    elif h_type == h_null:
        # `write_null` does nothing; just return
        return 0

    else:
        # User defined types will have to use the dict lookup to call their
        # corresponding `write_data` function
        WRITERS[record_type](fo, datum, schema)


# ---- Block Encoders --------------------------------------------------------#

cdef inline int null_write_block(Stream fo, bytes block_bytes) except -1:
    """Write a block of bytes with no codec ('null' codec)."""
    cdef int64_t block_len = len(block_bytes)
    write_long(fo, block_len, None)
    fo.write(block_bytes)


cdef inline int deflate_write_block(Stream fo, bytes block_bytes) except -1:
    """Write a block of bytes with the 'deflate' codec."""
    cdef bytes data = compress(block_bytes)
    cdef int64_t data_len = len(data)
    # The first two and last characters are zlib wrappers around deflate data
    data = data[2:data_len-1]
    write_long(fo, data_len - 3, None)
    fo.write(data)


cdef inline int snappy_write_block(Stream fo, bytes block_bytes) except -1:
    """Write a block of bytes wih the 'snappy' codec."""
    cdef bytes data = snappy.compress(block_bytes)
    # Add 4 bytes for the CRC32
    cdef int64_t block_len = len(data) + 4
    write_long(fo, block_len, None)
    fo.write(data)
    # Write the 4-byte, big-endian CRC32 checksum
    cdef uint32_t crc = crc32(block_bytes) & 0xFFFFFFFF
    fo.write(pack('>I', crc))


# ---- Schema Handling -------------------------------------------------------#

cdef dict SCHEMA_DEFS = dict((typ, typ) for typ in PRIMITIVE_TYPES)


cpdef get_schema_defs():
    """Return the registered schema definitions."""
    # This function is callable from Python
    return SCHEMA_DEFS


def acquaint_schema(schema, repo=None):
    """Extract `schema` into `repo` (default WRITERS)"""
    # This function is callable from Python
    repo = WRITERS if repo is None else repo
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: lambda fo, datum, _: write_data(fo, datum, schema),
    )
    extract_named_schemas_into_repo(
        schema,
        SCHEMA_DEFS,
        lambda schema: schema,
    )


cdef int write_header(Stream fo, dict metadata, bytes sync_marker) except -1:
    """Write the Avro header"""
    # Note: values in the `meta` dict are written as bytes.
    # See the definition of HEADER_SCHEMA in _schema.pyx
    header = {
        'magic': MAGIC,
        'meta': dict(
            (k, v.encode('utf-8') if isinstance(v, unicode) else v)
            for k, v in iteritems(metadata)
        ),
        'sync': sync_marker,
    }
    write_data(fo, header, HEADER_SCHEMA)


# ---- Public API - Writing Avro Files ---------------------------------------#

cpdef py_writer(
    stream, schema, records, codec='null', sync_interval=SYNC_INTERVAL,
    metadata=None
):
    """Write multiple `records` to the output stream `stream` using the
    specified Avro `schema`.

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
    # This function is callable from Python

    writer(stream, schema, records, codec, sync_interval, metadata)


cdef writer(
    stream, schema, records, codec, _sync_interval, metadata
):
    """Writes multiple `records` to the output stream `stream` using the
    specified Avro Schema `schema`.
    See full documentation in `py_writer`
    """
    cdef int64_t block_count = 0
    cdef SSize_t sync_interval
    cdef bytes sync_marker
    cdef StreamWrapper stream_obj = StreamWrapper(stream)

    # Default values
    codec = codec or 'null'
    sync_interval = _sync_interval or SYNC_INTERVAL
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
    write_header(stream_obj, metadata, sync_marker)

    # Register the schema
    acquaint_schema(schema)

    # Allocate a buffer twice the size of the sync_interval
    # (Hopefully this will avoid calls to `realloc()`)
    cdef ByteBuffer buf = ByteBuffer(sync_interval * 2)

    for record in records:
        write_data(buf, record, schema)
        block_count += 1
        if buf.pos >= sync_interval:
            write_long(stream_obj, block_count, None)
            block_writer(stream_obj, buf.getvalue())
            stream_obj.write(sync_marker)
            buf.pos = 0
            block_count = 0

    if buf.pos or block_count > 0:
        write_long(stream_obj, block_count, None)
        block_writer(stream_obj, buf.getvalue())
        stream_obj.write(sync_marker)

    stream.flush()
    buf.close()


cpdef schemaless_writer(stream, schema, record):
    """Write a single `record` to the output stream `stream` without
    writing the Avro schema and header information.

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
    cdef StreamWrapper stream_obj = StreamWrapper(stream)
    write_data(stream_obj, record, schema)
