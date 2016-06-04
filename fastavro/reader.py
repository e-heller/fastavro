# -*- coding: utf-8 -*-
"""Python implementation for reading Apache Avro files"""


# This code is based on the Apache 'avro' pacakge, found at:
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/
# Under the Apache 2.0 License (https://www.apache.org/licenses/LICENSE-2.0)

# Please refer to the Avro specification page for details on data encoding:
#   https://avro.apache.org/docs/current/spec.html


from __future__ import absolute_import

import sys
from struct import unpack
from zlib import decompress

try:
    import simplejson as json
except ImportError:
    import json

try:
    import snappy
except ImportError:
    snappy = None

from fastavro.compat import BytesIO, xrange, iteritems, byte2int
from fastavro.schema import (
    normalize_schema, extract_named_schemas, HEADER_SCHEMA, MAGIC, SYNC_SIZE,
    PRIMITIVE_TYPES, AVRO_TYPES,
)


# ---- Exceptions ------------------------------------------------------------#

class SchemaResolutionError(Exception):
    pass


class ReadError(Exception):
    def __init__(self, msg, original_exc=None):
        super(ReadError, self).__init__(msg, original_exc)
        self.msg = msg
        self.original_exc = original_exc


# ---- Schema Resolution / Matching ------------------------------------------#

def match_schemas(w_schema, r_schema):
    """Match the writer's schema `w_schema` with the reader's schema `r_schema`

    Return True if `w_schema` is a *match* for `r_schema`, else False

    From: https://avro.apache.org/docs/current/spec.html#Schema+Resolution

    It is an error if the two schemas do not *match*.
    To *match*, one of the following must hold:
      * Both schemas are arrays whose item types match
      * Both schemas are maps whose value types match
      * Both schemas are enums whose names match
      * Both schemas are fixed whose sizes and names match
      * Both schemas are records with the same name

      * Either schema is a union
        + If both are unions:
          - The first schema in the reader's union that matches the selected
            writer's union schema is recursively resolved against it.
        + If reader's is a union, but writer's is not:
          - The first schema in the reader's union that matches the writer's
            schema is recursively resolved against it.
        + If writer's is a union, but reader's is not
          - If the reader's schema matches the selected writer's schema, it is
            recursively resolved against it.

      * Both schemas have the same primitive type
        OR the writer's schema may be *promoted* to the reader's as follows:
        + int is promotable to long, float, or double
        + long is promotable to float or double
        + float is promotable to double
        + string is promotable to bytes
        + bytes is promotable to string
    """
    if isinstance(w_schema, dict) and isinstance(r_schema, dict):
        # Array, Map, Enum, Fixed, Record, Error
        w_type = w_schema['type']
        r_type = r_schema['type']
        if w_type != r_type:
            return False
        if w_type == 'array':
            # 'Both schemas are arrays whose item types match'
            return match_schemas(w_schema['items'], r_schema['items'])
        elif w_type == 'map':
            # 'Both schemas are maps whose value types match'
            return match_schemas(w_schema['values'], r_schema['values'])
        elif w_type in ('enum', 'record', 'error'):
            # 'Both schemas are enums whose names match'
            # 'Both schemas are records with the same name'
            # Note: Futher checks must be applied after data is read in
            # `read_enum()` and `read_record()`
            return w_schema['name'] == r_schema['name']
        elif w_type == 'fixed':
            # 'Both schemas are fixed whose sizes and names match'
            return (
                w_schema['name'] == r_schema['name'] and
                w_schema['size'] == r_schema['size']
            )
        elif w_type == r_type:
            # Unknown type - just return True
            return True

    elif isinstance(w_schema, list) or isinstance(r_schema, list):
        # 'Either schema is a union'
        if isinstance(w_schema, list):
            # If the writer is a union, the check is applied in `read_union()`
            # when the correct schema is known.
            return True
        else:
            # If the reader is a union, ensure at least one of the schemas in
            # the reader's union matches the writer's schema.
            return any(match_schemas(w_schema, s) for s in r_schema)

    elif w_schema == r_schema:
        return True

    # Promotion cases:
    elif w_schema == 'int' and r_schema in ('long', 'float', 'double'):
        return True
    elif w_schema == 'long' and r_schema in ('float', 'double'):
        return True
    elif w_schema == 'float' and r_schema == 'double':
        return True
    elif w_schema == 'string' and r_schema == 'bytes':
        return True
    elif w_schema == 'bytes' and r_schema == 'string':
        return True

    return False


# ---- Reading Avro primitives -----------------------------------------------#

def read_null(stream, writer_schema=None, reader_schema=None):
    """A `null` value is not written at all."""
    return None


def read_boolean(stream, writer_schema=None, reader_schema=None):
    """A `boolean` value is written as a single byte: b'0x00' for False,
    b'0x01' for True.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    # Although technically 0x01 == True and 0x00 == False, many languages will
    # cast anything other than 0 to True and only 0 to False
    c = stream.read(1)
    if not c:
        raise EOFError("EOF in read_boolean")
    return c != b'\x00'


def read_long(stream, writer_schema=None, reader_schema=None):
    """`int` and `long` values are written as 32-bit and 64-bit integers,
    respectively, using a variable-length zig-zag encoding. This is the same
    encoding used in Google protobufs.

    A good explanation of this encoding can be found at:
        https://developers.google.com/protocol-buffers/docs/encoding#signed-integers

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    c = stream.read(1)
    if not c:
        raise EOFError("EOF in read_long")
    b = ord(c)
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
        b = ord(stream.read(1))
        n |= (b & 0x7F) << shift
        shift += 7
    return (n >> 1) ^ -(n & 1)


# Alias `read_int` to `read_long`
read_int = read_long


def read_float(stream, writer_schema=None, reader_schema=None):
    """A `float` value is written as a single precision 32-bit IEEE 754
    floating-point value in little-endian format.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    return unpack('<f', stream.read(4))[0]


def read_double(stream, writer_schema=None, reader_schema=None):
    """A `double` value is written as a double precision 64-bit IEEE 754
    floating-point value in little-endian format.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    return unpack('<d', stream.read(8))[0]


def read_bytes(stream, writer_schema=None, reader_schema=None):
    """A `bytes` value is written as a `long` (length of the byte string),
    immediately followed by the raw byte data.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    size = read_long(stream)
    if reader_schema == 'string':
        # Schema Resolution: promote to unicode string
        return stream.read(size).decode('utf-8')
    else:
        return stream.read(size)


def read_string(stream, writer_schema=None, reader_schema=None):
    """A `string` value is written as a `long` (length of the UTF-8 encoded
    string), immediately followed by the UTF-8 encoded byte data.

    Note: Avro `string` values *must* be encoded to UTF-8 byte strings.

    Reference: https://avro.apache.org/docs/current/spec.html#binary_encode_primitive
    """  # noqa
    size = read_long(stream)
    if reader_schema == 'bytes':
        # Schema Resolution: promote to byte string
        return stream.read(size)
    else:
        return stream.read(size).decode('utf-8')


# ---- Reading Avro complex types --------------------------------------------#

def read_fixed(stream, writer_schema, reader_schema=None):
    """A `fixed` value is written as raw bytes. The length of the byte data
    is declared in the schema.

    Reference: https://avro.apache.org/docs/current/spec.html#Fixed
    """
    return stream.read(writer_schema['size'])


def read_enum(stream, writer_schema, reader_schema=None):
    """An `enum` value is written as an `int` representing the zero-based
    position of the symbol in the schema.

    Reference: https://avro.apache.org/docs/current/spec.html#Enums
    """
    index = read_int(stream)
    symbol = writer_schema['symbols'][index]
    if reader_schema and symbol not in reader_schema['symbols']:
        # Schema Resolution: 'If the writer's symbol is not present in the
        # reader's enum, then an error is signalled.'
        symlist = reader_schema['symbols']
        msg = '%s not found in reader symbol list %s' % (symbol, symlist)
        raise SchemaResolutionError(msg)
    return symbol


def read_array(stream, writer_schema, reader_schema=None):
    """An `array` value is written as a series of blocks.

    Each block consists of a `long` `count` value, followed by that many array
    items. A block with `count` zero indicates the end of the array. Each item
    is encoded per the array's item schema.

    If a block's `count` is negative, then the `count` is followed immediately
    by a `long` block size, indicating the number of bytes in the block. The
    actual `count` in this case is the absolute value of the `count` written.

    Reference: https://avro.apache.org/docs/current/spec.html#Arrays
    """
    w_item_schema = writer_schema['items']
    r_item_schema = reader_schema['items'] if reader_schema else None
    array_items = []

    block_count = read_long(stream)
    while block_count:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(stream)

        for i in xrange(block_count):
            array_items.append(read_data(stream, w_item_schema, r_item_schema))
        block_count = read_long(stream)
    return array_items


def read_map(stream, writer_schema, reader_schema=None):
    """A `map` value is written as a series of blocks.

    Each block consists of a `long` `count` value, followed by that many
    key / value pairs. A block with `count` zero indicates the end of the map.

    If a block's `count` is negative, then the `count` is followed immediately
    by a `long` block size, indicating the number of bytes in the block. The
    actual `count` in this case is the absolute value of the `count` written.

    For reading the key / value pairs:
        A map `key` is assumed to be string
        A map `value` is read per the map's 'value' schema

    Reference: https://avro.apache.org/docs/current/spec.html#Maps
    """
    w_value_schema = writer_schema['values']
    r_value_schema = reader_schema['values'] if reader_schema else None
    map_items = {}

    block_count = read_long(stream)
    while block_count:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(stream)

        for i in xrange(block_count):
            key = read_string(stream)
            map_items[key] = read_data(stream, w_value_schema, r_value_schema)
        block_count = read_long(stream)
    return map_items


def read_union(stream, writer_schema, reader_schema=None):
    """A `union` value is written as a `long` indicating the zero-based
    position of the `value` in the union's schema, immediately followed by
    the `value`, written per the indicated schema within the union.

    Reference: https://avro.apache.org/docs/current/spec.html#Unions
    """
    index = read_long(stream)
    w_schema = writer_schema[index]
    if reader_schema:
        # Schema Resolution
        r_schemas = (reader_schema if isinstance(reader_schema, list)
                     else (reader_schema,))
        for r_schema in r_schemas:
            if match_schemas(w_schema, r_schema):
                return read_data(stream, w_schema, r_schema)
        raise SchemaResolutionError(
            'Schema mismatch: %s cannot resolve to %s'
            % (writer_schema, reader_schema)
        )
    else:
        return read_data(stream, w_schema)


def read_record(stream, writer_schema, reader_schema=None):
    """A `record` value is written by encoding the values of its fields in the
    order in which they are declared. In other words, a `record` is written
    as just the concatenation of the encodings of its fields. Field values
    are encoded per their schema.

    Reference: https://avro.apache.org/docs/current/spec.html#schema_record

    Schema Resolution:
      * The ordering of fields may be different: fields are matched by name.
      * Schemas for fields with the same name in both records are resolved
        recursively.
      * If the writer's record contains a field with a name not present in the
        reader's record, the writer's value for that field is ignored.
      * If the reader's record schema has a field that contains a default
        value, and writer's schema does not have a field with the same name,
        then the reader should use the default value from its field.
      * If the reader's record schema has a field with no default value, and
        writer's schema does not have a field with the same name, an error is
        signalled.

    Reference: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
    """
    record = {}
    if reader_schema is None:
        for field in writer_schema['fields']:
            record[field['name']] = read_data(stream, field['type'])
    else:
        readers_field_dict = dict(
            (f['name'], f) for f in reader_schema['fields']
        )
        for field in writer_schema['fields']:
            readers_field = readers_field_dict.get(field['name'])
            if readers_field:
                record[field['name']] = read_data(
                    stream, field['type'], readers_field['type']
                )
            else:
                # should implement skip
                read_data(stream, field['type'], field['type'])

        # fill in default values
        if len(readers_field_dict) > len(record):
            writer_fields = set(f['name'] for f in writer_schema['fields'])
            for field_name, field in iteritems(readers_field_dict):
                if field_name not in writer_fields:
                    if 'default' in field:
                        record[field['name']] = field['default']
                    else:
                        msg = 'No default value for %s' % field['name']
                        raise SchemaResolutionError(msg)
    return record


# ---- Reader function lookup ------------------------------------------------#

READERS = {}


def reset_reader_funcs():
    """Reset the Reader functions to their original state."""
    READERS.clear()
    READERS.update({
        # Primitive types
        'null': read_null,
        'boolean': read_boolean,
        'int': read_int,
        'long': read_long,
        'float': read_float,
        'double': read_double,
        'bytes': read_bytes,
        'string': read_string,

        # Complex types
        'fixed': read_fixed,
        'enum': read_enum,
        'array': read_array,
        'map': read_map,
        'union': read_union,
        'record': read_record,
        'error': read_record,
        'error_union': read_union,
    })

reset_reader_funcs()


def get_reader_funcs():
    """Return the registered Reader functions."""
    return READERS


def read_data(stream, writer_schema, reader_schema=None):
    """Read data from the input `stream` according to the specified Avro
    `writer_schema`, optionally migrating to the `reader_schema` if provided.

    Paramaters
    ----------
    stream: file-like object
        Input file or stream
    writer_schema: dict
        Avro "writer's schema"
    reader_schema: dict, optional
        Avro "reader's schema"
    """
    if isinstance(writer_schema, dict):
        record_type = writer_schema['type']
    elif isinstance(writer_schema, list):
        record_type = 'union'
    else:
        record_type = writer_schema

    if reader_schema and record_type in AVRO_TYPES:
        if not match_schemas(writer_schema, reader_schema):
            raise SchemaResolutionError(
                'Schema mismatch: %s does not match %s'
                % (writer_schema, reader_schema)
            )

    try:
        return READERS[record_type](stream, writer_schema, reader_schema)
    except SchemaResolutionError:
        raise
    except Exception as exc:
        raise ReadError(
            'Failed to read %r type' % record_type, exc
        )


# ---- Block Decoders --------------------------------------------------------#

def null_read_block(stream, buffer):
    """Read a block of data with no codec ('null' codec)."""
    block_len = read_long(stream)
    data = stream.read(block_len)
    buffer.truncate(0)
    buffer.seek(0)
    buffer.write(data)
    buffer.seek(0)


def deflate_read_block(stream, buffer):
    """Read a block of data with the 'deflate' codec."""
    block_len = read_long(stream)
    data = stream.read(block_len)
    # -15 is the log of the window size; negative indicates "raw"
    # (no zlib headers) decompression.  See zlib.h.
    decompressed = decompress(data, -15)
    buffer.truncate(0)
    buffer.seek(0)
    buffer.write(decompressed)
    buffer.seek(0)


def snappy_read_block(stream, buffer):
    """Read a block of data with the 'snappy' codec."""
    block_len = read_long(stream)
    data = stream.read(block_len)
    # Trim off last 4 bytes which hold the CRC32
    decompressed = snappy.decompress(data[:-4])
    buffer.truncate(0)
    buffer.seek(0)
    buffer.write(decompressed)
    buffer.seek(0)


def skip_sync(stream, sync_marker):
    """Skip an expected sync marker. Raise a ValueError if it doesn't match."""
    if stream.read(SYNC_SIZE) != sync_marker:
        raise ValueError('Expected sync marker not found')


# ---- Schema Handling -------------------------------------------------------#

SCHEMA_DEFS = {}


def reset_schema_defs():
    """Reset the registered schema definitions to their original state."""
    SCHEMA_DEFS.clear()
    SCHEMA_DEFS.update((typ, typ) for typ in PRIMITIVE_TYPES)

reset_schema_defs()


def get_schema_defs():
    """Return the registered schema definitions."""
    return SCHEMA_DEFS


def acquaint_schema(schema, repo=None, reader_schema_defs=None):
    """Extract `schema` into `repo` (default READERS)"""
    repo = READERS if repo is None else repo
    reader_schema_defs = (
        SCHEMA_DEFS if reader_schema_defs is None else reader_schema_defs
    )
    extract_named_schemas(
        schema,
        repo,
        lambda schema: (
            lambda stream, _, r_schema: (
                read_data(stream, schema, reader_schema_defs.get(r_schema))
            )
        ),
    )


def populate_schema_defs(schema, repo=None):
    """Add a `schema` definition to `repo` (default SCHEMA_DEFS)"""
    repo = SCHEMA_DEFS if repo is None else repo
    extract_named_schemas(
        schema,
        repo,
        lambda schema: schema,
    )


# ---- Public API - Reading Avro Files ---------------------------------------#

class Reader(object):
    """Creates an Avro reader as an iterator over the records in an Avro file.
    """

    def __init__(self, stream, reader_schema=None):
        """Creates an Avro reader as an iterator over the records in the Avro
        file `stream`, optionally migrating to the `reader_schema` if provided.

        Paramaters
        ----------
        stream: file-like object
            Input file or stream
        reader_schema: dict, optional
            Avro "reader's schema"

        Example
        -------
        >>> import fastavro
        >>>
        >>> with open('some-file.avro', 'rb') as input:
        >>>     # Obtain the record iterator:
        >>>     reader = fastavro.Reader(input)
        >>>
        >>>     # Obtain the writer's schema if required:
        >>>     schema = reader.schema
        >>>
        >>>     # Iterate over the records:
        >>>     for record in reader:
        >>>         process_record(record)
        """
        self.stream = stream
        self.reader_schema = (
            normalize_schema(reader_schema) if reader_schema else None
        )

        self._read_header()

        # Verify `codec`
        if self.codec == 'snappy' and not snappy:
            raise ValueError(
                "Cannot read 'snappy' codec: 'snappy' module is not available"
            )
        elif self.codec not in ('null', 'deflate'):
            raise ValueError('Unknown codec: %r' % self.codec)

        # Register the schema
        acquaint_schema(self.writer_schema)
        if reader_schema:
            populate_schema_defs(reader_schema)

        self._iterator = self._record_iterator()

    def __iter__(self):
        return self._iterator

    def next(self):
        return next(self._iterator)

    def _read_header(self):
        """Read the Avro Header information"""
        try:
            self._header = read_data(self.stream, HEADER_SCHEMA)
        except Exception as exc:
            raise ReadError('Failed to read Avro header', exc)

        # Read `magic`
        self._magic = self._header['magic']
        if self._magic != MAGIC:
            version = byte2int(self._magic[-1])
            sys.stderr.write(
                'Warning: unsupported Avro version: %d\n' % version
            )

        self._sync_marker = self._header['sync']

        # Read Metadata - `meta` values are bytes, decode them to unicode
        self.metadata = dict(
            (k, v.decode('utf-8')) for k, v in iteritems(self._header['meta'])
        )

        self.schema = self.writer_schema = normalize_schema(
            json.loads(self.metadata['avro.schema'])
        )
        self.codec = self.metadata.get('avro.codec', u'null')

    def _record_iterator(self):
        """Iterator function over the records in the Avro file"""

        # Alias these values so the code won't need to keep performing
        # attribute lookups on `self` (small optimization)
        stream = self.stream
        sync_marker = self._sync_marker
        writer_schema = self.writer_schema
        reader_schema = self.reader_schema

        # Get the block decoder
        if self.codec == 'deflate':
            read_block = deflate_read_block
        elif self.codec == 'snappy':
            read_block = snappy_read_block
        else:
            read_block = null_read_block

        block_buf = BytesIO()

        try:
            while True:
                block_count = read_long(stream)
                read_block(stream, block_buf)

                for i in xrange(block_count):
                    yield read_data(block_buf, writer_schema, reader_schema)

                skip_sync(stream, sync_marker)

        except EOFError:
            pass
        finally:
            block_buf.close()


# For backwards compatability
iter_avro = Reader


def schemaless_reader(stream, schema):
    """Reads a single record writen with `fastavro.schemaless_writer`

    Paramaters
    ----------
    stream: file-like object
        Input file or stream
    schema: dict
        Avro "reader's schema"
    """
    acquaint_schema(schema)
    return read_data(stream, schema)
