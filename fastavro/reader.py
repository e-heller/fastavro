# -*- coding: utf-8 -*-
# cython: auto_cpdef=True
"""Python code for reading AVRO files"""


# This code is based on the Apache 'avro' pacakge, found at:
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/
# Under the Apache 2.0 License (https://www.apache.org/licenses/LICENSE-2.0)

# Please refer to the Avro specification page for details on data encoding:
#   https://avro.apache.org/docs/current/spec.html


from __future__ import absolute_import

import sys
from struct import unpack, error as StructError
from zlib import decompress

try:
    import simplejson as json
except ImportError:
    import json

try:
    import snappy
except ImportError:
    snappy = None

try:
    from fastavro._compat import BytesIO, xrange, iteritems, byte2int
    from fastavro._schema import (
        extract_named_schemas_into_repo, HEADER_SCHEMA, MAGIC, SYNC_SIZE,
        PRIMITIVE_TYPES, AVRO_TYPES,
    )
except ImportError:
    from fastavro.compat import BytesIO, xrange, iteritems, byte2int
    from fastavro.schema import (
        extract_named_schemas_into_repo, HEADER_SCHEMA, MAGIC, SYNC_SIZE,
        PRIMITIVE_TYPES, AVRO_TYPES,
    )


# ---- Exceptions ------------------------------------------------------------#

class SchemaResolutionError(Exception):
    pass


# ---- Schema Resolution / Matching ------------------------------------------#

def match_types(writer_type, reader_type):
    if isinstance(writer_type, list) or isinstance(reader_type, list):
        return True
    if writer_type == reader_type:
        return True
    # promotion cases
    elif writer_type == 'int' and reader_type in ('long', 'float', 'double'):
        return True
    elif writer_type == 'long' and reader_type in ('float', 'double'):
        return True
    elif writer_type == 'float' and reader_type == 'double':
        return True
    return False


def match_schemas(w_schema, r_schema):
    error_msg = 'Schema mismatch: %s is not %s' % (w_schema, r_schema)
    if isinstance(w_schema, list):
        # If the writer is a union, checks will happen in read_union after the
        # correct schema is known
        return True
    elif isinstance(r_schema, list):
        # If the reader is a union, ensure one of the new schemas is the same
        # as the writer
        for schema in r_schema:
            if match_types(w_schema, schema):
                return True
        else:
            raise SchemaResolutionError(error_msg)
    else:
        # Check for dicts as primitive types are just strings
        if isinstance(w_schema, dict):
            w_type = w_schema['type']
        else:
            w_type = w_schema
        if isinstance(r_schema, dict):
            r_type = r_schema['type']
        else:
            r_type = r_schema

        if w_type == r_type == 'map':
            if match_types(w_schema['values'], r_schema['values']):
                return True
        elif w_type == r_type == 'array':
            if match_types(w_schema['items'], r_schema['items']):
                return True
        elif match_types(w_type, r_type):
            return True
        raise SchemaResolutionError(error_msg)


# ---- Reading Avro primitives -----------------------------------------------#

def read_null(fo, writer_schema=None, reader_schema=None):
    """null is written as zero bytes."""
    return None


def read_boolean(fo, writer_schema=None, reader_schema=None):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).
    """
    # technically 0x01 == true and 0x00 == false, but many languages will cast
    # anything other than 0 to True and only 0 to False
    c = fo.read(1)
    if not c:
        raise EOFError("Failed to read 'boolean' value")
    return c != b'\x00'


def read_long(fo, writer_schema=None, reader_schema=None):
    """int and long values are written using variable-length, zig-zag
    coding."""
    c = fo.read(1)
    # We do EOF checking only here, since most reader start here
    if not c:
        raise StopIteration
    b = ord(c)
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
        b = ord(fo.read(1))
        n |= (b & 0x7F) << shift
        shift += 7
    return (n >> 1) ^ -(n & 1)


# Alias `read_int` to `read_long`
read_int = read_long


def read_float(fo, writer_schema=None, reader_schema=None):
    """A float is written as 4 bytes.

    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    return unpack('<f', fo.read(4))[0]


def read_double(fo, writer_schema=None, reader_schema=None):
    """A double is written as 8 bytes.

    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    return unpack('<d', fo.read(8))[0]


def read_bytes(fo, writer_schema=None, reader_schema=None):
    """Bytes are encoded as a long followed by that many bytes of data."""
    size = read_long(fo)
    return fo.read(size)


def read_string(fo, writer_schema=None, reader_schema=None):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.
    """
    size = read_long(fo)
    return fo.read(size).decode('utf-8')


# ---- Reading Avro complex types --------------------------------------------#

def read_fixed(fo, writer_schema, reader_schema=None):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    return fo.read(writer_schema['size'])


def read_enum(fo, writer_schema, reader_schema=None):
    """An enum is encoded by a int, representing the zero-based position of the
    symbol in the schema.
    """
    index = read_int(fo)
    symbol = writer_schema['symbols'][index]
    if reader_schema and symbol not in reader_schema['symbols']:
        symlist = reader_schema['symbols']
        msg = '%s not found in reader symbol list %s' % (symbol, symlist)
        raise SchemaResolutionError(msg)
    return symbol


def read_array(fo, writer_schema, reader_schema=None):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    w_item_schema = writer_schema['items']
    r_item_schema = reader_schema['items'] if reader_schema else None
    array_items = []

    block_count = read_long(fo)
    while block_count:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        for i in xrange(block_count):
            array_items.append(read_data(fo, w_item_schema, r_item_schema))
        block_count = read_long(fo)
    return array_items


def read_map(fo, writer_schema, reader_schema=None):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    w_value_schema = writer_schema['values']
    r_value_schema = reader_schema['values'] if reader_schema else None
    map_items = {}

    block_count = read_long(fo)
    while block_count:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        for i in xrange(block_count):
            key = read_string(fo)
            map_items[key] = read_data(fo, w_value_schema, r_value_schema)
        block_count = read_long(fo)
    return map_items


def read_union(fo, writer_schema, reader_schema=None):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value.

    The value is then encoded per the indicated schema within the union.
    """
    index = read_long(fo)
    w_schema = writer_schema[index]
    if reader_schema:
        # Schema Resolution
        r_schemas = (reader_schema if isinstance(reader_schema, list)
                     else (reader_schema,))
        for r_schema in r_schemas:
            if match_types(w_schema, r_schema):
                return read_data(fo, w_schema, r_schema)
        raise SchemaResolutionError(
            'Schema mismatch: %s cannot resolve to %s'
            % (writer_schema, reader_schema)
        )
    else:
        return read_data(fo, w_schema)


def read_record(fo, writer_schema, reader_schema=None):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema.

    Schema Resolution:
     * the ordering of fields may be different: fields are matched by name.
     * schemas for fields with the same name in both records are resolved
         recursively.
     * if the writer's record contains a field with a name not present in the
         reader's record, the writer's value for that field is ignored.
     * if the reader's record schema has a field that contains a default value,
         and writer's schema does not have a field with the same name, then the
         reader should use the default value from its field.
     * if the reader's record schema has a field with no default value, and
         writer's schema does not have a field with the same name, then the
         field's value is unset.
    """
    record = {}
    if reader_schema is None:
        for field in writer_schema['fields']:
            record[field['name']] = read_data(fo, field['type'])
    else:
        readers_field_dict = dict(
            (f['name'], f) for f in reader_schema['fields']
        )
        for field in writer_schema['fields']:
            readers_field = readers_field_dict.get(field['name'])
            if readers_field:
                record[field['name']] = read_data(
                    fo, field['type'], readers_field['type']
                )
            else:
                # should implement skip
                read_data(fo, field['type'], field['type'])

        # fill in default values
        if len(readers_field_dict) > len(record):
            writer_fields = set(f['name'] for f in writer_schema['fields'])
            for field_name, field in iteritems(readers_field_dict):
                if field_name not in writer_fields:
                    default = field.get('default')
                    if default:
                        record[field['name']] = default
                    else:
                        msg = 'No default value for %s' % field['name']
                        raise SchemaResolutionError(msg)
    return record


# ---- Reader function lookup ------------------------------------------------#

READERS = {
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
}


def read_data(fo, writer_schema, reader_schema=None):
    """Read data from file object according to schema."""

    if isinstance(writer_schema, dict):
        record_type = writer_schema['type']
    elif isinstance(writer_schema, list):
        record_type = 'union'
    else:
        record_type = writer_schema

    if reader_schema and record_type in AVRO_TYPES:
        match_schemas(writer_schema, reader_schema)
    try:
        return READERS[record_type](fo, writer_schema, reader_schema)
    except StructError:
        raise EOFError('Cannot read %s from %s' % (record_type, fo))


# ---- Block Decoders --------------------------------------------------------#

def null_read_block(fo, buffer):
    """Read a block of data with no codec ('null' codec)."""
    block_len = read_long(fo)
    data = fo.read(block_len)
    buffer.truncate(0)
    buffer.seek(0)
    buffer.write(data)
    buffer.seek(0)


def deflate_read_block(fo, buffer):
    """Read a block of data with the 'deflate' codec."""
    block_len = read_long(fo)
    data = fo.read(block_len)
    # -15 is the log of the window size; negative indicates "raw"
    # (no zlib headers) decompression.  See zlib.h.
    decompressed = decompress(data, -15)
    buffer.truncate(0)
    buffer.seek(0)
    buffer.write(decompressed)
    buffer.seek(0)


def snappy_read_block(fo, buffer):
    """Read a block of data with the 'snappy' codec."""
    block_len = read_long(fo)
    data = fo.read(block_len)
    # Trim off last 4 bytes which hold the CRC32
    decompressed = snappy.decompress(data[:-4])
    buffer.truncate(0)
    buffer.seek(0)
    buffer.write(decompressed)
    buffer.seek(0)


def skip_sync(fo, sync_marker):
    """Skip an expected sync marker. Raise a ValueError if it doesn't match."""
    if fo.read(SYNC_SIZE) != sync_marker:
        raise ValueError('Expected sync marker not found')


# ---- Schema Handling -------------------------------------------------------#

SCHEMA_DEFS = dict((typ, typ) for typ in PRIMITIVE_TYPES)


def get_schema_defs():
    """Return the registered schema definitions."""
    return SCHEMA_DEFS


def acquaint_schema(schema, repo=None, reader_schema_defs=None):
    """Extract `schema` into `repo` (default READERS)"""
    repo = READERS if repo is None else repo
    reader_schema_defs = (
        SCHEMA_DEFS if reader_schema_defs is None else reader_schema_defs
    )
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: lambda fo, _, r_schema: read_data(
            fo, schema, reader_schema_defs.get(r_schema)
        ),
    )


def populate_schema_defs(schema, repo=None):
    """Add a `schema` definition to `repo` (default SCHEMA_DEFS)"""
    repo = SCHEMA_DEFS if repo is None else repo
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: schema,
    )


# ---- Public API - Reading Avro Files ---------------------------------------#

class Reader(object):
    """Creates an Avro reader as an iterator over the records in an Avro file.
    """

    def __init__(self, fo, reader_schema=None):
        """Creates an Avro reader as an iterator over the records in the Avro
        file `fo`, optionally migrating to the `reader_schema` if provided.

        Paramaters
        ----------
        fo: file-like object
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
        self.fo = fo
        self.reader_schema = reader_schema

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
            self._header = read_data(self.fo, HEADER_SCHEMA)
        except StopIteration:
            raise ValueError('Failed to read Avro header')

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

        self.schema = self.writer_schema = (
            json.loads(self.metadata['avro.schema'])
        )
        self.codec = self.metadata.get('avro.codec', u'null')

    def _record_iterator(self):
        """Iterator function over the records in the Avro file"""

        # Alias these values so the code won't need to keep performing
        # attribute lookups on `self` (small optimization)
        fo = self.fo
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

        while True:
            block_count = read_long(fo)
            read_block(fo, block_buf)

            for i in xrange(block_count):
                yield read_data(block_buf, writer_schema, reader_schema)

            skip_sync(fo, sync_marker)

        block_buf.close()


# For backwards compatability
iter_avro = Reader


def schemaless_reader(fo, schema):
    """Reads a single record writen with `fastavro.schemaless_writer`

    Paramaters
    ----------
    fo: file-like object
        Input file or stream
    schema: dict
        Avro "reader's schema"
    """
    acquaint_schema(schema)
    return read_data(fo, schema)
