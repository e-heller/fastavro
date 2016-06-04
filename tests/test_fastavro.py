# -*- coding: utf-8 -*-
"""Tests for fastavro"""


# Some of this code is derived from the Apache 'avro' pacakge, found at:
# http://svn.apache.org/viewvc/avro/trunk/lang/py/
# Under the Apache 2.0 License (https://www.apache.org/licenses/LICENSE-2.0)


from __future__ import absolute_import

import re
import sys
from glob import iglob
from os.path import join, abspath, dirname, basename

try:
    # Import unittest module (requires `unittest2` for Python 2.x)
    PY2 = sys.version_info[0] == 2
    if PY2:
        import unittest2 as unittest
    else:
        import unittest
except ImportError:
    raise ImportError("The 'unittest2' module is required for Python 2.x")

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

try:
    import snappy
except ImportError:
    snappy = None

import fastavro
from fastavro import (
    ReadError, SchemaResolutionError, UnknownTypeError, InvalidTypeError,
    SchemaAttributeError,
)

from tests.utils import (
    random_byte_str, random_unicode_str, _unicode_type, _bytes_type,
)


# ---- Utils -----------------------------------------------------------------#

def _write(schema, records, **kwargs):
    buffer = BytesIO()
    fastavro.write(buffer, schema, records, **kwargs)
    buffer.seek(0)
    return buffer


def _read(input, readers_schema=None):
    reader = fastavro.Reader(input, readers_schema)
    return list(reader)


class NoSeekBytesIO(object):
    """Shim around BytesIO which blocks access to everything but read.
    Used to ensure seek API isn't being depended on."""

    def __init__(self, *args):
        self.underlying = BytesIO(*args)

    def read(self, n):
        return self.underlying.read(n)

    def seek(self, *args):
        raise AssertionError('fastavro reader should not depend on seek')

    def close(self):
        self.underlying.close()


# ---- Test Read and Write files from /tests/avro-files/ ---------------------#

data_dir = join(abspath(dirname(__file__)), 'avro-files')

NO_DATA = set([
    'class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro',
    'testDataFileMeta.avro',
])


def _examine_exception(self, exc, substrs, quote=True):
    substrs = substrs if isinstance(substrs, (list, tuple)) else (substrs,)
    self.assertTrue(
        all(("'%s'" % s if quote else s) in str(exc) for s in substrs),
        'Incorrect exception raised: %s' % repr(exc)
    )


class TestAvroFiles(unittest.TestCase):

    def read_write_file(self, filename):
        fastavro.reset_schema_repository()
        with open(filename, 'rb') as input:
            reader = fastavro.Reader(input)
            self.assertTrue(hasattr(reader, 'schema'), 'Failed to read schema')
            if basename(filename) in NO_DATA:
                return
            records = list(reader)
            self.assertGreaterEqual(len(records), 0, 'No records found')

        output = _write(reader.schema, records, codec=reader.codec)
        serialized_data = output.getvalue()

        # Read back output from `fastavro.write`
        input = BytesIO(serialized_data)
        output_reader = fastavro.Reader(input)
        self.assertTrue(hasattr(output_reader, 'schema'),
                        'Schema was not written')
        self.assertEqual(output_reader.schema, reader.schema)
        self.assertEqual(output_reader.codec, reader.codec)
        new_records = list(output_reader)
        self.assertEqual(new_records, records)
        input.close()

        # Test schema migration with the same schema
        input = BytesIO(serialized_data)
        migration_reader = fastavro.Reader(input, reader.schema)
        self.assertTrue(hasattr(migration_reader, 'schema'),
                        'Schema was not written')
        self.assertEqual(migration_reader.reader_schema, reader.schema)
        self.assertEqual(output_reader.codec, reader.codec)
        new_records = list(migration_reader)
        self.assertEqual(new_records, records)
        input.close()


def create_avro_file_methods():
    """
    This function is run at module import. It finds each avro test file in
    `data_dir` (/tests/avro-files/), and dynamically creates a new method on
    in the `TestAvroFiles` class to test that file.
    The actual test is performed by `TestAvroFiles.read_write_file()`
    """
    filenames = iglob(join(data_dir, '*.avro'))
    for idx, filename in enumerate(sorted(filenames)):
        if 'snappy' in filename and not snappy:
            continue

        def wrapper(self):
            with self.subTest('Test file: %s' % filename):
                self.read_write_file(filename)

        # Cleanup the filename, to use as the name of the test method
        name, ext = basename(filename).rsplit('.', 1)
        name = re.sub(r'\s|-|\.', '_', name)
        wrapper.__name__ = func_name = 'test_file_%s' % name

        # Create the test method
        setattr(TestAvroFiles, func_name, wrapper)

create_avro_file_methods()


# ---- Some Test Scenarios / Definitions -------------------------------------#

# These scenarios are copied from the Apache Avro 1.8.1 Python unit tests:
#    /lang/py/test/test_io.py

LONG_RECORD_SCHEMA = {
    'name': 'Test',
    'type': 'record',
    'fields': [
        {'name': 'A', 'type': 'int'},
        {'name': 'B', 'type': 'int'},
        {'name': 'C', 'type': 'int'},
        {'name': 'D', 'type': 'int'},
        {'name': 'E', 'type': 'int'},
        {'name': 'F', 'type': 'int'},
        {'name': 'G', 'type': 'int'},
    ]
}

LONG_RECORD_DATUM = {
    'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7
}

EXAMPLE_SCHEMAS = (
    # These are pairs: (schema, example datum)
    ('null', None),
    ('boolean', True),
    ('string', u'unicode value'),
    ('bytes', b'\x01\xce\xb6\xcf\x89\xce\xae\xFF'),
    ('int', 1234),
    ('long', 5678),
    ('float', 123.456),
    ('double', 456.789),
    ({'type': 'fixed', 'name': 'TestFixed', 'size': 8},
     b'\x01\xce\xb6\xcf\x89\xce\xae\xFF'),
    ({'type': 'enum', 'name': 'TestEnum', 'symbols': ['A', 'B']}, 'B'),
    ({'type': 'array', 'items': 'int'}, [123, -456, 789]),
    ({'type': 'map', 'values': 'long'}, {'a': -123, 'b': 456, 'c': -789}),
    (['string', 'null', 'long'], None),
    ({'type': 'record',
        'name': 'TestRecord',
        'fields': [{'name': 'f', 'type': 'long'}],
      }, {'f': 5}),
    ({'type': 'record',
        'name': 'Lisp',
        'fields': [{
            'name': 'value',
            'type': [
                'null',
                'string',
                {'type': 'record', 'name': 'Cons',
                 'fields': [
                     {'name': 'car', 'type': 'Lisp'},
                     {'name': 'cdr', 'type': 'Lisp'},
                 ]},
            ],
        }],
      }, {'value': {'car': {'value': 'head'}, 'cdr': {'value': None}}}),
)

DEFAULT_VALUE_TESTS = (
    # These are pairs: (field type, default value)
    ('null', None),
    ('boolean', True),
    ('string', u'foo'),
    ('bytes', b'\x01\xce\xb6\xcf\x89\xce\xae\xFF'),
    ('int', 1234),
    ('long', 5678),
    ('float', 12.34),
    ('double', 56.78),
    ({'type': 'fixed', 'name': 'F', 'size': 8},
     b'\x01\xce\xb6\xcf\x89\xce\xae\xFF'),
    ({'type': 'enum', 'name': 'F', 'symbols': ['FOO', 'BAR']}, u'FOO'),
    ({'type': 'array', 'items': 'int'}, [123, -456, 789]),
    ({'type': 'map', 'values': 'long'}, {'a': -123, 'b': 456, 'c': -789}),
    (['int', 'null'], 5),
    ({'type': 'record', 'name': 'F', 'fields': [{'name': 'A', 'type': 'int'}]},
     {'A': 5}),
)

PRIMITIVE_TYPE_AS_DICT_TESTS = (
    # These are pairs: (schema, example datum)
    ({'type': 'null'}, None),
    ({'type': 'boolean'}, True),
    ({'type': 'string'}, u'unicode value'),
    ({'type': 'bytes'}, b'\x01\xce\xb6\xcf\x89\xce\xae\xFF'),
    ({'type': 'int'}, 1234),
    ({'type': 'long'}, 5678),
    ({'type': 'float'}, 123.456),
    ({'type': 'double'}, 456.789),
)


# ---- Tests behavior of `fastavro.normalize_schema` -------------------------#

class TestNormalizeSchema(unittest.TestCase):

    examine_exception = _examine_exception

    def setUp(self):
        fastavro.reset_schema_repository()

    def test_normalize_schema_resolves_namespaces(self):
        schema = {
            'namespace': 'test.record',
            'name': 'Record',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': 'string'},
                {'name': 'b', 'type': 'Type'},
                {'name': 'c', 'type': 'test.other.Other'},
            ],
        }
        expected = {
            'namespace': 'test.record',
            'name': 'test.record.Record',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': 'string'},
                {'name': 'b', 'type': 'test.record.Type'},
                {'name': 'c', 'type': 'test.other.Other'},
            ],
        }
        self.assertEqual(fastavro.normalize_schema(schema), expected)

    def test_normalize_schema_resolves_nested_namespaces(self):
        schema = {
            'namespace': 'test.record',
            'name': 'Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': {
                    'name': 'Inner', 'type': 'record',
                    'fields': [
                        {'name': 'Field', 'type': 'string'},
                    ],
                }},
                {'name': 'b', 'type': 'Inner'},
                {'name': 'c', 'type': 'test.record.Inner'},
                {'name': 'd', 'type': 'test.other.Type'},
            ],
        }
        expected = {
            'namespace': 'test.record',
            'name': 'test.record.Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': {
                    'name': 'test.record.Inner', 'type': 'record',
                    'fields': [
                        {'name': 'Field', 'type': 'string'},
                    ],
                }},
                {'name': 'b', 'type': 'test.record.Inner'},
                {'name': 'c', 'type': 'test.record.Inner'},
                {'name': 'd', 'type': 'test.other.Type'},
            ],
        }
        self.assertEqual(fastavro.normalize_schema(schema), expected)

    def test_normalize_schema_resolves_nested_namespaces_from_unions(self):
        schema = {
            'namespace': 'test.union',
            'name': 'Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': ['null', {
                    'name': 'Inner', 'type': 'record',
                    'fields': [
                        {'name': 'Field', 'type': 'string'},
                    ],
                }]},
                {'name': 'b', 'type': ['null', 'Inner']},
                {'name': 'c', 'type': 'Inner'},
                {'name': 'd', 'type': 'test.other.Type'},
            ],
        }
        expected = {
            'namespace': 'test.union',
            'name': 'test.union.Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': ['null', {
                    'name': 'test.union.Inner', 'type': 'record',
                    'fields': [
                        {'name': 'Field', 'type': 'string'},
                    ],
                }]},
                {'name': 'b', 'type': ['null', 'test.union.Inner']},
                {'name': 'c', 'type': 'test.union.Inner'},
                {'name': 'd', 'type': 'test.other.Type'},
            ],
        }
        self.assertEqual(fastavro.normalize_schema(schema), expected)

    def test_normalize_schema_resolves_nested_namespaces_from_arrays(self):
        schema = {
            'namespace': 'test.array',
            'name': 'Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': {
                    'type': 'array', 'items': {
                        'name': 'Nested', 'type': 'record',
                        'fields': [
                            {'name': 'Field', 'type': 'string'},
                        ],
                    },
                }},
                {'name': 'b', 'type': {
                    'type': 'array', 'items': 'Nested',
                }},
                {'name': 'c', 'type': 'Nested'},
                {'name': 'd', 'type': 'test.array.Nested'},
                {'name': 'e', 'type': 'test.other.Type'},
            ],
        }
        expected = {
            'namespace': 'test.array',
            'name': 'test.array.Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': {
                    'type': 'array', 'items': {
                        'name': 'test.array.Nested', 'type': 'record',
                        'fields': [
                            {'name': 'Field', 'type': 'string'},
                        ],
                    },
                }},
                {'name': 'b', 'type': {
                    'type': 'array', 'items': 'test.array.Nested',
                }},
                {'name': 'c', 'type': 'test.array.Nested'},
                {'name': 'd', 'type': 'test.array.Nested'},
                {'name': 'e', 'type': 'test.other.Type'},
            ],
        }
        self.assertEqual(fastavro.normalize_schema(schema), expected)

    def test_normalize_schema_accepts_primitives_as_dicts(self):
        new_schema = fastavro.normalize_schema({'type': 'string'})
        self.assertEqual(new_schema, 'string')

        new_schema = fastavro.normalize_schema({
            'type': 'array',
            'items': {'type': 'int'},
        })
        self.assertEqual(new_schema, {
            'type': 'array',
            'items': 'int',
        })

    def test_normalize_schema_rejects_dicts_without_type(self):
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({})
        self.examine_exception(
            err.exception, "requires a 'type' attribute", quote=False
        )

        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({'name': 'test', 'values': 'int'})
        self.examine_exception(
            err.exception, "requires a 'type' attribute", quote=False
        )

    def test_acquaint_rejects_dicts_with_unknown_type(self):
        with self.assertRaises(InvalidTypeError) as err:
            fastavro.normalize_schema({'type': 'invalid'})
        self.examine_exception(
            err.exception, "Unknown schema type 'invalid'", quote=False
        )

        with self.assertRaises(InvalidTypeError) as err:
            fastavro.normalize_schema({'type': 'union'})
        self.examine_exception(
            err.exception, "Invalid type 'union'", quote=False
        )

        with self.assertRaises(InvalidTypeError) as err:
            fastavro.normalize_schema({
                'type': 'array',
                'items': {'type': 'invalid'},
            })
        self.examine_exception(
            err.exception, "Unknown schema type 'invalid'", quote=False
        )

    def test_normalize_schema_rejects_record_without_name(self):
        # 'record' is a 'Named' type. Per the Avro specification, the 'name'
        # attribute is required.
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'record',
                'fields': [{'name': 'test', 'type': 'int'}],
            })
        self.examine_exception(err.exception, ('record', 'name'))

    def test_normalize_schema_rejects_record_without_fields(self):
        # Per the Avro specification, the 'fields' attribute is required for
        # the 'record' type
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'record',
                'name': 'record_test',
            })
        self.examine_exception(err.exception, ('record', 'fields'))

        # However an empty list is OK
        fastavro.normalize_schema({
            'type': 'record',
            'name': 'record_test',
            'fields': [],
        })

    def test_normalize_schema_rejects_record_fields_without_name(self):
        # Per the Avro specification, each `field` in a 'record' requires a
        # 'name' attribute.
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'record',
                'name': 'record_test',
                'fields': [
                    {'name': 'test', 'type': 'string'},
                    {'type': 'int'},
                ]
            })
        self.examine_exception(err.exception, ('record.fields', 'name'))

    def test_normalize_schema_rejects_record_fields_without_type(self):
        # Per the Avro specification, each `field` in a 'record' requires a
        # 'type' attribute.
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'record',
                'name': 'record_test',
                'fields': [{'name': 'test'}],
            })
        self.examine_exception(err.exception, ('record.fields', 'type'))

    def test_normalize_schema_rejects_enum_without_name(self):
        # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
        # attribute is required.
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'enum',
                'symbols': ['foo', 'bar', 'baz'],
            })
        self.examine_exception(err.exception, ('enum', 'name'))

    def test_normalize_schema_rejects_enum_without_symbols(self):
        # Per the Avro specification, the 'symbols' attribute is required for
        # the 'enum' type
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'enum',
                'name': 'enum_test',
            })
        self.examine_exception(err.exception, ('enum', 'symbols'))

        # However an empty list is OK (maybe it should be rejected?)
        fastavro.normalize_schema({
            'type': 'enum',
            'name': 'enum_test',
            'symbols': [],
        })

    def test_normalize_schema_rejects_fixed_without_name(self):
        # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
        # attribute is required.
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'fixed',
                'size': 32,
            })
        self.examine_exception(err.exception, ('fixed', 'name'))

    def test_normalize_schema_rejects_fixed_without_size(self):
        # Per the Avro specification, the 'size' attribute is required for
        # the 'fixed' type
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'fixed',
                'name': 'fixed_test',
            })
        self.examine_exception(err.exception, ('fixed', 'size'))

        # However a zero 'size' is OK
        fastavro.normalize_schema({
            'type': 'fixed',
            'name': 'fixed_test',
            'size': 0
        })

    def test_normalize_schema_rejects_array_without_items(self):
        # Per the Avro specification, the 'items' attribute is required for
        # the 'array' type
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'array',
            })
        self.examine_exception(err.exception, ('array', 'items'))

        # Rejects None, '', {}
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'array', 'values': None,
            })
        self.examine_exception(err.exception, ('array', 'items'))

        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'array', 'values': '',
            })
        self.examine_exception(err.exception, ('array', 'items'))

        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'array', 'values': {},
            })
        self.examine_exception(err.exception, ('array', 'items'))

        # However an empty list is OK
        fastavro.normalize_schema({
            'type': 'array',
            'items': [],
        })

    def test_normalize_schema_rejects_map_without_values(self):
        # Per the Avro specification, the 'values' attribute is required for
        # the 'map' type
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'map',
            })
        self.examine_exception(err.exception, ('map', 'values'))

        # Rejects None, '', {}
        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'map', 'values': None,
            })
        self.examine_exception(err.exception, ('map', 'values'))

        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'map', 'values': '',
            })
        self.examine_exception(err.exception, ('map', 'values'))

        with self.assertRaises(SchemaAttributeError) as err:
            fastavro.normalize_schema({
                'type': 'map', 'values': {},
            })
        self.examine_exception(err.exception, ('map', 'values'))

        # However an empty list is OK
        fastavro.normalize_schema({
            'type': 'map',
            'values': [],
        })


# ---- Tests behavior of `fastavro.acquaint_schema` --------------------------#

class TestAcquaintSchema(unittest.TestCase):

    examine_exception = _examine_exception

    def setUp(self):
        fastavro.reset_schema_repository()

    def test_acquaint_schema_examples(self):
        # For un-named schema types, this only tests that the example schema
        # does not raise an exception. For 'named' schema types, also assert
        # that the schema was added to the `SCHEMA_DEFS` dict in both the
        # `_writer` and `_reader` modules.
        for schema, _ in EXAMPLE_SCHEMAS:
            with self.subTest():
                fastavro.acquaint_schema(schema)
                if isinstance(schema, dict) and 'name' in schema:
                    schema_defs = fastavro._writer.get_schema_defs()
                    self.assertIn(schema['name'], schema_defs)

    def test_acquaint_schema_rejects_undeclared_name(self):
        with self.assertRaises(UnknownTypeError) as err:
            fastavro.acquaint_schema({
                'name': 'schema_test',
                'type': 'record',
                'fields': [
                    {'name': 'left', 'type': 'Thinger'},
                ],
            })
        self.assertEqual(err.exception.schema, 'Thinger')

    def test_acquaint_schema_rejects_unordered_references(self):
        with self.assertRaises(UnknownTypeError) as err:
            fastavro.acquaint_schema({
                'name': 'schema_test',
                'type': 'record',
                'fields': [
                    {'name': 'left', 'type': 'Thinger'},
                    {'name': 'right', 'type': {
                        'name': 'Thinger',
                        'type': 'record',
                        'fields': [
                            {'name': 'the_thing', 'type': 'string'},
                        ],
                    }},
                ],
            })
        self.assertEqual(err.exception.schema, 'Thinger')

    def test_acquaint_schema_handles_nested_namespaces(self):
        schema = {
            'namespace': 'test.record',
            'name': 'Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': {
                    'name': 'Inner', 'type': 'record',
                    'fields': [
                        {'name': 'Field', 'type': 'string'},
                    ],
                }},
                {'name': 'b', 'type': 'Inner'},
                {'name': 'c', 'type': 'test.record.Inner'},
            ],
        }
        fastavro.acquaint_schema(schema)
        schema_defs = fastavro._writer.get_schema_defs()
        self.assertIn('test.record.Outer', schema_defs)
        self.assertIn('test.record.Inner', schema_defs)
        b_schema = schema_defs['test.record.Outer']['fields'][1]
        self.assertEqual(b_schema['type'], 'test.record.Inner')
        c_schema = schema_defs['test.record.Outer']['fields'][2]
        self.assertEqual(c_schema['type'], 'test.record.Inner')

    def test_acquaint_schema_handles_nested_namespaces_from_unions(self):
        schema = {
            'namespace': 'test.union',
            'name': 'Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': ['null', {
                    'name': 'Inner', 'type': 'record',
                    'fields': [
                        {'name': 'Field', 'type': 'string'},
                    ],
                }]},
                {'name': 'b', 'type': ['null', 'Inner']},
                {'name': 'c', 'type': ['test.union.Inner', 'int', 'null']},
            ],
        }
        fastavro.acquaint_schema(schema)
        schema_defs = fastavro._writer.get_schema_defs()
        self.assertIn('test.union.Outer', schema_defs)
        self.assertIn('test.union.Inner', schema_defs)
        b_schema = schema_defs['test.union.Outer']['fields'][1]
        self.assertEqual(b_schema['type'][1], 'test.union.Inner')
        c_schema = schema_defs['test.union.Outer']['fields'][2]
        self.assertEqual(c_schema['type'][0], 'test.union.Inner')

    def test_acquaint_schema_handles_nested_namespaces_from_arrays(self):
        schema = {
            'namespace': 'test.array',
            'name': 'Outer',
            'type': 'record',
            'fields': [
                {'name': 'a', 'type': {
                    'type': 'array', 'items': {
                        'name': 'Nested', 'type': 'record',
                        'fields': [
                            {'name': 'Field', 'type': 'string'},
                        ],
                    },
                }},
                {'name': 'b', 'type': {
                    'type': 'array', 'items': 'Nested',
                }},
                {'name': 'c', 'type': 'Nested'},
            ],
        }
        fastavro.acquaint_schema(schema)
        schema_defs = fastavro._writer.get_schema_defs()
        self.assertIn('test.array.Outer', schema_defs)
        self.assertIn('test.array.Nested', schema_defs)
        b_schema = schema_defs['test.array.Outer']['fields'][1]
        self.assertEqual(b_schema['type']['items'], 'test.array.Nested')
        c_schema = schema_defs['test.array.Outer']['fields'][2]
        self.assertEqual(c_schema['type'], 'test.array.Nested')


# ---- Tests migration from a "writer's schema" to a "reader's schema" -------#

class TestSchemaMigration(unittest.TestCase):

    def setUp(self):
        fastavro.reset_schema_repository()

    def test_schema_migration_projection(self):
        # This test is adapted from the Apache Avro 1.8.1 Python unit tests:
        #   `test_projection()`  in  /lang/py/test/test_io.py
        writers_schema = LONG_RECORD_SCHEMA
        readers_schema = {
            'name': 'Test',
            'type': 'record',
            'fields': [
                {'name': 'E', 'type': 'int'},
                {'name': 'F', 'type': 'int'},
            ]
        }
        records = [LONG_RECORD_DATUM]
        expected = [{'E': 5, 'F': 6}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, expected)

    def test_schema_migration_remove_field(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string'},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        records = [{'test': 'test'}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, [{}])

    def test_schema_migration_add_default_field(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string', 'default': 'default'},
            ],
        }
        records = [{}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, [{'test': 'default'}])

    def test_schema_migration_string_type_record_add_falsy_default(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string', 'default': ''},
            ],
        }
        records = [{}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, [{'test': ''}])

    def test_schema_migration_null_type_record_add_falsy_default(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string', 'default': None},
            ],
        }
        records = [{}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, [{'test': None}])

    def test_schema_migration_boolean_type_record_add_falsy_default(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string', 'default': False},
            ],
        }
        records = [{}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, [{'test': False}])

    def test_schema_migration_int_type_record_add_falsy_default(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string', 'default': 0},
            ],
        }
        records = [{}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, [{'test': 0}])

    def test_schema_migration_float_type_record_add_falsy_default(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string', 'default': 0.0},
            ],
        }
        records = [{}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, [{'test': 0.0}])

    def test_schema_migration_numeric_promotions(self):
        # This test is adapted from the Apache Avro 1.8.1 Python unit tests:
        #   `test_schema_promotion()`  in  /lang/py/test/test_io.py
        datum = 219
        promotable = ['int', 'long', 'float', 'double']
        test_cases = [
            (ws, rs) for i, ws in enumerate(promotable)
            for rs in promotable[i + 1:]
        ]
        for writers_schema, readers_schema in test_cases:
            with self.subTest():
                output = _write(writers_schema, [datum])
                new_records = _read(output, readers_schema)
                value = new_records[0]
                if isinstance(datum, float):
                    self.assertAlmostEqual(value, datum, places=5)
                else:
                    self.assertEqual(value, datum)

    def test_schema_migration_union_int_to_float_promotion(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['string', 'int']},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['float', 'string']},
            ],
        }
        records = [{'test': 1}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, records)

    def test_schema_migration_union_bytes_to_string_promotion(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['int', 'bytes', 'string']},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['int', 'string']},
            ],
        }
        byte_str = b'byte_str'
        records = [{'test': byte_str}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertIsInstance(new_records[0]['test'], _unicode_type)
        self.assertEqual(new_records[0]['test'], byte_str.decode('utf-8'))

    def test_schema_migration_union_string_to_bytes_promotion(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['int', 'bytes', 'string']},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['int', 'bytes']},
            ],
        }
        unicode_str = u'unicode_str'
        records = [{'test': unicode_str}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertIsInstance(new_records[0]['test'], _bytes_type)
        self.assertEqual(new_records[0]['test'], unicode_str.encode('utf-8'))

    def test_schema_migration_maps_with_union_promotion(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'map',
                    'values': ['string', 'int'],
                },
            }],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'map',
                    'values': ['string', 'long'],
                },
            }],
        }
        records = [{'test': {'foo': 1}}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, records)

    def test_schema_migration_array_with_union_promotion(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'array',
                    'items': ['boolean', 'long'],
                },
            }],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'array',
                    'items': ['string', 'float'],
                },
            }],
        }
        records = [{'test': [1, 2, 3]}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, records)

    def test_schema_migration_writer_union(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['string', 'int']},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'int'},
            ],
        }
        records = [{'test': 1}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, records)

    def test_schema_migration_reader_union(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'int'},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['string', 'int']},
            ],
        }
        records = [{'test': 1}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, records)

    def test_schema_migration_writer_and_reader_union(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['double', 'string', 'null'],
            }],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['int', 'long', 'float', 'string'],
            }],
        }
        records = [{'test': u'foo'}]

        output = _write(writers_schema, records)
        new_records = _read(output, readers_schema)
        self.assertEqual(new_records, records)

    def test_schema_migration_reader_union_failure(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'boolean'},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['string', 'int']},
            ],
        }
        records = [{'test': True}]

        output = _write(writers_schema, records)
        with self.assertRaises(SchemaResolutionError):
            _read(output, readers_schema)

    def test_schema_migration_writer_union_failure(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['boolean', 'string']},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'int'
            }],
        }
        records = [{'test': u'foo'}]

        output = _write(writers_schema, records)
        with self.assertRaises(SchemaResolutionError):
            _read(output, readers_schema)

    def test_schema_migration_array_failure(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'array',
                    'items': ['string', 'int'],
                },
            }],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'array',
                    'items': ['string', 'boolean'],
                },
            }],
        }
        records = [{'test': [1, 2, 3]}]

        output = _write(writers_schema, records)
        with self.assertRaises(SchemaResolutionError):
            _read(output, readers_schema)

    def test_schema_migration_maps_failure(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'map',
                    'values': 'string',
                },
            }],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': {
                    'type': 'map',
                    'values': 'long',
                },
            }],
        }
        records = [{'test': {'foo': 'a'}}]

        output = _write(writers_schema, records)
        with self.assertRaises(SchemaResolutionError):
            _read(output, readers_schema)

    def test_schema_migration_enum_failure(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'enum',
            'symbols': ['FOO', 'BAR'],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'enum',
            'symbols': ['BAZ', 'BAR'],
        }
        records = ['FOO']

        output = _write(writers_schema, records)
        with self.assertRaises(SchemaResolutionError):
            _read(output, readers_schema)

    def test_schema_migration_schema_mismatch(self):
        writers_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string'},
            ],
        }
        readers_schema = {
            'name': 'migration_test',
            'type': 'enum',
            'symbols': ['FOO', 'BAR'],
        }
        records = [{'test': 'test'}]

        output = _write(writers_schema, records)
        with self.assertRaises(SchemaResolutionError):
            _read(output, readers_schema)


# ---- General tests ---------------------------------------------------------#

class TestFastavro(unittest.TestCase):

    def setUp(self):
        fastavro.reset_schema_repository()

    def test_not_avro(self):
        with self.assertRaises(ReadError):
            with open(__file__, 'rb') as input:
                fastavro.Reader(input)

    def test_empty(self):
        io = BytesIO()
        schema = {
            'name': 'test',
            'type': 'record',
            'fields': [
                {'type': 'boolean', 'name': 'a'},
            ],
        }
        with self.assertRaises(ReadError):
            fastavro.load(io, schema)

    def test_metadata(self):
        schema = {
            'name': 'metadata_test',
            'type': 'record',
            'fields': [],
        }
        records = [{}]
        metadata = {'key': 'value'}

        output = _write(schema, records, metadata=metadata)
        reader = fastavro.Reader(output)
        self.assertEqual(reader.metadata['key'], metadata['key'])

    def test_repo_caching_issue(self):
        schema = {
            'name': 'B',
            'type': 'record',
            'fields': [{
                'name': 'b',
                'type': {
                    'name': 'C',
                    'type': 'record',
                    'fields': [{
                        'name': 'c',
                        'type': 'string',
                    }],
                },
            }],
        }
        records = [{'b': {'c': 'test'}}]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

        other_schema = {
            'name': 'A',
            'type': 'record',
            'fields': [{
                'name': 'a',
                'type': {
                    'name': 'B',
                    'type': 'record',
                    'fields': [{
                        'name': 'b',
                        'type': {
                            'name': 'C',
                            'type': 'record',
                            'fields': [{
                                'name': 'c',
                                'type': 'int',
                            }],
                        },
                    }],
                },
            }, {
                'name': 'aa',
                'type': 'B',
            }],
        }
        records = [{'a': {'b': {'c': 1}}, 'aa': {'b': {'c': 2}}}]

        output = _write(other_schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_primitve_type_as_dict(self):
        for schema, datum in PRIMITIVE_TYPE_AS_DICT_TESTS:
            with self.subTest():
                output = _write(schema, [datum])
                reader = fastavro.Reader(output)
                new_records = list(reader)
                self.assertEqual(len(new_records), 1)
                if isinstance(datum, float):
                    self.assertAlmostEqual(new_records[0], datum, places=5)
                else:
                    self.assertEqual(new_records[0], datum)
                # Schema has been normalized:
                self.assertEqual(reader.schema, schema['type'])

    def test_example_round_trips(self):
        # Uses the `EXAMPLE_SCHEMAS` extracted from the Apache Avro 1.8.1
        # Python unit tests:  /lang/py/test/test_io.py
        for schema, datum in EXAMPLE_SCHEMAS:
            with self.subTest():
                output = _write(schema, [datum])
                reader = fastavro.Reader(output)
                new_records = list(reader)
                self.assertEqual(len(new_records), 1)
                if isinstance(datum, float):
                    self.assertAlmostEqual(new_records[0], datum, places=5)
                else:
                    self.assertEqual(new_records[0], datum)
                self.assertEqual(reader.schema, schema)

    def test_boolean_roundtrip(self):
        schema = {
            'name': 'boolean_test',
            'type': 'record',
            'fields': [
                {'name': 'field', 'type': 'boolean'},
            ],
        }
        records = [
            {'field': True},
            {'field': False},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_fixed_roundtrip(self):
        schema = {
            'name': 'fixed_test',
            'type': 'record',
            'fields': [{
                'name': 'magic',
                'type': {'type': 'fixed', 'name': 'Magic', 'size': 4},
            }, {
                'name': 'stuff',
                'type': {'type': 'fixed', 'name': 'Stuff', 'size': 8},
            }],
        }
        records = [
            {'magic': b'1234', 'stuff': random_byte_str(8)},
            {'magic': b'xxxx', 'stuff': random_byte_str(8)},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_bytes_roundtrip(self):
        schema = {
            'name': 'bytes_test',
            'type': 'record',
            'fields': [
                {'name': 'test1', 'type': 'bytes'},
                {'name': 'test2', 'type': 'bytes'},
            ],
        }
        records = [
            {'test1': b'foobar', 'test2': random_byte_str(8)},
            {'test1': b'bizbaz', 'test2': random_byte_str(8)},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_string_roundtrip(self):
        schema = {
            'name': 'string_test',
            'type': 'record',
            'fields': [
                {'name': 'test1', 'type': 'string'},
                {'name': 'test2', 'type': 'string'},
            ],
        }
        records = [
            {'test1': u'foobar', 'test2': random_unicode_str(20)},
            {'test1': u'bizbaz', 'test2': random_unicode_str(20)},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_string_with_non_unicode_values_roundtrip(self):
        schema = {
            'name': 'string_test',
            'type': 'record',
            'fields': [
                {'name': 'test1', 'type': 'string'},
                {'name': 'test2', 'type': 'string'},
            ],
        }
        records = [{
            'test1': b'Obviously safe ascii string',
            # UTF-8 encoded Cyrillic chars
            'test2': b'\xd0\xb1\xd1\x8b\xd1\x81\xd1\x82\xd1\x80\xd1\x8b\xd0\xb9',  # noqa
        }, {
            'test1': b'Not\x09Obviously\x0AUTF-8 Safe',
            # UTF-8 encoded Greek chars
            'test2': b'\xce\xb3\xcf\x81\xce\xae\xce\xb3\xce\xbf\xcf\x81\xce\xbf\xcf\x82',  # noqa
        }]

        output = _write(schema, records)

        # Decode binary strings for result comparison
        for r, k, v in ((r, k, v) for r in records for k, v in r.items()):
            r[k] = v.decode('utf-8')

        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_float_roundtrip(self):
        schema = {
            'name': 'float_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'float'},
            ],
        }
        records = [
            {'test': 3.141592653589793},
            {'test': 2.718281828459045},
            {'test': 3141.592653589793},
            {'test': 2718.281828459045},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        for n, r in zip(new_records, records):
            # The `places=3` arg is a bit arbitrary I guess
            self.assertAlmostEqual(n['test'], r['test'], places=3)

    def test_double_roundtrip(self):
        schema = {
            'name': 'double_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'double'},
            ],
        }
        records = [
            {'test': 3.141592653589793},
            {'test': 2.718281828459045},
            {'test': 31415926.53589793},
            {'test': 27182818.28459045},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        for n, r in zip(new_records, records):
            # The `places=6` arg is a bit arbitrary I guess
            self.assertAlmostEqual(n['test'], r['test'], places=6)

    def test_int_roundtrip(self):
        int_min_value = -(1 << 31)
        int_max_value = (1 << 31) - 1
        schema = {
            'name': 'int_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'int'},
            ],
        }
        records = [
            {'test': int_min_value},
            {'test': int_min_value + 1},
            {'test': int_max_value},
            {'test': int_max_value - 1},
            {'test': 1},
            {'test': -1},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_long_roundtrip(self):
        long_min_value = -(1 << 63)
        long_max_value = (1 << 63) - 1
        schema = {
            'name': 'int_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'long'},
            ],
        }
        records = [
            {'test': long_min_value},
            {'test': long_min_value + 1},
            {'test': long_max_value},
            {'test': long_max_value - 1},
            {'test': 1},
            {'test': -1},
        ]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, records)

    def test_empty_record_default_values(self):
        # This test is adapted from the Apache Avro 1.8.1 Python unit tests:
        #   `test_default_value()`  in  /lang/py/test/test_io.py
        for type_, default in DEFAULT_VALUE_TESTS:
            with self.subTest():
                schema = {
                    'name': 'Test',
                    'type': 'record',
                    'fields': [{
                        'name': 'Field', 'type': type_, 'default': default,
                    }],
                }
                records = [{}]

                output = _write(schema, records)
                new_records = _read(output)
                self.assertEqual(len(new_records), 1)
                self.assertIn('Field', new_records[0])
                value = new_records[0]['Field']
                if isinstance(default, float):
                    self.assertAlmostEqual(value, default, places=5)
                else:
                    self.assertEqual(value, default)

    def test_record_default_values(self):
        # This test is adapted from the Apache Avro 1.8.1 Python unit tests:
        #   `test_default_value()`  in  /lang/py/test/test_io.py
        writers_schema = LONG_RECORD_SCHEMA

        for type_, default in DEFAULT_VALUE_TESTS:
            with self.subTest():
                readers_schema = {
                    'name': 'Test',
                    'type': 'record',
                    'fields': [
                        {'name': 'H', 'type': type_, 'default': default},
                    ]
                }
                records = [LONG_RECORD_DATUM]

                output = _write(writers_schema, records)
                new_records = _read(output, readers_schema)
                self.assertEqual(len(new_records), 1)
                self.assertIn('H', new_records[0])
                value = new_records[0]['H']
                if isinstance(default, float):
                    self.assertAlmostEqual(value, default, places=5)
                else:
                    self.assertEqual(value, default)

    def test_string_type_record_with_falsy_default_value(self):
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string', 'default': u''},
            ],
        }
        records = [{}]
        expected = [{'test': u''}]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, expected)

    def test_boolean_type_record_with_falsy_default_value(self):
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'boolean', 'default': False},
            ],
        }
        records = [{}]
        expected = [{'test': False}]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, expected)

    def test_int_type_record_with_falsy_default_value(self):
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'int', 'default': 0},
            ],
        }
        records = [{}]
        expected = [{'test': 0}]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, expected)

    def test_float_type_record_with_falsy_default_value(self):
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'float', 'default': 0.0},
            ],
        }
        records = [{}]
        expected = [{'test': 0}]

        output = _write(schema, records)
        new_records = _read(output)
        self.assertEqual(new_records, expected)

    def test_missing_value_for_string_type_record_with_no_default(self):
        # See: https://github.com/tebeka/fastavro/issues/49
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'string'},
            ],
        }
        records = [{}]

        with self.assertRaises(TypeError):
            _write(schema, records)

    def test_missing_value_for_bool_type_record_with_no_default(self):
        # See: https://github.com/tebeka/fastavro/issues/49
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': 'boolean'},
            ],
        }
        records = [{}]

        with self.assertRaises(TypeError):
            _write(schema, records)

    def test_missing_value_for_null_union_type_record_with_no_default(self):
        # See: https://github.com/tebeka/fastavro/issues/49
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [
                {'name': 'test', 'type': ['int', 'null']},
            ],
        }
        records = [{}]

        with self.assertRaises(ValueError):
            _write(schema, records)


# ---- Tests `schemaless_writer` and `schemaless_reader` ---------------------#

class TestSchemalessWriterAndReader(unittest.TestCase):

    def setUp(self):
        fastavro.reset_schema_repository()

    def test_schemaless_writer_and_reader(self):
        schema = {
            'namespace': 'test',
            'name': 'Test',
            'type': 'record',
            'fields': [
                {'name': 'field', 'type': {'type': 'string'}},
            ],
        }
        record = {'field': 'test'}

        output = BytesIO()
        fastavro.schemaless_writer(output, schema, record)
        output.seek(0)

        new_record = fastavro.schemaless_reader(output, schema)
        self.assertEqual(record, new_record)


if __name__ == '__main__':
    verbosity = 2
    defaultTest = None
    # To run a single function or class, you can set `defaultTest`:
    # defaultTest = 'TestNormalizeSchema'

    unittest.main(
        verbosity=verbosity, defaultTest=defaultTest, catchbreak=True,
    )
