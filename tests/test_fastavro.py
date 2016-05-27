# -*- coding: utf-8 -*-
"""Tests for fastavro"""

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
from fastavro import ReadError, UnknownType, SchemaResolutionError, SchemaError

from tests.utils import (
    random_byte_str, random_unicode_str, _unicode_type, _bytes_type,
)


data_dir = join(abspath(dirname(__file__)), 'avro-files')

NO_DATA = set([
    'class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro',
    'testDataFileMeta.avro',
])


class NoSeekBytesIO(object):
    """Shim around BytesIO which blocks access to everything but read.
    Used to ensure seek API isn't being depended on."""

    def __init__(self, *args):
        self.underlying = BytesIO(*args)

    def read(self, n):
        return self.underlying.read(n)

    def seek(self, *args):
        raise AssertionError("fastavro reader should not depend on seek")

    def close(self):
        self.underlying.close()


class TestReadWrite(unittest.TestCase):

    def read_write_file(self, filename):
        with open(filename, 'rb') as input:
            reader = fastavro.Reader(input)
            self.assertTrue(hasattr(reader, 'schema'), 'Failed to read schema')
            if basename(filename) in NO_DATA:
                return
            records = list(reader)
            self.assertGreaterEqual(len(records), 0, 'No records found')

        write_buffer = BytesIO()
        fastavro.write(write_buffer, reader.schema, records, reader.codec)
        serialized_data = write_buffer.getvalue()
        write_buffer.close()

        # Read back output from `fastavro.write`
        read_buffer = BytesIO(serialized_data)
        output_reader = fastavro.Reader(read_buffer)
        self.assertTrue(hasattr(output_reader, 'schema'),
                        'Schema was not written')
        self.assertEqual(output_reader.schema, reader.schema)
        self.assertEqual(output_reader.codec, reader.codec)
        new_records = list(output_reader)
        self.assertEqual(new_records, records)
        read_buffer.close()

        # Test schema migration with the same schema
        read_buffer = BytesIO(serialized_data)
        migration_reader = fastavro.Reader(read_buffer, reader.schema)
        self.assertTrue(hasattr(migration_reader, 'schema'),
                        'Schema was not written')
        self.assertEqual(migration_reader.reader_schema, reader.schema)
        self.assertEqual(output_reader.codec, reader.codec)
        new_records = list(migration_reader)
        self.assertEqual(new_records, records)
        read_buffer.close()

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


def setup_read_write():
    filenames = iglob(join(data_dir, '*.avro'))
    for idx, filename in enumerate(sorted(filenames)):
        if 'snappy' in filename and not snappy:
            continue

        def wrapper(self):
            with self.subTest('Test file: %s' % filename):
                self.read_write_file(filename)

        name, ext = basename(filename).rsplit('.', 1)
        name = re.sub(r'\s|-|\.', '_', name)
        func_name = 'test_file_%s' % name
        wrapper.__name__ = func_name
        setattr(TestReadWrite, func_name, wrapper)

setup_read_write()


class TestAcquaintSchema(unittest.TestCase):

    def check_exception(self, exc, substrs):
        substrs = substrs if isinstance(substrs, (list, tuple)) else (substrs,)
        self.assertTrue(
            all("'%s'" % s in str(exc) for s in substrs),
            'Incorrect exception raised: %s' % repr(exc)
        )

    def test_acquaint_schema_rejects_undeclared_name(self):
        with self.assertRaises(UnknownType) as err:
            fastavro.acquaint_schema({
                'name': 'schema_test',
                'type': 'record',
                'fields': [{
                    'name': 'left',
                    'type': 'Thinger',
                }],
            })
        self.assertEqual(err.exception.name, 'Thinger')

    def test_acquaint_schema_rejects_unordered_references(self):
        with self.assertRaises(UnknownType) as err:
            fastavro.acquaint_schema({
                'name': 'schema_test',
                'type': 'record',
                'fields': [{
                    'name': 'left',
                    'type': 'Thinger',
                }, {
                    'name': 'right',
                    'type': {
                        'name': 'Thinger',
                        'type': 'record',
                        'fields': [{
                            'name': 'the_thing',
                            'type': 'string',
                        }],
                    },
                }],
            })
        self.assertEqual(err.exception.name, 'Thinger')

    def test_acquaint_schema_accepts_nested_namespaces(self):
        fastavro.acquaint_schema({
            'namespace': 'com.example',
            'name': 'Outer',
            'type': 'record',
            'fields': [{
                'name': 'a',
                'type': {
                    'name': 'Inner',
                    'type': 'record',
                    'fields': [{
                        'name': 'the_thing',
                        'type': 'string',
                    }],
                },
            }, {
                'name': 'b',
                # This should resolve to com.example.Inner because of the
                # `namespace` of the enclosing record.
                'type': 'Inner',
            }, {
                'name': 'b',
                'type': 'com.example.Inner',
            }],
        })
        self.assertIn('com.example.Inner', fastavro._writer.get_schema_defs())

    def test_acquaint_schema_resolves_references_from_unions(self):
        fastavro.acquaint_schema({
            'namespace': 'com.other',
            'name': 'Outer',
            'type': 'record',
            'fields': [{
                'name': 'a',
                'type': ['null', {
                    'name': 'Inner',
                    'type': 'record',
                    'fields': [{
                        'name': 'the_thing',
                        'type': 'string',
                    }],
                }],
            }, {
                'name': 'b',
                # This should resolve to com.example.Inner because of the
                # `namespace` of the enclosing record.
                'type': ['null', 'Inner'],
            }],
        })
        schema_defs = fastavro._writer.get_schema_defs()
        b_schema = schema_defs['com.other.Outer']['fields'][1]
        self.assertEqual(b_schema['type'][1], 'com.other.Inner')

    def test_acquaint_schema_accepts_nested_records_from_arrays(self):
        fastavro.acquaint_schema({
            'name': 'Outer',
            'type': 'record',
            'fields': [{
                'name': 'multiple',
                'type': {
                    'type': 'array',
                    'items': {
                        'name': 'Nested',
                        'type': 'record',
                        'fields': [{'type': 'string', 'name': 'text'}],
                    },
                },
            }, {
                'name': 'single',
                'type': {
                    'type': 'array',
                    'items': 'Nested',
                },
            }],
        })
        self.assertIn('Nested', fastavro._writer.get_schema_defs())

    def test_acquaint_schema_rejects_record_without_name(self):
        # 'record' is a 'Named' type. Per the Avro specification, the 'name'
        # attribute is required.
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'record',
                'fields': [{'name': 'test', 'type': 'int'}],
            })
        self.check_exception(err.exception, ('record', 'name'))

    def test_acquaint_schema_rejects_record_without_fields(self):
        # Per the Avro specification, the 'fields' attribute is required for
        # the 'record' type
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'record',
                'name': 'record_test',
            })
        self.check_exception(err.exception, ('record', 'fields'))

        # However an empty list is OK
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
            'fields': [],
        })

    def test_acquaint_schema_rejects_record_fields_without_name(self):
        # Per the Avro specification, each `field` in a 'record' requires a
        # 'name' attribute.
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'record',
                'name': 'record_test',
                'fields': [
                    {'name': 'test', 'type': 'string'},
                    {'type': 'int'},
                ]
            })
        self.check_exception(err.exception, ('record', 'field', 'name'))

    def test_acquaint_schema_rejects_record_fields_without_type(self):
        # Per the Avro specification, each `field` in a 'record' requires a
        # 'type' attribute.
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'record',
                'name': 'record_test',
                'fields': [{'name': 'test'}],
            })
        self.check_exception(err.exception, ('record', 'field', 'type'))

    def test_acquaint_schema_rejects_enum_without_name(self):
        # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
        # attribute is required.
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'enum',
                'symbols': ['foo', 'bar', 'baz'],
            })
        self.check_exception(err.exception, ('enum', 'name'))

    def test_acquaint_schema_rejects_enum_without_symbols(self):
        # Per the Avro specification, the 'symbols' attribute is required for
        # the 'enum' type
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'enum',
                'name': 'enum_test',
            })
        self.check_exception(err.exception, ('enum', 'symbols'))

        # However an empty list is OK (maybe it should be rejected?)
        fastavro.acquaint_schema({
            'type': 'enum',
            'name': 'enum_test',
            'symbols': [],
        })

    def test_acquaint_schema_rejects_fixed_without_name(self):
        # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
        # attribute is required.
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'fixed',
                'size': 32,
            })
        self.check_exception(err.exception, ('fixed', 'name'))

    def test_acquaint_schema_rejects_fixed_without_size(self):
        # Per the Avro specification, the 'size' attribute is required for
        # the 'fixed' type
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'fixed',
                'name': 'fixed_test',
            })
        self.check_exception(err.exception, ('fixed', 'size'))

        # However a zero 'size' is OK (maybe it should be rejected?)
        fastavro.acquaint_schema({
            'type': 'fixed',
            'name': 'fixed_test',
            'size': 0
        })

    def test_acquaint_schema_rejects_array_without_items(self):
        # Per the Avro specification, the 'items' attribute is required for
        # the 'array' type
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'array',
            })
        self.check_exception(err.exception, ('array', 'items'))

        # However an empty list is OK (maybe it should be rejected?)
        fastavro.acquaint_schema({
            'type': 'array',
            'items': [],
        })

    def test_acquaint_schema_rejects_map_without_values(self):
        # Per the Avro specification, the 'values' attribute is required for
        # the 'map' type
        with self.assertRaises(SchemaError) as err:
            fastavro.acquaint_schema({
                'type': 'map',
            })
        self.check_exception(err.exception, ('map', 'values'))

        # However an empty list is OK (maybe it should be rejected?)
        fastavro.acquaint_schema({
            'type': 'map',
            'values': [],
        })


class TestSchemaMigration(unittest.TestCase):

    def test_schema_migration_remove_field(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'string',
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        records = [{'test': 'test'}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, [{}])

    def test_schema_migration_add_default_field(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'string',
                'default': 'default',
            }],
        }
        records = [{}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, [{'test': 'default'}])

    def test_schema_migration_union_int_to_float_promotion(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['string', 'int'],
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['float', 'string'],
            }],
        }
        records = [{'test': 1}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, records)

    def test_schema_migration_union_bytes_to_string_promotion(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{'name': 'test', 'type': ['int', 'bytes', 'string']}],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{'name': 'test', 'type': ['int', 'string']}],
        }
        byte_str = b'byte_str'
        records = [{'test': byte_str}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertIsInstance(new_records[0]['test'], _unicode_type)
        self.assertEqual(new_records[0]['test'], byte_str.decode('utf-8'))

    def test_schema_migration_union_string_to_bytes_promotion(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{'name': 'test', 'type': ['int', 'bytes', 'string']}],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{'name': 'test', 'type': ['int', 'bytes']}],
        }
        unicode_str = u'unicode_str'
        records = [{'test': unicode_str}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertIsInstance(new_records[0]['test'], _bytes_type)
        self.assertEqual(new_records[0]['test'], unicode_str.encode('utf-8'))

    def test_schema_migration_maps_with_union_promotion(self):
        schema = {
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
        new_schema = {
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, records)

    def test_schema_migration_array_with_union_promotion(self):
        schema = {
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
        new_schema = {
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, records)

    def test_schema_migration_writer_union(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['string', 'int'],
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'int',
            }],
        }
        records = [{'test': 1}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, records)

    def test_schema_migration_reader_union(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'int',
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['string', 'int'],
            }],
        }
        records = [{'test': 1}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, records)

    def test_schema_migration_writer_and_reader_union(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['double', 'string', 'null'],
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['int', 'long', 'float', 'string'],
            }],
        }
        records = [{'test': u'foo'}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        new_records = list(reader)
        self.assertEqual(new_records, records)

    def test_schema_migration_reader_union_failure(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'boolean',
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['string', 'int'],
            }],
        }
        records = [{'test': True}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        with self.assertRaises(SchemaResolutionError):
            list(reader)

    def test_schema_migration_writer_union_failure(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['boolean', 'string']
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'int'
            }],
        }
        records = [{'test': u'foo'}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        with self.assertRaises(SchemaResolutionError):
            list(reader)

    def test_schema_migration_array_failure(self):
        schema = {
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
        new_schema = {
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        with self.assertRaises(SchemaResolutionError):
            list(reader)

    def test_schema_migration_maps_failure(self):
        schema = {
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
        new_schema = {
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        with self.assertRaises(SchemaResolutionError):
            list(reader)

    def test_schema_migration_enum_failure(self):
        schema = {
            'name': 'migration_test',
            'type': 'enum',
            'symbols': ['FOO', 'BAR'],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'enum',
            'symbols': ['BAZ', 'BAR'],
        }
        records = ['FOO']

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        with self.assertRaises(SchemaResolutionError):
            list(reader)

    def test_schema_migration_schema_mismatch(self):
        schema = {
            'name': 'migration_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'string',
            }],
        }
        new_schema = {
            'name': 'migration_test',
            'type': 'enum',
            'symbols': ['FOO', 'BAR'],
        }
        records = [{'test': 'test'}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer, new_schema)
        with self.assertRaises(SchemaResolutionError):
            list(reader)


class TestFastavro(unittest.TestCase):

    def test_metadata(self):
        schema = {
            'name': 'metadata_test',
            'type': 'record',
            'fields': [],
        }
        records = [{}]
        metadata = {'key': 'value'}

        buffer = BytesIO()
        fastavro.write(buffer, schema, records, metadata=metadata)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
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

        buffer = BytesIO()
        fastavro.write(buffer, other_schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
        self.assertEqual(new_records, records)

    def test_boolean_roundtrip(self):
        schema = {
            'name': 'boolean_test',
            'type': 'record',
            'fields': [{
                'name': 'field',
                'type': 'boolean',
            }],
        }
        records = [
            {'field': True},
            {'field': False},
        ]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        # Decode binary strings for result comparison
        for r, k, v in ((r, k, v) for r in records for k, v in r.items()):
            r[k] = v.decode('utf-8')

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
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

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
        for n, r in zip(new_records, records):
            # The `places=6` arg is a bit arbitrary I guess
            self.assertAlmostEqual(n['test'], r['test'], places=6)

    def test_record_default_value(self):
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [{
                'name': 'default_field',
                'type': 'string',
                'default': 'default_value',
            }],
        }
        records = [{}]
        expected = [{'default_field': 'default_value'}]

        buffer = BytesIO()
        fastavro.write(buffer, schema, records)
        buffer.seek(0)

        reader = fastavro.Reader(buffer)
        new_records = list(reader)
        self.assertEqual(new_records, expected)

    def test_missing_value_for_string_type_record_with_no_default(self):
        # See: https://github.com/tebeka/fastavro/issues/49
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': 'string',
            }],
        }
        records = [{}]

        buffer = BytesIO()
        with self.assertRaises(TypeError):
            fastavro.write(buffer, schema, records)

    def test_missing_value_for_bool_type_record_with_no_default(self):
        # See: https://github.com/tebeka/fastavro/issues/49
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [{
                'name': 'test_bool',
                'type': 'boolean',
            }],
        }
        records = [{}]

        buffer = BytesIO()
        with self.assertRaises(TypeError):
            fastavro.write(buffer, schema, records)

    def test_missing_value_for_null_union_type_record_with_no_default(self):
        # See: https://github.com/tebeka/fastavro/issues/49
        schema = {
            'name': 'default_test',
            'type': 'record',
            'fields': [{
                'name': 'test',
                'type': ['int', 'null'],
            }],
        }
        records = [{}]

        buffer = BytesIO()
        with self.assertRaises(ValueError):
            fastavro.write(buffer, schema, records)


class TestSchemalessReadWrite(unittest.TestCase):

    def test_schemaless_writer_and_reader(self):
        schema = {
            'namespace': 'test',
            'name': 'Test',
            'type': 'record',
            'fields': [{
                'name': 'field',
                'type': {'type': 'string'},
            }],
        }
        record = {'field': 'test'}

        buffer = BytesIO()
        fastavro.schemaless_writer(buffer, schema, record)
        buffer.seek(0)

        new_record = fastavro.schemaless_reader(buffer, schema)
        self.assertEqual(record, new_record)


if __name__ == '__main__':
    import nose
    config = nose.config.Config(logStream=sys.stdout, stream=sys.stdout)
    nose.runmodule(config=config)
