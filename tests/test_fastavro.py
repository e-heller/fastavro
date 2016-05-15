# -*- coding: utf-8 -*-
"""Tests for fastavro"""

from __future__ import absolute_import

import sys
from glob import iglob
from os.path import join, abspath, dirname, basename

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

try:
    import snappy
except ImportError:
    snappy = None

import fastavro

from tests.utils import (
    random_byte_str, random_unicode_str,
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


def read_write_file(filename):
    with open(filename, 'rb') as fo:
        reader = fastavro.reader(fo)
        assert hasattr(reader, 'schema'), 'no schema on file'

        if basename(filename) in NO_DATA:
            return

        records = list(reader)
        assert len(records) > 0, 'no records found'

    new_file = BytesIO()
    fastavro.writer(new_file, reader.schema, records, reader.codec)
    new_file_bytes = new_file.getvalue()

    new_file = NoSeekBytesIO(new_file_bytes)
    new_reader = fastavro.reader(new_file)
    assert hasattr(new_reader, 'schema'), "schema wasn't written"
    assert new_reader.schema == reader.schema
    assert new_reader.codec == reader.codec
    new_records = list(new_reader)

    assert new_records == records

    # Test schema migration with the same schema
    new_file = NoSeekBytesIO(new_file_bytes)
    schema_migration_reader = fastavro.reader(new_file, reader.schema)
    assert schema_migration_reader.reader_schema == reader.schema
    new_records = list(schema_migration_reader)

    assert new_records == records


def test_read_write_files():
    filenames = iglob(join(data_dir, '*.avro'))
    for filename in sorted(filenames):
        if 'snappy' in filename and not snappy:
            continue
        yield read_write_file, filename


def test_not_avro():
    try:
        with open(__file__, 'rb') as fo:
            fastavro.reader(fo)
        assert False, 'ValueError not raised: Opened non-avro file'
    except ValueError:
        pass
    except Exception as exc:
        assert False, 'Expected ValueError: %s raised instead' % type(exc)


def test_acquaint_schema_rejects_undeclared_name():
    try:
        fastavro.acquaint_schema({
            'name': 'schema_test',
            'type': 'record',
            'fields': [{
                'name': 'left',
                'type': 'Thinger',
            }],
        })
        assert False, 'UnknownType not raised'
    except fastavro.UnknownType as e:
        assert 'Thinger' == e.name
    except Exception as exc:
        assert False, 'Expected UnknownType: %s raised instead' % type(exc)


def test_acquaint_schema_rejects_unordered_references():
    try:
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
        assert False, 'UnknownType not raised'
    except fastavro.UnknownType as e:
        assert 'Thinger' == e.name
    except Exception as exc:
        assert False, 'Expected ValueError: %s raised instead' % type(exc)


def test_acquaint_schema_accepts_nested_namespaces():
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
    assert 'com.example.Inner' in fastavro._writer.SCHEMA_DEFS


def test_acquaint_schema_resolves_references_from_unions():
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
    b_schema = fastavro._writer.SCHEMA_DEFS['com.other.Outer']['fields'][1]
    assert b_schema['type'][1] == 'com.other.Inner'


def test_acquaint_schema_accepts_nested_records_from_arrays():
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
    assert 'Nested' in fastavro._writer.SCHEMA_DEFS


def _check_exception(exc, substrs, quoted=False):
    substrs = substrs if isinstance(substrs, (list, tuple)) else (substrs,)
    if quoted:
        return all("'%s'" % s in str(exc) for s in substrs)
    else:
        return all(s in str(exc) for s in substrs)


def test_acquaint_schema_rejects_record_without_name():
    # 'record' is a 'Named' type. Per the Avro specification, the 'name'
    # attribute is required.
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'fields': [{'name': 'test', 'type': 'int'}],
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('record', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)


def test_acquaint_schema_rejects_record_without_fields():
    # Per the Avro specification, the 'fields' attribute is required for
    # the 'record' type
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('record', 'fields'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)

    # However an empty list is OK
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
            'fields': [],
        })
    except fastavro.SchemaError:
        assert False, 'No SchemaError should be raised'


def test_acquaint_schema_rejects_record_fields_without_name():
    # Per the Avro specification, each `field` in a 'record' requires a
    # 'name' attribute.
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
            'fields': [
                {'name': 'test', 'type': 'string'},
                {'type': 'int'},
            ]
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('record', 'field', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)


def test_acquaint_schema_rejects_record_fields_without_type():
    # Per the Avro specification, each `field` in a 'record' requires a
    # 'type' attribute.
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
            'fields': [{'name': 'test'}],
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('record', 'field', 'type'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)


def test_acquaint_schema_rejects_enum_without_name():
    # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
    # attribute is required.
    try:
        fastavro.acquaint_schema({
            'type': 'enum',
            'symbols': ['foo', 'bar', 'baz'],
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('enum', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)


def test_acquaint_schema_rejects_enum_without_symbols():
    # Per the Avro specification, the 'symbols' attribute is required for
    # the 'enum' type
    try:
        fastavro.acquaint_schema({
            'type': 'enum',
            'name': 'enum_test',
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('enum', 'symbols'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)

    # However an empty list is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'enum',
            'name': 'enum_test',
            'symbols': [],
        })
    except fastavro.SchemaError:
        assert False, 'No SchemaError should be raised'


def test_acquaint_schema_rejects_fixed_without_name():
    # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
    # attribute is required.
    try:
        fastavro.acquaint_schema({
            'type': 'fixed',
            'size': 32,
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('fixed', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)


def test_acquaint_schema_rejects_fixed_without_size():
    # Per the Avro specification, the 'size' attribute is required for
    # the 'fixed' type
    try:
        fastavro.acquaint_schema({
            'type': 'fixed',
            'name': 'fixed_test',
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('fixed', 'size'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)

    # However a zero 'size' is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'fixed',
            'name': 'fixed_test',
            'size': 0
        })
    except fastavro.SchemaError:
        assert False, 'No SchemaError should be raised'


def test_acquaint_schema_rejects_array_without_items():
    # Per the Avro specification, the 'items' attribute is required for
    # the 'array' type
    try:
        fastavro.acquaint_schema({
            'type': 'array',
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('array', 'items'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)

    # However an empty list is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'array',
            'items': [],
        })
    except fastavro.SchemaError:
        assert False, 'No SchemaError should be raised'


def test_acquaint_schema_rejects_map_without_values():
    # Per the Avro specification, the 'values' attribute is required for
    # the 'map' type
    try:
        fastavro.acquaint_schema({
            'type': 'map',
        })
        assert False, 'SchemaError not raised'
    except fastavro.SchemaError as exc:
        check = _check_exception(exc, ('map', 'values'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        assert False, 'Expected SchemaError: %s raised instead' % type(exc)

    # However an empty list is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'map',
            'values': [],
        })
    except fastavro.SchemaError:
        assert False, 'No SchemaError should be raised'


def test_schemaless_writer_and_reader():
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
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record


def test_default_values():
    schema = {
        'name': 'default_test',
        'type': 'record',
        'fields': [{
            'name': 'default_field',
            'type': 'string',
            'default': 'default_value',
        }],
    }
    new_file = BytesIO()
    records = [{}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == [{'default_field': 'default_value'}]


def test_boolean_roundtrip():
    schema = {
        'name': 'boolean_test',
        'type': 'record',
        'fields': [{
            'name': 'field',
            'type': 'boolean',
        }],
    }
    record = {'field': True}
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record

    record = {'field': False}
    new_file = BytesIO()
    fastavro.schemaless_writer(new_file, schema, record)
    new_file.seek(0)
    new_record = fastavro.schemaless_reader(new_file, schema)
    assert record == new_record


def test_metadata():
    schema = {
        'name': 'metadata_test',
        'type': 'record',
        'fields': [],
    }

    new_file = BytesIO()
    records = [{}]
    metadata = {'key': 'value'}
    fastavro.writer(new_file, schema, records, metadata=metadata)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    assert new_reader.metadata['key'] == metadata['key']


def test_repo_caching_issue():
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

    new_file = BytesIO()
    records = [{'b': {'c': 'test'}}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == records

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

    new_file = BytesIO()
    records = [{'a': {'b': {'c': 1}}, 'aa': {'b': {'c': 2}}}]
    fastavro.writer(new_file, other_schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_remove_field():
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

    new_file = BytesIO()
    records = [{'test': 'test'}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == [{}]


def test_schema_migration_add_default_field():
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

    new_file = BytesIO()
    records = [{}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == [{'test': 'default'}]


def test_schema_migration_union_int_to_float_promotion():
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

    new_file = BytesIO()
    records = [{'test': 1}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_maps_with_union_promotion():
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

    new_file = BytesIO()
    records = [{'test': {'foo': 1}}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_array_with_union_promotion():
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

    new_file = BytesIO()
    records = [{'test': [1, 2, 3]}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_writer_union():
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

    new_file = BytesIO()
    records = [{'test': 1}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_reader_union():
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

    new_file = BytesIO()
    records = [{'test': 1}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_writer_and_reader_union():
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

    new_file = BytesIO()
    records = [{'test': u'foo'}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert new_records == records


def test_schema_migration_reader_union_failure():
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

    new_file = BytesIO()
    records = [{'test': True}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    try:
        list(new_reader)
        assert False, 'SchemaResolutionError not raised'
    except fastavro.SchemaResolutionError:
        pass
    except Exception as exc:
        assert False, ('Expected SchemaResolutionError: %s raised instead'
                       % type(exc))


def test_schema_migration_writer_union_failure():
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

    new_file = BytesIO()
    records = [{'test': u'foo'}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    try:
        list(new_reader)
        assert False, 'SchemaResolutionError not raised'
    except fastavro.SchemaResolutionError:
        pass
    except Exception as exc:
        assert False, ('Expected SchemaResolutionError: %s raised instead'
                       % type(exc))


def test_schema_migration_array_failure():
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

    new_file = BytesIO()
    records = [{'test': [1, 2, 3]}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    try:
        list(new_reader)
        assert False, 'SchemaResolutionError not raised'
    except fastavro.SchemaResolutionError:
        pass
    except Exception as exc:
        assert False, ('Expected SchemaResolutionError: %s raised instead'
                       % type(exc))


def test_schema_migration_maps_failure():
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

    new_file = BytesIO()
    records = [{'test': {'foo': 'a'}}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    try:
        list(new_reader)
        assert False, 'SchemaResolutionError not raised'
    except fastavro.SchemaResolutionError:
        pass
    except Exception as exc:
        assert False, ('Expected SchemaResolutionError: %s raised instead'
                       % type(exc))


def test_schema_migration_enum_failure():
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

    new_file = BytesIO()
    records = ['FOO']
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    try:
        list(new_reader)
        assert False, 'SchemaResolutionError not raised'
    except fastavro.SchemaResolutionError:
        pass
    except Exception as exc:
        assert False, ('Expected SchemaResolutionError: %s raised instead'
                       % type(exc))


def test_schema_migration_schema_mismatch():
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

    new_file = BytesIO()
    records = [{'test': 'test'}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    try:
        list(new_reader)
        assert False, 'SchemaResolutionError not raised'
    except fastavro.SchemaResolutionError:
        pass
    except Exception as exc:
        assert False, ('Expected SchemaResolutionError: %s raised instead'
                       % type(exc))


def test_empty():
    io = BytesIO()
    schema = {
        'name': 'test',
        'type': 'record',
        'fields': [
            {'type': 'boolean', 'name': 'a'},
        ],
    }
    try:
        fastavro.load(io, schema)
        assert False, 'EOFError not raised: Read from an empty file'
    except EOFError:
        pass
    except Exception as exc:
        assert False, 'Expected EOFError: %s raised instead' % type(exc)


def test_fixed_roundtrip():
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
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file)
    new_records = list(reader)
    assert new_records == records


def test_bytes_roundtrip():
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
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file)
    new_records = list(reader)
    assert new_records == records


def test_string_roundtrip():
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
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file)
    new_records = list(reader)
    assert new_records == records


def test_string_with_non_unicode_values_roundtrip():
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
        # UTF-8 encoded "быстрый" (~= 'fast' in Russian)
        'test2': b'\xd0\xb1\xd1\x8b\xd1\x81\xd1\x82\xd1\x80\xd1\x8b\xd0\xb9',
    }, {
        'test1': b'Not\x09Obviously\x0AUTF-8 Safe',
        # UTF-8 encoded "γρήγορος" (~= 'fast' in Greek)
        'test2': b'\xce\xb3\xcf\x81\xce\xae\xce\xb3\xce\xbf\xcf\x81\xce\xbf\xcf\x82',  # noqa
    }]
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    # Decode binary strings for result comparison
    for r, k, v in ((r, k, v) for r in records for k, v in r.items()):
        r[k] = v.decode('utf-8')

    reader = fastavro.reader(new_file)
    new_records = list(reader)
    assert new_records == records


def test_float_double_roundtrip():
    schema = {
        'name': 'float_test',
        'type': 'record',
        'fields': [
            {'name': 'float', 'type': 'float'},
            {'name': 'double', 'type': 'double'},
        ],
    }

    records = [
        {'float': 3.141592654, 'double': 3.141592654},
        {'float': 2.718281828, 'double': 2.718281828},
    ]
    new_file = BytesIO()
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)

    reader = fastavro.reader(new_file)
    new_records = list(reader)
    for n, r in zip(new_records, records):
        diff_float = abs(n['float'] - r['float'])
        diff_double = abs(n['double'] - r['double'])
        # This is a bit arbitrary I guess:
        assert diff_float < 0.001 and diff_double < 0.0001


if __name__ == '__main__':
    import nose
    config = nose.config.Config(logStream=sys.stdout, stream=sys.stdout)
    nose.runmodule(config=config)
