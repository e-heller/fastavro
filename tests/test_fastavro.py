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
from fastavro import ReadError, UnknownType, SchemaResolutionError, SchemaError

from tests.utils import (
    random_byte_str, random_unicode_str, _unicode_type, _bytes_type,
)


data_dir = join(abspath(dirname(__file__)), 'avro-files')

NO_DATA = set([
    'class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro',
    'testDataFileMeta.avro',
])


def _exc_name(exc):
    cls = getattr(exc, '__class__', None)
    cls_name = getattr(cls, '__name__', None) if cls else None
    if cls_name and cls_name != 'type':
        return str(cls_name)
    name = getattr(exc, '__name__', None)
    if name:
        return str(name)
    return str(type(exc))


class UnexpectedException(AssertionError):
    def __init__(self, expected, raised):
        msg = (
            "Expected '%s' Exception. '%s' raised instead"
            % (_exc_name(expected), _exc_name(raised))
        )
        super(UnexpectedException, self).__init__(msg)


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


def read_write_file(filename):
    with open(filename, 'rb') as fo:
        reader = fastavro.reader(fo)
        assert hasattr(reader, 'schema'), 'Failed to read schema'

        if basename(filename) in NO_DATA:
            return

        records = list(reader)
        assert len(records) > 0, 'No records found'

    write_buf = BytesIO()
    fastavro.writer(write_buf, reader.schema, records, reader.codec)
    serialized_data = write_buf.getvalue()
    write_buf.close()

    read_buf = BytesIO(serialized_data)
    read_back = fastavro.reader(read_buf)
    assert hasattr(read_back, 'schema'), 'Schema was not written'
    assert read_back.schema == reader.schema
    assert read_back.codec == reader.codec
    new_records = list(read_back)
    assert new_records == records
    read_buf.close()

    # Test schema migration with the same schema
    read_buf = BytesIO(serialized_data)
    schema_migration_reader = fastavro.reader(read_buf, reader.schema)
    assert schema_migration_reader.reader_schema == reader.schema
    new_records = list(schema_migration_reader)
    assert new_records == records
    read_buf.close()


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
    except ReadError:
        pass
    except Exception as exc:
        raise UnexpectedException(ReadError, exc)
    else:
        assert False, 'ReadError not raised: Opened non-avro file'


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
    except UnknownType as e:
        assert 'Thinger' == e.name
    except Exception as exc:
        raise UnexpectedException(UnknownType, exc)
    else:
        assert False, 'UnknownType not raised'


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
    except UnknownType as e:
        assert 'Thinger' == e.name
    except Exception as exc:
        raise UnexpectedException(UnknownType, exc)
    else:
        assert False, 'UnknownType not raised'


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
    assert 'com.example.Inner' in fastavro._writer.get_schema_defs()


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
    schema_defs = fastavro._writer.get_schema_defs()
    b_schema = schema_defs['com.other.Outer']['fields'][1]
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
    assert 'Nested' in fastavro._writer.get_schema_defs()


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
    except SchemaError as exc:
        check = _check_exception(exc, ('record', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'


def test_acquaint_schema_rejects_record_without_fields():
    # Per the Avro specification, the 'fields' attribute is required for
    # the 'record' type
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('record', 'fields'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'

    # However an empty list is OK
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
            'fields': [],
        })
    except SchemaError:
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
    except SchemaError as exc:
        check = _check_exception(exc, ('record', 'field', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'


def test_acquaint_schema_rejects_record_fields_without_type():
    # Per the Avro specification, each `field` in a 'record' requires a
    # 'type' attribute.
    try:
        fastavro.acquaint_schema({
            'type': 'record',
            'name': 'record_test',
            'fields': [{'name': 'test'}],
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('record', 'field', 'type'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'


def test_acquaint_schema_rejects_enum_without_name():
    # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
    # attribute is required.
    try:
        fastavro.acquaint_schema({
            'type': 'enum',
            'symbols': ['foo', 'bar', 'baz'],
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('enum', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'


def test_acquaint_schema_rejects_enum_without_symbols():
    # Per the Avro specification, the 'symbols' attribute is required for
    # the 'enum' type
    try:
        fastavro.acquaint_schema({
            'type': 'enum',
            'name': 'enum_test',
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('enum', 'symbols'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'

    # However an empty list is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'enum',
            'name': 'enum_test',
            'symbols': [],
        })
    except SchemaError:
        assert False, 'No SchemaError should be raised'


def test_acquaint_schema_rejects_fixed_without_name():
    # 'enum' is a 'Named' type. Per the Avro specification, the 'name'
    # attribute is required.
    try:
        fastavro.acquaint_schema({
            'type': 'fixed',
            'size': 32,
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('fixed', 'name'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'


def test_acquaint_schema_rejects_fixed_without_size():
    # Per the Avro specification, the 'size' attribute is required for
    # the 'fixed' type
    try:
        fastavro.acquaint_schema({
            'type': 'fixed',
            'name': 'fixed_test',
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('fixed', 'size'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'

    # However a zero 'size' is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'fixed',
            'name': 'fixed_test',
            'size': 0
        })
    except SchemaError:
        assert False, 'No SchemaError should be raised'


def test_acquaint_schema_rejects_array_without_items():
    # Per the Avro specification, the 'items' attribute is required for
    # the 'array' type
    try:
        fastavro.acquaint_schema({
            'type': 'array',
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('array', 'items'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'

    # However an empty list is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'array',
            'items': [],
        })
    except SchemaError:
        assert False, 'No SchemaError should be raised'


def test_acquaint_schema_rejects_map_without_values():
    # Per the Avro specification, the 'values' attribute is required for
    # the 'map' type
    try:
        fastavro.acquaint_schema({
            'type': 'map',
        })
    except SchemaError as exc:
        check = _check_exception(exc, ('map', 'values'), True)
        assert check, 'Incorrect SchemaError raised: %s' % exc
    except Exception as exc:
        raise UnexpectedException(SchemaError, exc)
    else:
        assert False, 'SchemaError not raised'

    # However an empty list is OK (maybe it should be rejected?)
    try:
        fastavro.acquaint_schema({
            'type': 'map',
            'values': [],
        })
    except SchemaError:
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


def test_schema_migration_union_bytes_to_string_promotion():
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

    new_file = BytesIO()
    byte_str = b'byte_str'
    records = [{'test': byte_str}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert isinstance(new_records[0]['test'], _unicode_type)
    assert new_records[0]['test'] == byte_str.decode('utf-8')


def test_schema_migration_union_string_to_bytes_promotion():
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

    new_file = BytesIO()
    unicode_str = u'unicode_str'
    records = [{'test': unicode_str}]
    fastavro.writer(new_file, schema, records)
    new_file.seek(0)
    new_reader = fastavro.reader(new_file, new_schema)
    new_records = list(new_reader)
    assert isinstance(new_records[0]['test'], _bytes_type)
    assert new_records[0]['test'] == unicode_str.encode('utf-8')


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
    except SchemaResolutionError:
        pass
    except Exception as exc:
        raise UnexpectedException(SchemaResolutionError, exc)
    else:
        assert False, 'SchemaResolutionError not raised'


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
    except SchemaResolutionError:
        pass
    except Exception as exc:
        raise UnexpectedException(SchemaResolutionError, exc)
    else:
        assert False, 'SchemaResolutionError not raised'


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
    except SchemaResolutionError:
        pass
    except Exception as exc:
        raise UnexpectedException(SchemaResolutionError, exc)
    else:
        assert False, 'SchemaResolutionError not raised'


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
    except SchemaResolutionError:
        pass
    except Exception as exc:
        raise UnexpectedException(SchemaResolutionError, exc)
    else:
        assert False, 'SchemaResolutionError not raised'


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
    except SchemaResolutionError:
        pass
    except Exception as exc:
        raise UnexpectedException(SchemaResolutionError, exc)
    else:
        assert False, 'SchemaResolutionError not raised'


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
    except SchemaResolutionError:
        pass
    except Exception as exc:
        raise UnexpectedException(SchemaResolutionError, exc)
    else:
        assert False, 'SchemaResolutionError not raised'


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
    except ReadError:
        pass
    except Exception as exc:
        raise UnexpectedException(ReadError, exc)
    else:
        assert False, 'ReadError not raised: Read from an empty file'


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
        # UTF-8 encoded Cyrillic chars
        'test2': b'\xd0\xb1\xd1\x8b\xd1\x81\xd1\x82\xd1\x80\xd1\x8b\xd0\xb9',
    }, {
        'test1': b'Not\x09Obviously\x0AUTF-8 Safe',
        # UTF-8 encoded Greek chars
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
