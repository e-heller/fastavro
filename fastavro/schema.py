# -*- coding: utf-8 -*-
"""Utilities for interacting with Avro schemas"""


# This code is based on the Apache 'avro' pacakge, found at:
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/
# Under the Apache 2.0 License (https://www.apache.org/licenses/LICENSE-2.0)

# Please refer to the Avro specification page for details on data encoding:
#   https://avro.apache.org/docs/current/spec.html


from __future__ import absolute_import

import sys


if sys.version_info[0] == 2:
    _bytes_type = str
    _string_type = basestring  # noqa
    _int_type = (int, long)  # noqa
else:
    _bytes_type = bytes
    _string_type = str
    _int_type = int

# All `Schema` objects should be one of these types
_schema_type = (_string_type, dict, list)


# ---- Avro Header Specification ---------------------------------------------#

# Magic bytes for Avro (version 1)
MAGIC = b'Obj\x01'

SYNC_SIZE = 16
SYNC_INTERVAL = 4000 * SYNC_SIZE


# Avro Header schema
HEADER_SCHEMA = {
    'type': 'record',
    'name': 'org.apache.avro.file.Header',
    'fields': [
        {
            'name': 'magic',
            'type': {'type': 'fixed', 'name': 'magic', 'size': len(MAGIC)},
        }, {
            'name': 'meta',
            'type': {'type': 'map', 'values': 'bytes'}
        }, {
            'name': 'sync',
            'type': {'type': 'fixed', 'name': 'sync', 'size': SYNC_SIZE}
        },
    ]
}


# ---- Avro Types ------------------------------------------------------------#

# Primitive types
#   Ref: https://avro.apache.org/docs/current/spec.html#schema_primitive
PRIMITIVE_TYPES = set([
    'boolean',
    'bytes',
    'double',
    'float',
    'int',
    'long',
    'null',
    'string',
])


# Complex types
#   Ref: https://avro.apache.org/docs/current/spec.html#schema_complex

# Named complex types
#   Ref: https://avro.apache.org/docs/current/spec.html#names
NAMED_TYPES = set([
    'enum',
    'error',
    'fixed',
    'record',
])

# Other complex types
COMPLEX_TYPES = NAMED_TYPES | set([
    'array',
    'error_union',
    'map',
    'union',
])


# All Avro types
AVRO_TYPES = PRIMITIVE_TYPES | COMPLEX_TYPES


# Schema types represented by a Python `dict`
DICT_TYPES = NAMED_TYPES | set([
    'array',
    'map',
])


# Required Attributes for Avro types
REQUIRED_ATTRIBUTES = {
    'enum': {
        'name': {'type': _string_type},
        'symbols': {'type': list},
    },
    'fixed': {
        'name': {'type': _string_type},
        'size': {'type': _int_type},
    },
    'record': {
        'name': {'type': _string_type},
        'fields': {
            'type': list,
            'attributes': {
                'name': {'type': _string_type},
                'type': {'type': _schema_type},
            },
        },
    },
    'array': {
        'items': {'type': _schema_type},
    },
    'map': {
        'values': {'type': _schema_type},
    },
}
REQUIRED_ATTRIBUTES['error'] = REQUIRED_ATTRIBUTES['record']


# ---- Schema Exceptions -----------------------------------------------------#

class SchemaError(Exception):
    pass


class SchemaAttributeError(SchemaError):
    def __init__(self, msg, schema, attr):
        super(SchemaAttributeError, self).__init__(msg, schema, attr)
        self.schema = schema
        self.attr = attr


class InvalidTypeError(SchemaError):
    def __init__(self, msg, schema):
        super(InvalidTypeError, self).__init__(msg, schema)
        self.schema = schema


class UnknownTypeError(InvalidTypeError):
    pass


# ---- Schema Parsing --------------------------------------------------------#

def normalize_schema(schema, parent_ns=None):
    """Normalize `schema` using the parent namespace `parent_ns`
        - Resolve names to full namespaced names
        - Normalize primitives expressed as `{'type': 'PRIMITIVE'}` to the
          simple form: `'PRIMITIVE'`
        - Verify schemas have required attributes  (see: `REQUIRED_ATTRIBUTES`)
    """
    if isinstance(schema, _string_type):
        # If a reference to another schema is an unqualified name, but not one
        # of the primitive types, then we should add the current enclosing
        # namespace to reference name.
        if schema not in PRIMITIVE_TYPES and '.' not in schema and parent_ns:
            schema = '%s.%s' % (parent_ns, schema)
        return schema

    elif isinstance(schema, list):
        # A list indicates a 'union' schema
        for index, union_schema in enumerate(schema):
            schema[index] = normalize_schema(union_schema, parent_ns)
        return schema

    elif isinstance(schema, dict):
        # A dict schema must have a 'type' attribute -
        schema_type = schema.get('type')
        if not schema_type:
            msg = "Schema definition requires a 'type' attribute: %r"
            raise SchemaAttributeError(msg % schema, schema, 'type')

        # Avro permits a dict-type schema definition for primitive types:
        #   e.g., `{'type': 'string'}`  is equivalent to  `'string'`
        # Normalize primitives expressed in this way to the simple form
        if schema_type in PRIMITIVE_TYPES:
            return schema_type

        # Otherwise a dict-type schema type should be in `DICT_TYPES`:
        #   i.e., (array, enum, fixed, map, record, error)
        if schema_type not in DICT_TYPES:
            if schema_type in AVRO_TYPES:
                msg = 'Invalid type %r in dict-type schema %r'
                raise InvalidTypeError(msg % (schema_type, schema), schema)
            else:
                msg = 'Unknown schema type %r in %r'
                raise UnknownTypeError(msg % (schema_type, schema), schema)

        # Verify some required schema attributes (see: `REQUIRED_ATTRIBUTES`)
        verify_required_attributes(schema)

        if schema_type in NAMED_TYPES:
            # Resolve the namespaced-name for 'Named' types
            namespace = schema.get('namespace', parent_ns)
            name = schema['name']
            if namespace and '.' not in name:
                schema['name'] = '%s.%s' % (namespace, name)
        else:
            namespace = parent_ns

        # Recursively resolve namespaced-names:
        if schema_type == 'array':
            schema['items'] = normalize_schema(schema['items'], namespace)
        elif schema_type == 'map':
            schema['values'] = normalize_schema(schema['values'], namespace)
        elif schema_type in ('record', 'error'):
            for field in schema['fields']:
                field['type'] = normalize_schema(field['type'], namespace)
        return schema

    else:
        raise UnknownTypeError('Unknown schema type: %r' % schema, schema)


def extract_named_schemas(schema, repo, transformer):
    """Extract all 'Named' schema types in `schema` into the repository `repo`
    """
    if isinstance(schema, _string_type):
        if schema not in repo:
            raise UnknownTypeError('Unknown schema type: %r' % schema, schema)
    elif isinstance(schema, list):
        for union_schema in schema:
            extract_named_schemas(union_schema, repo, transformer)
    elif isinstance(schema, dict):
        schema_type = schema['type']
        # Add 'Named' schema types into the `repo`
        if schema_type in NAMED_TYPES:
            repo[schema['name']] = transformer(schema)

        if schema_type == 'array':
            extract_named_schemas(schema['items'], repo, transformer)
        elif schema_type == 'map':
            extract_named_schemas(schema['values'], repo, transformer)
        elif schema_type in ('record', 'error'):
            for field in schema['fields']:
                extract_named_schemas(field['type'], repo, transformer)


def verify_required_attributes(schema):
    schema_type = schema['type']
    required = REQUIRED_ATTRIBUTES.get(schema_type)
    if required:
        for attr, verify in required.items():
            verify_attribute(schema, attr, verify['type'])
            if 'attributes' in verify:
                verify_attribute_values(schema, attr, verify['attributes'])


def verify_attribute_values(schema, attr, required):
    schema_type = '%s.%s' % (schema['type'], attr)
    for child_attr, verify in required.items():
        for attr_value in schema[attr]:
            verify_attribute(
                attr_value, child_attr, verify['type'], schema_type
            )


def verify_attribute(schema, attr, attr_type, schema_type=None):
    schema_type = schema_type or schema['type']
    if attr not in schema:
        msg = "A '%s' schema requires a '%s' attribute"
        raise SchemaAttributeError(msg % (schema_type, attr), schema, attr)
    attr_value = schema[attr]
    if attr_value in (None, '', {}):
        # Reject values of None, '' or {}
        msg = "A '%s' schema requires a non-empty '%s' attribute"
        raise SchemaAttributeError(msg % (schema_type, attr), schema, attr)
    elif not isinstance(attr_value, attr_type):
        # Other than rejecting (None, '', {}), we do not otherwise assert that
        # an attribute's value is 'non-empty'.
        # For example, this permits values of:
        #   [] : `fields`  attr of `record` type
        #   [] : `symbols` attr of `enum` type
        #   [] : `items`   attr of `array` type
        #   [] : `values`  attr of `map` type
        # This is consistent with the behavior of the Apache avro package.
        msg = "The '%s' attribute of a '%s' schema cannot be %r"
        raise SchemaAttributeError(
            msg % (attr, schema_type, attr_value), schema, attr,
        )


# ---- Prepare Schema for JSON Dump ------------------------------------------#

def decode_bytes_in_schema(schema):
    """Decode `bytes` values in `schema` to unicode.
    The `json.dumps` method will fail if any bytes values cannot be decoded
    to UTF-8. Here, we first try decoding with 'utf-8'. If that fails, then
    default to decoding with 'raw_unicode_escape' instead.
    """
    _decode = decode_bytes_in_schema
    if isinstance(schema, _bytes_type):
        try:
            return schema.decode('utf-8')
        except UnicodeDecodeError:
            return schema.decode('raw_unicode_escape')
    elif isinstance(schema, list):
        return [_decode(v) for v in schema]
    elif isinstance(schema, dict):
        return dict((_decode(k), _decode(v)) for k, v in schema.items())
    else:
        return schema
