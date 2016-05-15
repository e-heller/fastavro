# -*- coding: utf-8 -*-
# cython: auto_cpdef=True
"""Utilities for interacting with Avro schemas"""


# This code is based on the Apache 'avro' pacakge, found at:
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/
# Under the Apache 2.0 License (https://www.apache.org/licenses/LICENSE-2.0)

# Please refer to the Avro specification page for details on data encoding:
#   https://avro.apache.org/docs/current/spec.html


from __future__ import absolute_import

import sys


if sys.version_info[0] == 2:
    _string_type = basestring  # noqa
else:
    _string_type = str


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


# ---- Schema Handling -------------------------------------------------------#

class UnknownType(Exception):
    def __init__(self, name):
        super(UnknownType, self).__init__(name)
        self.name = name


class SchemaError(Exception):
    pass


def schema_name(schema, parent_ns):
    name = schema.get('name')
    if not name:
        return parent_ns, None

    namespace = schema.get('namespace', parent_ns)
    if not namespace:
        return namespace, name

    return namespace, '%s.%s' % (namespace, name)


def extract_named_schemas_into_repo(
    schema, repo, transformer, parent_ns=None
):
    def resolve(schema_, parent_ns_):
        return extract_named_schemas_into_repo(
            schema_,
            repo,
            transformer,
            parent_ns_,
        )

    if isinstance(schema, _string_type):
        # If a reference to another schema is an unqualified name, but not one
        # of the primitive types, then we should add the current enclosing
        # namespace to reference name.
        if schema not in PRIMITIVE_TYPES and '.' not in schema and parent_ns:
            schema = parent_ns + '.' + schema
        if schema not in repo:
            raise UnknownType(schema)
        return schema

    elif isinstance(schema, list):
        # A list indicates a 'union' schema
        for index, union_schema in enumerate(schema):
            namespaced_name = resolve(union_schema, parent_ns)
            if namespaced_name:
                schema[index] = namespaced_name

    elif isinstance(schema, dict):
        # A dict indicates one of: 'array', 'enum', 'fixed', 'map', 'record'
        # These types require all require a 'type' attribute
        check_schema_attrs(schema, 'type')
        schema_type = schema['type']
        namespace, name = schema_name(schema, parent_ns)

        # The Avro 'Named' types require a 'name' attribute
        if schema_type in NAMED_TYPES:
            check_schema_attrs(schema, 'name')
            repo[name] = transformer(schema)

        if schema_type == 'enum':
            check_schema_attrs(schema, 'symbols')
        elif schema_type == 'fixed':
            check_schema_attrs(schema, 'size')

        # Must recursively resolve namespaced names in these types:
        elif schema_type == 'array':
            check_schema_attrs(schema, 'items')
            namespaced_name = resolve(schema['items'], namespace)
            if namespaced_name:
                schema['items'] = namespaced_name
        elif schema_type == 'map':
            check_schema_attrs(schema, 'values')
            namespaced_name = resolve(schema['values'], namespace)
            if namespaced_name:
                schema['values'] = namespaced_name
        elif schema_type in ('record', 'error'):
            check_schema_attrs(schema, 'fields')
            for field in schema['fields']:
                check_sub_attrs(field, 'field', schema_type, ('name', 'type'))
                namespaced_name = resolve(field['type'], namespace)
                if namespaced_name:
                    field['type'] = namespaced_name
    else:
        raise SchemaError('Unknown schema type: %r' % schema)


def check_schema_attrs(schema, attrs):
    attrs = attrs if isinstance(attrs, (tuple, list)) else (attrs,)
    ty = schema.get('type')
    for attr in (a for a in attrs if schema.get(a) in (None, '')):
        raise SchemaError(
            "The '%s' schema type requires a '%s' attribute" % (ty, attr)
        )


def check_sub_attrs(obj, type_name, parent_type, attrs):
    attrs = attrs if isinstance(attrs, (tuple, list)) else (attrs,)
    for attr in (a for a in attrs if obj.get(a) in (None, '')):
        raise SchemaError(
            "Each '%s' in a '%s' schema type requires a '%s' attribute"
            % (type_name, parent_type, attr)
        )
