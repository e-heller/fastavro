# -*- coding: utf-8 -*-
# cython: auto_cpdef=True
"""Utilities for interacting with Avro schemas"""


# This code is based on the Apache 'avro' pacakge, found at:
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/
# Under the Apache 2.0 License (https://www.apache.org/licenses/LICENSE-2.0)

# Please refer to the Avro specification page for details on data encoding:
#   https://avro.apache.org/docs/1.8.0/spec.html


from __future__ import absolute_import


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


# Avro Primitive types
# see: https://avro.apache.org/docs/1.8.0/spec.html#schema_primitive
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

# Avro Complex types
# see: https://avro.apache.org/docs/1.8.0/spec.html#schema_complex
NAMED_TYPES = set([
    'enum',
    'error',
    'fixed',
    'record',
])

COMPLEX_TYPES = NAMED_TYPES | set([
    'array',
    'error_union',
    'map',
    'request',
    'union',
])

# All Avro types
AVRO_TYPES = PRIMITIVE_TYPES | COMPLEX_TYPES


class UnknownType(Exception):
    def __init__(self, name):
        super(UnknownType, self).__init__(name)
        self.name = name


def extract_record_type(schema):
    if isinstance(schema, dict):
        return schema['type']

    if isinstance(schema, list):
        return 'union'

    return schema


def schema_name(schema, parent_ns):
    name = schema.get('name')
    if not name:
        return parent_ns, None

    namespace = schema.get('namespace', parent_ns)
    if not namespace:
        return namespace, name

    return namespace, '%s.%s' % (namespace, name)


def extract_named_schemas_into_repo(schema, repo, transformer, parent_ns=None):
    if type(schema) == list:
        for index, enum_schema in enumerate(schema):
            namespaced_name = extract_named_schemas_into_repo(
                enum_schema,
                repo,
                transformer,
                parent_ns,
            )
            if namespaced_name:
                schema[index] = namespaced_name
        return

    if type(schema) != dict:
        # If a reference to another schema is an unqualified name, but not one
        # of the primitive types, then we should add the current enclosing
        # namespace to reference name.
        if schema not in PRIMITIVE_TYPES and '.' not in schema and parent_ns:
            schema = parent_ns + '.' + schema

        if schema not in repo:
            raise UnknownType(schema)
        return schema

    namespace, name = schema_name(schema, parent_ns)

    if name:
        repo[name] = transformer(schema)

    schema_type = schema.get('type')
    if schema_type == 'array':
        namespaced_name = extract_named_schemas_into_repo(
            schema['items'],
            repo,
            transformer,
            namespace,
        )
        if namespaced_name:
            schema['items'] = namespaced_name
        return
    if schema_type == 'map':
        namespaced_name = extract_named_schemas_into_repo(
            schema['values'],
            repo,
            transformer,
            namespace,
        )
        if namespaced_name:
            schema['values'] = namespaced_name
        return
    # Normal record.
    for field in schema.get('fields', []):
        namespaced_name = extract_named_schemas_into_repo(
            field['type'],
            repo,
            transformer,
            namespace,
        )
        if namespaced_name:
            field['type'] = namespaced_name
