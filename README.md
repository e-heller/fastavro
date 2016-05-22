fastavro - Now even faster!
===========================

The Python [`avro`][avro_pypi] package distributed by 
[Apache][apache_avro] is **incredibly slow**. When dealing with very 
large datasets, the performance of [`avro`][avro_pypi] is unacceptable,
more or less unfit for any production use.

**`fastavro`** is less feature complete than [`avro`][avro_pypi], 
however performance reading and writing Avro files is significantly
faster.

With the [Cython][cython] extension modules compiled, **`fastavro`**
is **really** fast, almost on par with the native Java implementation
of Avro.

The focus of this fork ([e-heller/fastavro]) is on reimplementing and 
optimizing `fastavro's` [Cython][cython] extension modules in *pure
Cython code* to achieve screaming speed.

I hope these improvements will eventually be pulled into the upstream 
repo, but that may take some time.

This **`fastavro`** fork supports these Python versions:

* Python 2.6
* Python 2.7
* Python 3.4
* Python 3.5


[e-heller/fastavro]: https://github.com/e-heller/fastavro
[avro_pypi]: https://pypi.python.org/pypi/avro
[apache_avro]: http://avro.apache.org
[Cython]: http://cython.org


Build and Install
=================

Because **`fastavro`** is shipped with [Cython][cython] extension
modules, you will require the following to build and install:

1. [Cython][cython_pypi] to generate the C extension files

2. Some kind of C compiler like `gcc`

If you can't compile the extension modules, you can still use the
*pure Python* implementation, but you will be missing out on the
significant performance improvements.

Compile the source
------------------

First, download [Cython][cython_pypi]:

```shell
$ pip install cython
```

To build the compiled extensions, from the root `fastavro` source
directory:

```shell
$ make cfiles
$ python setup.py build
```

Install the package
-------------------

Assuming the build worked, now you can just type:

```shell
$ python setup.py install
```


Basic Usage
===========

Reading from Avro format
------------------------

```python
import fastavro

with open('some_file.avro', 'rb') as input:
    # Create a `Reader` object
    reader = fastavro.Reader(input)

    # Obtain the writer's schema if required
    schema = reader.schema

    # Iteratively read the records:
    for record in reader:
        process_record(record)
        
    # Read the records in one shot:
    records = list(reader)
```


Writing to Avro format
----------------------

```python
import fastavro

# Define an Avro Schema as a dict:
schema = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
    ],
}

# Create some records:
records = [
    {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

# Open a file, and write the records:
with open('some_file.avro', 'wb') as out:
    fastavro.write(out, schema, records)
```


Limitations
===========

**`fastavro`** is missing many of the features of the official Apache
[`avro`][avro_pypi] package. Essentially, **`fastavro`** only supports
reading and writing in Avro format.

Notably, there is no support for:

* [Avro Protocols][spec_protocol]

* The [Protocol Wire Format][spec_wire_format] or any kind of Wire
  Transmission

There are also some limitations with reading and writing:

* Incomplete support for *'reader's schemas'* - i.e., reading an Avro
  file written with one schema (the 'writer's schema') and interpreting
  the data with another schema (the 'reader's schema').
  See [Schema Resolution][spec_schema_res] in the Avro specification
  for details.
  
* No support for *Aliases* - i.e., the `aliases` attribute in named 
  Avro types. See [Aliases][spec_aliases] in the Avro specification
  for details.
  
* No support for *Logical Types* - i.e., the new `logicalType` attribute
  that adds support for derived types in Avro 1.8 like `decimal`,
  `date`, `time-millis`, `timestamp-millis`, etc.
  See [Logical Types][spec_logical] in the Avro specification for
  details.
  > Note: the [ViaSat/fastavro] fork currently has support for some
  Logical Types. I hope to incorporate this work soon.


[spec_wire_format]: https://avro.apache.org/docs/current/spec.html#Protocol+Wire+Format
[spec_protocol]: https://avro.apache.org/docs/current/spec.html#Protocol+Declaration
[spec_schema_res]: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
[spec_aliases]: https://avro.apache.org/docs/current/spec.html#Aliases
[spec_logical]: https://avro.apache.org/docs/current/spec.html#Logical+Types
[ViaSat/fastavro]: https://github.com/ViaSat/fastavro/


Hacking
=======

If you want to play around and modify any of the [Cython][cython]
`.pyx` files, you will need to recompile the C module code.

You will require:

1. [Cython][cython_pypi] to regenerate the C files
2. Some kind of C compiler

To easiest way to recompile the C extensions is as follows.

From the root `fastavro` source directory:

```shell
$ make build
```

The `make build` command first calls `cython` to generate the C files,
then compiles them via `setup.py`. (The exact command is
`python setup.py build_ext -i`)

If you are feeling more adventurous, and you're on a Linux-y platform
with `gcc` you can try to compile the extensions with the GCC settings
I personally use:

```shell
$ make compile
```

This will compile the extensions "in place" in the source directory.

To install the package with your modifications, you'll just have to 
manually copy the `fastavro` package directory to your Python
`site-packages` directory.

I definitely recommend working inside a virtual environment if you plan 
to modify and rebuild `fastavro`. If you are unfamiliar with virtual
environments, check out [virtualenv][venv] or [pew][pew].
Personally, I recommend [pew][pew].


[cython]: http://cython.org/
[cython_pypi]: https://pypi.python.org/pypi/Cython
[venv]: http://pypi.python.org/pypi/virtualenv
[pew]: https://pypi.python.org/pypi/pew
[distutils]: https://docs.python.org/2.7/extending/building.html


Changes
=======

See the [ChangeLog]

[ChangeLog]: https://github.com/e-heller/fastavro/blob/master/ChangeLog


Contact
=======

[The e-heller/fastavro fork](https://github.com/e-heller/fastavro/)

[Official fastavro repo](https://github.com/tebeka/fastavro)
