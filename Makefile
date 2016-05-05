# Makefile for fastavro project
#
# Since we distribute the generated C file for simplicity (and so that end users
# won't need to install cython). You can re-create the C file using this
# Makefile.


ifndef PYTHON
    PYTHON=python
endif


#------ Project Files

py_files = fastavro/reader.py fastavro/writer.py \
			fastavro/compat.py fastavro/schema.py

c_files  = $(subst /,/_,$(py_files:.py=.c))


#------ Cython Arguments

cython_args    :=
cython_profile := profile=True


#------ Build Rules

_%.c: %.py
	cp $< $(<D)/_$(<F)
	cython $(cython_args) $(<D)/_$(<F)
	rm $(<D)/_$(<F)


all: build

# `cfiles` just calls Cython to generate the C files.
cfiles: $(c_files)


# `build` calls Cython to generate the C files, then compiles the modules
# with distutils by calling `setup.py build_ext`
build: $(c_files)
	python setup.py build_ext -i
	rm -rfv build dist *.egg-info


# `prof` calls cython with `-X profile=True` when generating the C files,
# which allows profiling the modules with python's cProfile module.
prof: cython_args := -X $(strip $(filter -X,$(cython_args)))$(cython_profile)
prof: build


clean:
	rm -rfv */*.c
	rm -rfv */*.o
	rm -rfv */*.s
	rm -rfv */*.so
	rm -rfv */*.dll
	rm -rfv */__pycache__
	rm -rfv */*.pyc
	rm -rfv build dist *.egg-info

fresh: clean all

publish:
	./publish.sh

test:
	PATH="${PATH}:${HOME}/.local/bin" tox


.PHONY: all cfiles build prof clean fresh publish test
