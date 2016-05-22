# Find Python Include directory
# This might not be correct in all circumstances

define PY_INCLUDE_S
from distutils import sysconfig
INC_PY = sysconfig.get_python_inc()
if INC_PY:
    print("-I%s" % INC_PY)
else:
    print("")
endef
PY_INCLUDE := $(shell python -c '$(PY_INCLUDE_S)')

