# Find Python compiled extension suffix (e.g., '.so')
# This might not be correct in all circumstances

define PY_SO_SUFFIX_S
from distutils import sysconfig
SO = sysconfig.get_config_var("EXT_SUFFIX")
if not SO:
    SO = sysconfig.get_config_var("SO")
print(SO or ".so")
endef
PY_SO_SUFFIX := $(shell python -c '$(PY_SO_SUFFIX_S)')
