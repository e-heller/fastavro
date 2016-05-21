# Find Python Include directory
# This might not be correct in all circumstances

define PY_INCLUDE_S
import sysconfig
INC_PY = sysconfig.get_config_var("INCLUDEPY")
if not INC_PY:
    INC_PY = sysconfig.get_path("include")
INC_PLT = sysconfig.get_path("platinclude")
if INC_PY and INC_PLT and INC_PY != INC_PLT:
    print("-I%s -I%s" % (INC_PLT, INC_PY))
elif INC_PLT:
    print("-I%s" % INC_PLT)
elif INC_PY:
    print("-I%s" % INC_PY)
else:
    print("")
endef
PY_INCLUDE := $(shell python -c '$(PY_INCLUDE_S)')

