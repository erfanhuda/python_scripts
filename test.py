import ctypes.util
import sys

python_lib = "python{}{}.dll".format(sys.version_info.major, sys.version_info.minor)
python_lib = ctypes.util.find_library(python_lib)