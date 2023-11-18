import Cython
import subprocess
import os
import sys
import pyMSVC

PYTHON_VERSION = sys.version
CYTHON_VERSION = Cython.__version__
NODE_VERSION = subprocess.check_output(
    [os.environ.get('node', 'node'), "--version"]).decode().splitlines()[0]
# GCC_DETAIL_VERSION = [line for line in subprocess.check_output(
#     [os.environ.get('CC', 'cc'), "--version", "-v"], stderr=subprocess.STDOUT).decode().splitlines() if 'version' in line][0]

print("Python version :", PYTHON_VERSION)
print("Node version :", NODE_VERSION)
print("Cython version :", CYTHON_VERSION)y
print("MSVC version :", pyMSVC.Environment())
# print(GCC_DETAIL_VERSION)
