import Cython
import subprocess
import os
import sys
from PIL import Image
import cv2
import pytesseract

PYTHON_VERSION = sys.version
CYTHON_VERSION = Cython.__version__
NODE_VERSION = subprocess.check_output(
    [os.environ.get('node', 'node'), "--version"]).decode().splitlines()[0]
# GCC_DETAIL_VERSION = [line for line in subprocess.check_output(
#     [os.environ.get('CC', 'cc'), "--version", "-v"], stderr=subprocess.STDOUT).decode().splitlines() if 'version' in line][0]

print("Python version :", PYTHON_VERSION)
print("Node version :", NODE_VERSION)
print("Cython version :", CYTHON_VERSION)

file = "D:/dev/GitHub/python_scripts/file/img_20190511_084303.jpg"
im = cv2.imread(file)
cv2.imshow("Original Image", im)
cv2.waitKey(0)
