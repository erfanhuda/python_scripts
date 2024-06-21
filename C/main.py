import os
import ctypes

path = os.getcwd()
clib = ctypes.CDLL(os.path.join(path, 'main.so'))


x = clib.add
x.argtypes = [ctypes.c_int, ctypes.c_int]
x.restype = ctypes.c_int

# clib.prompt(3000)
# print(x(20, 1000))


class Point(ctypes.Structure):
    _fields_ = [("x", ctypes.c_int), ("y", ctypes.c_int)]

p1 = Point(10, 20)
point = clib.Point(p1)