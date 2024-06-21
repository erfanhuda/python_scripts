import ctypes

win = ctypes.CDLL(".\\main.so")
win.prompt(3000)