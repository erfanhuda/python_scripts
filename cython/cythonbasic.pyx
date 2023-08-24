import cython

# 1. Cython Data Types
def buckets(a):
    print(cython.inline("return a+b", b=3))