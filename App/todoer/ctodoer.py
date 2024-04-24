import time
import cython

if not cython.compiled:
    print("Running in native Python mode")
    from math import sin
else:
    print("Running in Cython mode")
    # if using built-in C/C++ libraries
    # from cython.cimports.libc.math import sin

    # if using pxd file as external library
    from cmath import sin

@cython.cfunc
def save_name(name: cython.char) -> cython.char:
    print(f"Success to create new user. Hello {name} !")
    return {'username': name}

@cython.cfunc
def fibonacci_fun(n):
    a, b = 0, 1
    res = []
    while b < n:
        res.append(b)
        a, b = b, a + b

    return res

def counter(func, args):
    s1 = time.time()
    func_runner = func(args)
    s2 = time.time()
    
    return {"result": func_runner, "ETA": s2 - s1}

def fibonacci_py(n):
    a, b = 0, 1
    res = []
    while b < n:
        res.append(b)
        a, b = b, a + b

    return res

if __name__ == '__main__':
    ...