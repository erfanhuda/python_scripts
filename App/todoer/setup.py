from setuptools import setup, Extension
from Cython.Build import cythonize

ext_modules = [
    Extension("ctodoer", ["ctodoer.py"], define_macros=[("CYTHON_LIMITED_API", 1)], py_limited_api=True)
]

setup(
    name="Todoer App",
    ext_modules=cythonize(ext_modules, annotate=True)
)
