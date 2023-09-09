import os
import logging
from abc import ABC, abstractmethod
from functools import wraps
from sys import getsizeof

from custom_data_types import Matrix

class DirectorySize:
    """ Descriptor Dynamic Lookups"""
    def __get__(self, obj, objtype=None):
        return len(os.listdir(obj.dirname))

class Directory:
    size = DirectorySize() # Descriptor instance
    def __init__(self, dirname):
        self.dirname = dirname

def find_dir_size():
    s = Directory("src")
    g = Directory("node_modules")
    print(s.size)

class LoggedAgeAccess:
    """ Descriptor Managed Attributes """
    def __get__(self, obj, objtype=None):
        value = obj._age
        logging.info('Accessing %r giving %r', 'age', value)
        return value
    
    def __set__(self, obj, value):
        logging.info('Updating %r to %r', 'age', value)
        obj._age = value


class LoggedAccess:
    """ Descriptor Customized Name """
    def __set_name__(self, owner, name):
        self.public_name = name
        self.private_name = "_" + name

    def __get__(self, obj, objtype=None):
        value = getattr(obj, self.private_name)
        logging.info('Accessing %r giving %r', self.public_name, value)
        return value
    
    def __set__(self, obj, value):
        logging.info('Updating %r to %r', self.public_name, value)
        setattr(obj, self.private_name, value)

class ModelValidator(ABC):
    def __set_name__(self, owner, name):
        self.public_name = name
        self.private_name = "_" + name

    def __get__(self, obj, objType=None):
        value = getattr(obj, self.private_name)
        logging.info("The object validation %r to %r is pass", self.public_name, value)
        return value
    
    def __set__(self, obj, value):
        logging.info("U")


class Person:
    age = LoggedAccess() # Descriptor instance
    name = LoggedAccess()

    def __init__(self, name, age):
        self.name = name # Regular instance attribute
        self.age = age # Calls __set__()

    def birthday(self):
        self.age += 1 # Calls both __get__() and __set__()

def test_orm_model():
    first_person = Person("Erfan Huda", 29)
    assert first_person.name == "Erfan Huda"

class ImmutableValidator(ABC):
    __slots__= ("name", "result")

    def __set_name__(self, owner, name):
        self.public_name = name
        self.private_name = "_" + name

    def __get__(self, obj, objType=None):
        pass
        
    def __set__(self, obj, value):
        if self.public_name not in self.__slots__:
            raise TypeError("Expected instantiated only %r", self.__slots__)
        
    @abstractmethod
    def validate(self, value):
        if not isinstance(value, str):
            raise TypeError("Expected string but got %r", type(value))


class Age:
    __slots__ = ("_name", "_result")

    def __init__(self, name, result=[]):
        self._name = name
        self._result = result

    @property
    def name(self):
        return self._name

    @property
    def result(self):
        return self._result

    def validate(self, value):
        pass

def logger(func):
    import logging

    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        func(*args, **kwargs)
        
    return wrapper

def timer(func):
    import timeit

    @wraps(func)
    def wrapper(*args, **kwargs):
        t1 = timeit.timeit()
        func(*args, **kwargs)
        t2 = timeit.timeit() - t1

        return t2
    return wrapper


def retry(func):
    import time
    max_retries = 10

    @wraps
    def wrapper(*args, **kwargs):
        for t in range(max_retries):
            try:
                time.sleep(0.3)
                func(*args, **kwargs)
                break
            except BaseException:
                continue

    return wrapper

def enpickle(obj, file):
    import pickle
    import timeit

    t1 = timeit.timeit()
    with open(file, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    t2 = timeit.timeit() - t1

    print("Successfully pickling the object \"{}\" in file \"{}\" running in {}secs".format(obj,file, t2))


def unpickle(file):
    import pickle
    import timeit

    t1 = timeit.timeit()
    with open(file, "rb") as f:
        obj = pickle.load(f)
    t2 = timeit.timeit() - t1

    print("Successfully unpickle the file \"{}\" running in {}secs".format(file, t2))

    return obj

def m_transpose(matrix):
    return [[row[i] for row in matrix] for i in range(0,len(matrix))]

def m_mult(A, B):
    return [[sum(a * b for a, b in zip(A_row, B_col)) for B_col in zip(*B)] for A_row in A]

def main():
    m1 = Matrix([[[1,3,3,4], [2,2,2,2], [3,3,3,3], [4,4,4,4]],[[1,3,3,4], [2,2,2,2], [3,3,3,3], [4,4,4,4]]])
    m2 = Matrix([[1,3,3,4], [2,2,2,2], [3,3,3,1]])
    print(m1, "\n", m2.size)


if "__main__" == __name__:
    main()