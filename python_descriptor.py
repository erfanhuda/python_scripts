from collections import Counter
import os
import logging
from abc import ABC, abstractmethod
from functools import wraps
from sys import getsizeof
import pandas as pd
from typing import Any

from custom_data_types import Matrix

class Parameters:
    def __get_attr__(self, name: str):
        try:
            return self.__dict__[f"_{name}"]
        except KeyError:
            return "The value of _{name} is not found. Try to setup this attribute."

    def __set_attr__(self, name: str, value: Any):
        self.__dict__[name] = value

    def __repr__(self):
        return str(self.__dict__)

    def insert_row(self):
        ...

    def to_dataframe(self):
        return pd.DataFrame(data={'keys': [x for x in self.__dict__.keys()], "values":[x for x in self.__dict__.values()]}, index=[x for x in range(0,len(self.__dict__))])

class AdvancedDataFrame:
    def __init__(self):
        self.__df = pd.DataFrame()
        self.__args = Parameters()
        
    @property
    def data(self):
        return self.__df

    def __get_attr__(self, name: str):
        try:
            return self.__args[f"_{name}"]
        except KeyError:
            return "The value of _{name} is not found. Try to setup this attribute."

    def __set_attr__(self, name: str, value: Any):
        self.__args[name] = value

    def __repr__(self):
        return str({k: v for k, v in self.__dict__.items() if k not in ('_AdvancedDataFrame__args', '_AdvancedDataFrame__df')})

    def __str__(self):
        return self.__repr__()

    def __dataframe__(self):
        return self.__df
class Regression:
    RUN_START_DATE: pd.DatetimeIndex
    RUN_END_DATE: pd.DatetimeIndex
    
    def __init__(self, name):
        self._df = pd.DataFrame(columns=['name', 'contact'])

    @property
    def start_date(self):
        self._df['start_date'] = self._start_date
        return self._df['start_date']

    @start_date.setter
    def start_date(self, value):
        self._start_date = pd.to_datetime(value)

    @start_date.getter
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        self._df['end_date'] = self._end_date
        return self._df['end_date']

    @end_date.setter
    def end_date(self, value):
        self._end_date = pd.to_datetime(value)

    @end_date.getter
    def end_date(self):
        return self._end_date
        
class DirectorySize:
    """ Descriptor Dynamic Lookups"""
    def __get__(self, obj, objtype=None):
        return len(os.listdir(obj.dirname))

class Directory:
    size = DirectorySize() # Descriptor instance
    def __init__(self, dirname):
        self.dirname = dirname

class Validator(ABC):
    """Abstract class for validate type"""
    def __set_name__(self, owner, name):
        self.private_name = "_" + name

    def __get__(self, obj, objType=None):
        return getattr(obj, self.private_name)
    

    def __set__(self, obj, value):
        self.validate(value)
        setattr(obj, self.private_name, value)

    @abstractmethod
    def validate(self, value):
        pass

class Number(Validator):
    """Implement the interface class for validator numbers or float type."""
    def __init__(self, minValue=None, maxValue=None):
        self.minvalue = minValue
        self.maxvalue = maxValue

    def validate(self, value):
        if not isinstance(value, (int, float)):
            raise TypeError(f'Expected {value!r} to be an int or float')
        if self.minvalue is not None and value < self.minvalue:
            raise ValueError(f'Expected {value!r} to be at least {self.minvalue!r}')
        if self.maxvalue is not None and value > self.maxvalue:
            raise ValueError(f'Expected {value!r} to be no more than {self.maxvalue!r}')
        
class String(Validator):
    """Implement the interface class for string, char types."""
    def __init__(self, char):
        self.char = char

    def validate(self,value):
        if not isinstance(value, (str)):
            raise TypeError(f"Expected {value!r} to be str")
        
class Dimensional(Validator):
    """Implement the interface class for array matrix with contains of number or float only."""

    def validate(self, value):
        length = []
        for i, row in enumerate(value):
            length.append({"col": len(row)})

        if not all(x == length[0] for x in length):
            raise TypeError(f"Cannot build the matrix. Expected same length column.")

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


class Matrix:
    """Implementing the matrix object."""
    array = Dimensional()
    def __init__(self, array=[]):
        self.array = array

    @property
    def length(self):
        length = []
        for i, row in enumerate(self.array):
            length.append({"row": i + 1, f"col_length": len(row)})
        
        return length
    
    @property
    def dimension(self):
        return "{} X {}".format(self.length[-1]['row'], self.length[-1]['col_length'])

    def __len__(self):
        return len(self.array)

    def __mul__(self, other):
        self.array = [[sum(a * b for a, b in zip(A_row, B_col)) for B_col in zip(*self.array)] for A_row in other]

    def __iter__(self):
        return self.array

    def __getitem__(self, idx):
        return self.array[idx]
    
    def __setitem__(self, idx, val):
        self.array[idx] = val
    
    def __transpose__(self):
        return [[row[i] for row in self.array] for i in range(0,len(self.array))]

    def __str__(self):
        return "{}".format(str(self.array).replace("],", "],\n"))
        
class Point:
    """Implementing the point data types"""
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    def __add__(self, other):
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other):
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, other):
        return Point(self.x * other.x, self.y * other.y)

    def __div__(self, other):
        return Point(self.x / other.x, self.y / other.y)
    
    def __str__(self):
        return f"Point({self.x}, {self.y})"

def m_transpose(matrix):
    return [[row[i] for row in matrix] for i in range(0,len(matrix))]

def m_mult(A, B):
    return [[sum(a * b for a, b in zip(A_row, B_col)) for B_col in zip(*B)] for A_row in A]


def same_property(items):
    return all(x == items[0] for x in items)

def main():
    m1 = Matrix([[1,3,3,4], [2,2,2,1], [3,3,3,3], [4,4,4,4]])
    m2 = Matrix([[1,3,3,4], [2,2,2,1,0], [3,3,3,3,4]])

    print(m1.dimension)


if "__main__" == __name__:
    main()