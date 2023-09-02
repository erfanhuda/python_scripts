import os
import logging
from abc import ABC, abstractmethod
from functools import wraps
logging.basicConfig(level=logging.INFO)


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

class Logger:
    def __init__(self, func):
        self.func = func

    def __call__(self):
        import timeit

        t1 = timeit.timeit()
        self.func()
        t2 = timeit.timeit() - t1

        print("The function {} took time {}".format(self.func.__name__, t2))

def logger(func):
    import logging

    @wraps(func)
    def wrapper():
        logging.basicConfig(level=logging.INFO, format='%levelname)')
        func()
        
    return wrapper

def timer(func):
    import timeit

    @wraps(func)
    def wrapper():
        t1 = timeit.timeit()
        func()
        t2 = timeit.timeit() - t1

        print("The function {} took time {}".format(func.__name__, t2))

    return wrapper

@logger
def main():
    age = Age("Erfan")
    print(age.name)


if "__main__" == __name__:
    main()