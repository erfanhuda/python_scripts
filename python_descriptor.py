import os

class DirectorySize:
    """ Descriptor Dynamic Lookups"""
    def __get__(self, obj, objtype=None):
        return len(os.listdir(obj.dirname))

class Directory:
    size = DirectorySize() # Descriptor instance
    def __init__(self, dirname):
        self.dirname = dirname

s = Directory("src")
g = Directory("node_modules")


import logging
logging.basicConfig(level=logging.INFO)

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

class Person:
    age = LoggedAccess() # Descriptor instance
    name = LoggedAccess()

    def __init__(self, name, age):
        self.name = name # Regular instance attribute
        self.age = age # Calls __set__()

    def birthday(self):
        self.age += 1 # Calls both __get__() and __set__()

first_person = Person("Erfan Huda", 29)