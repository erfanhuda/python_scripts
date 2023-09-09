from abc import ABC, abstractmethod


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
    """Implement the interface class validator numbers or float type."""
    def __init__(self, minvalue=None, maxvalue=None):
        self.minvalue = minvalue
        self.maxvalue = maxvalue

    def validate(self, value):
        if not isinstance(value, (int, float)):
            raise TypeError(f'Expected {value!r} to be an int or float')
        if self.minvalue is not None and value < self.minvalue:
            raise ValueError(f'Expected {value!r} to be at least {self.minvalue!r}')
        if self.maxvalue is not None and value > self.maxvalue:
            raise ValueError(f'Expected {value!r} to be no more than {self.maxvalue!r}')
        
class String(Validator):
    """Implement the interface class validator for string, char types."""
    def __init__(self, char):
        self.char = char

    def validate(self,value):
        if not isinstance(value, (str)):
            raise TypeError(f"Expected {value!r} to be str")

        
class Dimensional(Validator):
    """Implement the interface class data types validator for array matrix with contains of number or float only."""
    def __init__(self):
        self.max_value = 3

    def two_dimensional(self, value):
        try:
            size = {"x": None, "y": None}
            for i in value:
                size["y"] = len(i)
                for j in i:
                    size["x"] = len(j)
            
            return True
        
        except ValueError as e:
            return False

        except TypeError as e:
            return False
    
    def three_dimensional(self, value):
        try:
            size = {"x": None, "y": None, "z": None}
            for i in value:
                size["z"] = len(i)
                for j in i:
                    size["y"] = len(j)
                    for k in j:
                        size["z"] = len(k)

            return True
        
        except ValueError as e:
            return False
        
        except TypeError as e:
            return False

    
    def validate(self, value):
        if isinstance(value, list):
            if not self.three_dimensional(value):
                self.two_dimensional(value)
            else: 
                raise TypeError("Value must be either two-dimensional array or three-dimensional array.")

class Map(dict):
    """
    Provide dot access to the dictionary of python. With exception that key of dictionary are strings.
    """
    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.items():
                self[k] = v

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Map, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Map, self).__delitem__(key)
        del self.__dict__[key]



class Matrix:
    """Implementing the matrix object."""
    array = Dimensional()
    def __init__(self, array=[]):
        self.array = array

    @property
    def x_size(self):
        if isinstance(self.array, list):
            return len(self.array)
        else: 
            return 0
    
    @property
    def y_size(self):
        for item in self.array:
            if isinstance(item, list):
                return len(item)
            else: 
                return 0

    @property
    def z_size(self):
        for i in self.array:
            for j in i:
                if isinstance(j, list):
                    return len(j)
                else: 
                    return 0

    @property
    def size(self):
        return {"x": self.x_size, "y": self.y_size, "z": self.z_size}

    def __len__(self):
        return len(self.array)

    def __mul__(self, other):
        self.array = [[sum(a * b for a, b in zip(A_row, B_col)) for B_col in zip(*self.array)] for A_row in other]

    def __iter__(self):
        return self.array
    
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
    


class ArrayNumber(Validator):
    def __init__(self, array=None):
        self.array = array

    def depth_validate(self, value):
        for item in self.array:
            if not isinstance(item, (int, float)):
                raise TypeError(f'Expected {value!r} to be an int or float')

    def validate(self, value):
        if self.array is not None:
            raise ValueError(f'Expected {value!r} to be at least 2-dimensional matrix {self.minvalue!r}')
        else:
            self.depth_validate(self.array)
        
        
class TestValidator:
    arr = ArrayNumber()

    def __init__(self, number):
        self.arr = number



arr1 = [[1,2,3,4,5], [2,3,4,5,6]]
arr2 = [1,2,3,4,5]
arr3 = [[1,2,3,4,5]]


def depth_check(array):
    column = 0
    for index, value in enumerate(array):
        if isinstance(value, list):
            column += 1
            depth_check(value)
        elif isinstance(value, (float, int)):
            break

    return column


print(depth_check(arr1))
# print(len(arr3))