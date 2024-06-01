from collections import defaultdict
import pandas as pd
import numpy as np
from typing import Any, Optional
from datetime import datetime
import itertools
from abc import ABC, abstractmethod
import sys
import functools
from dataclasses import dataclass

class BaseConnection(object):
    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_value, trace):
        ...

def context_manager(obj: BaseConnection, exp: str, do_something: str):
    manager = exp
    enter = type(obj).__enter__
    exit = type(obj).__exit__
    value = enter(obj)
    hit_except = False

    try:
        TARGET = value
        do_something
    except:
        hit_except = True
        if not exit(manager, *sys.exc_info()):
            raise
    finally:
        if not hit_except:
            exit(manager, None, None, None)

class BaseReader(ABC):
    def __close__(self):
        ...

    @abstractmethod
    def to_dataframe(self, data: str):
        ...

class ExcelReader:
    __DEFAULT_ENGINE_READER = ""

    def to_dataframe(self, data: str):
        data = pd.read_excel(data)
        return data

class SQLReader:
    __DEFAULT_ENGINE_READER = ""

    def to_dataframe(self, query: str):
        data = pd.read_sql(query)
        return data

class CSVReader:
    __DEFAULT_ENGINE_READER = ""

    def to_dataframe(self, path: str):
        data = pd.read_csv(path)
        return data

class Parameters(object):
    __name__ = 'Parameters'
    
    def __get__(self, instance, owner):
        return instance.__dict__[self._name]

    def __set__(self, instance, value):
        instance.__dict__[self._name] = value
    
    def __getattr__(self, name: str):
        try:
            return self.__dict__[f"_{name}"]
        except KeyError:
            return f"The value of _{name} is not found. Try to setup this attribute."

    def __setitem__(self, name, value):
        setattr(self, name, value)

    def __getitem__(self, name):
        return getattr(self, name)

    def __setattr__(self, name: str, value: Any):
        self.__dict__[name] = value

    def __delattr__(self, name: str):
        del self.__dict__[name]

    def __repr__(self):
        return str(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__)

    def items(self):
        return iter(self.__dict__.items())

    def values(self):
        return iter(self.__dict__.values())

    def keys(self):
        return iter(self.__dict__.keys())

    def to_dataframe(self):
        return pd.DataFrame(data={'keys': [x for x in self.__dict__.keys()], "values":[x for x in self.__dict__.values()]}, index=[x for x in range(0,len(self.__dict__))])

class AdvancedPandas:
    RUNNING_DATE = datetime.now()
    _GLOBAL_Parameters = Parameters()

    def __init__(self, args: Parameters =None, data: Optional[pd.DataFrame | pd.Series] = None):
        if data is not None:
            self.__infer_pandas(data)
        else:
            self.__df = pd.DataFrame()
        
        self.__args = args

    @classmethod
    def __running_time(cls):
        cls.__RESULT = cls.RUNNING_DATE - datetime.now()

    def __infer_pandas(self, data):
        if type(data) == pd.DataFrame:
            self.__df = pd.DataFrame()
        elif type(data) == pd.Series:
            self.__df = pd.Series()
        else:
            raise ValueError("Data should be Pandas Series or Pandas Dataframe")

    @property
    def args(self):
        return self.__args.__repr__()

    @args.setter
    def args(self, name, value):
        self.__args[name] = value
        
    @property
    def data(self):
        return self.__df

    @data.setter
    def data(self, value: Optional[pd.DataFrame | pd.Series]):
        del self.__df
        self.__df = value

    def __repr__(self):
        return str({k: v for k, v in self.__dict__.items() if k not in ('_AdvancedPandas__args', '_AdvancedPandas__df')})

    def __str__(self):
        return self.__repr__()

    def from_reader(self, data: BaseReader):
        ...
        
class Transformation:
    @classmethod
    def z_score(value: list) -> int:
        """Z-Score for ODR"""
        return [(value - value.mean()) / value.std(ddof=1)]

    @classmethod
    def standardize(value: list) -> list[int]:
        """Standardize formula for Macroeconomic variables"""
        return [(value - value.mean()) / value.std(ddof=1)]

    @classmethod
    def log10(value: list[int]) -> list[int]:
        return np.log10(value)

    @classmethod
    def adj_r2_score(value: list) -> list:
        """Adjusted-R2 for model"""
        ...

class Regressor:
    RUNNING_DATE = datetime.now()
    _GLOBAL_Parameters = Parameters()

    def __init__(self, param: Parameters = None):
        self.__df = AdvancedPandas()
        self.__args = param
        self.__multiple = False
        self.__many = 1
        self.__timeseries = False
        self.__start = None
        self.__end = None

    @classmethod
    def __running_time(cls):
        cls.__RESULT = cls.RUNNING_DATE - datetime.now()

    def __add_x_columns(self):
        new_columns = []
        if (self.__multiple is False):
            for x in range(1, 2):
                new_columns.append(f"x_{x}")

            for x in range(1, 2):
                new_columns.append(f"p_value_x{x}")
                new_columns.append(f"bpe_x{x}")

        elif (self.__multiple is True):
            n = itertools.combinations(range(1, self.__many + 1),r=2)

            for x in range(1, self.__many + 1):
                new_columns.append(f"x_{x}")

            for x in list(n):
                new_columns.append(f"corr_{x[0]}_{x[1]}")

            for x in range(1, self.__many + 1):
                new_columns.append(f"p_value_x{x}")
                new_columns.append(f"bpe_x{x}")

        self.__df.data = self.__df.data.reindex(columns=self.__df.data.columns.tolist() + new_columns)

    @property
    def args(self):
        return self.__args

    @args.setter
    def args(self, name, value):
        self.__args[name] = value

    @property
    def data(self):
        self.__add_x_columns()
        return self.__df.data

    @data.setter
    def data(self, data):
        self.__df.data = data

    @property
    def multiple(self):
        return bool(self.__multiple)

    @multiple.setter
    def multiple(self, value):
        self.__multiple = value

    @property
    def many(self):
        return self.__many

    @many.setter
    def many(self, value: int = 0):
        self.__many = value

    @property
    def timeseries(self):
        return self.__timeseries

class TimeseriesRegressor(Regressor):
    def __init__(self):
        super().__init__(self)
        self.__start_date = pd.to_datetime("2000-01-01")
        self.__end_date = pd.to_datetime("2000-01-02")
    
    @property
    def start(self):
        return self.__start_date

    @start.setter
    def start(self, value):
        self.__start_date = value
        self.__add_index()

    @property
    def end(self):
        return self.__end_date

    @end.setter
    def end(self, value):
        self.__end_date = value
        self.__add_index()

    def __add_index(self):
        self.data.set_index([x for x in pd.date_range(start=self.__start_date, end=self.__end_date, periods=1,freq="M")])

def adder(x, y):
    return x + y

if __name__ == "__main__":
    regr1 = Parameters()
    regr1.start = '2024-04-01'
    regr1.end = '2024-04-30'
    regr1.p_value = '0.05'
    regr1.t_value = '0.05'
    regr1.p_value = '0.05'
    regr1.p_value = '0.05'
    # regr1['t_value'] = '0.011'
    regr1.name = "Erfan"
    
    regr2 = Parameters()
    regr2.name = 'YOY'
    regr2.start = '2024-04-02'
    regr2.end = '2024-04-30'
    regr2.p_value = '0.05'
    regr2.many = 2
    regr2.multiple = True
    regr2.data = pd.DataFrame(columns=["id", "product", "tenor"])
    regr2.keys = [x for x in regr2.get_keys()]
    regr1 = Parameters()
    regr1 = Parameters()
    regr1.add_method = adder(1,2)