import pandas as pd
import itertools
from typing import Optional
from collections.abc import Iterable, Iterator
from typing import overload
from typing_extensions import Self, TypeAlias
from abc import ABC
import json

# date = [ x for x in pd.date_range(start="2021-07-31", end="2023-06-30", freq="M").strftime("%Y/%m/%d")]

# DATE : 2024-01-24
# SPL : [1,3,6,12,18,24]
# BCL : [3,6,12]
# SCL : [3,6,12]
# KPL : [1 - 180] -- 276 periods
# EML : [1 - 60] -- 276 periods
# RCL : [1,2,3,4]
# AKL : [1,2,3,6,9]
# EAL : [2,4,6,9]
# PYL : [3,6,12,18,24] -- 276 periods
# UDL : [3,6,12]
# APL : [3]
# SCF : [32]
# SME : [24]

# STARTED DATE
# SPL 3, 6, 12 : 2021-08-31
# BCL 2 : 2021-11-30
# BCL 3 : 2021-12-31
# BCL 6 : 2022-01-31
# BCL 12 : 2022-01-31
# SPL 1 : 2021-12-31
# EML 60 : 2022-10-31

# _Pr: str
# _Sg: str
# _Te: list[int] = []
# _Bu: tuple[int]
# _Mo: tuple[int]

class _L:
    def __init__(self, name="", *args):
        self._name = name
        self._value = args[0]

    def to_json(self):
        return self.__dict__

class _H:
    def __init__(self, name="", *args):
        self._name = name
        self._value = args[0]
        self.children = []

    def add(self, child):
        self.children.append(child)

    def remove(self, child):
        self.children.remove(child)

    def to_json(self):
        # print(json.dumps({self._name: self._value}, indent=1))
        result = {self._name: self._value, "detail": [k.to_json() for k in self.children]}
        # for child in self.children:
        #     # json.dump("\t", end="")
        #     result.append(child.to_json())

        return result


class AutoParams:
    """Autoparams infer the parameter list for each data types"""
    def __init__(self, columns: Optional[list] = [], data: Optional[list] = []):
        self._mode = None
        self._filename = None
        self._columns = columns
        self._data = data
        self._result = []
        self._config = {"products": "", }

        if len(columns) != len(data):
            raise ValueError

    def remove_default_number(self, col:list, default_number: Optional[int] = 1) -> None:
        if default_number == 0:
            self._columns.append(col)

    def set(self, name: str, data: list) -> None:
        self._columns.append(name)

    def run(self):
        data = itertools.product(*self._data)
        self._result = list(data)
    
    def export_to_csv(self, mode, filename):
        self.run()

        result = pd.DataFrame(list(self._result), columns=[x for x in self._columns])
        result.to_csv(mode=mode, path_or_buf=filename, index=False, header=False)


# def generate_cohort_template(mode: str, filename: str = None) -> None:
#     result = itertools.product(date_range, products, tenor, buckets, mob)
#     result = pd.DataFrame(list(result), columns=[
#         "pt_date", "product", "tenor", "ecl_bucket", "period"])

#     result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)

# def generate_with_scenario(mode: str, filename: str = None) -> None:
#     result = itertools.product(date_range, tenor, payment_freq, flow_rate_matrix)
#     result = pd.DataFrame(list(result), columns=[
#         "product", "tenor", "payment_freq", "matrix"])

#     result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)

# def generate_without_scenario(mode: str, filename: str = None) -> None:
#     result = itertools.product(products, 0, mob, buckets)
#     result = pd.DataFrame(list(result), columns=[
#                           "pt_date", "product", "tenor", "bucket"])

#     result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)

# def generate_cohort_pd(mode: str, filename: str = None) -> None:
#     result = itertools.product(["2024-02-29"], products, tenor, buckets, mob)
#     result = pd.DataFrame(list(result), columns=[
#                           "pt_date", "product", "tenor", "bucket", "period"])

#     result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)

# def generate_cohort_comparison_model(mode: str, filename: str = None) -> None:
#     result = itertools.product(products, tenor, buckets, models, mob)
#     result = pd.DataFrame(list(result), columns=["product", "tenor", "bucket", "model", "period"])

#     result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)

if __name__ == "__main__":
    ch_segments = ["Channeling"]
    ch_products = ["AKL", "APL", "EAL", "RCL", "KPL"]
    ddl_segments = ["DDL"]
    ddl_products = ["EML", "PYL", "SME", "SCF", "UDL"]
    non_tenor_product = ["EML", "PYL", "KPL", "SME", "SCF", "UDL"]
    tenor_product = ["AKL", "APL", "EAL", "RCL", "KPL"]
    product = ["APL"]

    scenario = ['BASE', 'BEST', 'WORST']
    # date_range = [x for x in pd.date_range(start="2021-06-30", end="2024-02-29", freq="M").strftime("%Y/%m/%d")]
    flow_rate_matrix = ['Current - Current', 'Current - M1', 'Current - M2', 'Disburse - Current', 'Disburse - M1', 'Disburse - M2', 'M1 - M2', 'M1 - M3', 'M2 - M3', 'M2 - M4', 'M3 - M4', 'M3 - M5', 'M4 - M5', 'M4 - M6', 'M5 - M6', 'M6 - WO']
    tenor = [""]
    payment_freq = [1]
    buckets = range(1, 6)
    mob = range(1, 13)
    models = ['Regional Model', 'SBID Cohort Model', 'SBID TTC YOY Model']
    FILENAME = "./file/pivot_channeling/csv_templates_channeling_pivot_tenor.csv"

    # Mode "a" for append, "w" for new writing
    # generate_cohort_comparison_model(mode="a", filename=FILENAME)
    # final = itertools.product(ch_segments, non_tenor_product, tenor)
    # final = pd.DataFrame(list(final), columns=["Segments", "Products", "Tenor"])
    # final.to_csv(mode="a", path_or_buf=FILENAME, index=False, header=False)

    # cohort_comparison_model = AutoParams(["Products", "Scenario"], [products, scenario])
    # cohort_comparison_model.export_to_csv(mode="a", filename=FILENAME)

    AKL = _H("Product", "AKL")
    Tenor1 = _H("Tenor", "1M")
    Pay1 = _H("Payment Freq", "1")
    Date1 = _L("Date Start", "2024-03-31")
    Pay2 = _L("Payment Freq","2")

    Pay1.add(Date1)
    Tenor1.add(Pay1)
    Tenor1.add(Pay2)
    AKL.add(Tenor1)
    print(json.dumps(AKL.to_json(), indent=3))