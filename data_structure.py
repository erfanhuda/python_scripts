"""
1. Create class to accept the result of testing arima
2. Create class to accept the input parameters and add method to processing the list of class of result testing
3. Create empty list for accept the result objects.

1. Process the input of csv file to hold in a list by sorting the datetime index. Hold the datetime index frequency cycle.
2. Do preprocessing statistical tests, by inputing the parameters e.g. null values method, normality treshold, other type of variable transformation.
2A. Evaluate the null value by default use interpolation method, default method are: backward value, forward value.
2B. If the normality test didnt pass by out range of more than 50% of data rows. Then, do logarithmic natural by default, if do the same thing performs other transformation type, e.g. logarithmic natural, moving average 12M, z-score, exponential, and others.
3. Process AUTO ARIMA procedures, by find the best orders P, D, Q, selecting top 5 orders by p-value.
4. Process other ARIMA procedures, by find the best orders default starting orders are: 0, 0, 0 to 5, 5, 5.
5. Order the result, by default using p-value for ARIMA, AIC, BIC.
6. If parameter in class parameter to choose manual, then stop process here and throw the result in the file or excel.
"""
from abc import ABC
import argparse
import platform
import sys
import csv
import pathlib

class InterfaceStatResult(ABC):
    def set_flag():
        raise NotImplementedError("The method not been implemented yet")

class ArimaResult(InterfaceStatResult):
    def __init__(self, **kwargs):
        self.result = kwargs
        self.isFlag = None

    def set_flag(self, flag: bool):
        if isinstance(flag, bool):
            self.isFlag = flag
        else:
            raise ValueError("Only boolean values are supported")

class ParameterParser:
    def __init__(self, **kwargs):
        self.parameters = kwargs

class InputHandler:
    def __init__(self):
        self.date_index = []
        self.values = []
        self._type = None
    
    def csv(self, file):
        try:
            with open(file) as f:
                content = f.read()
                result = csv.DictReader({"date": self.date_index, "data": self.values})

            for item in content:
                self.date_index.append(item[0])
                self.values.append(item[1:])

            self._type = result.dialect

            return result.reader

        except ReferenceError as e:
            print("Kindly check again", e) 

def input_handler():
    input_file = InputHandler()
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True)    
    cmd_list = parser.parse_args()

    current_path = pathlib.Path().cwd()
    parser = input_file.csv(current_path.joinpath(cmd_list.file))

    for item in parser:
        return item

def main():
    input_file = input_handler()

    match platform.system():
        case "Windows":
            try:
                print(input_file)
            except ValueError as e:
                print("Kindly check again the your file path.")

if __name__ == "__main__":
    main()