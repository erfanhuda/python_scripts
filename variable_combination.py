import time
import pandas as pd
import argparse
import csv
import itertools
import logging

start_time = time.time()

SOURCE_PATH = "C:/Users/muhammad.huda/Downloads/MEV.csv"
OUTPUT_PATH = "C:/Users/muhammad.huda/Downloads/combination.csv"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser()
parser.add_argument("--type", type=str, default="combinations")
parser.add_argument("--file", type=str, default=None)
parser.add_argument("--n", type=int, default=2)
args = parser.parse_args()

input_file = [*csv.DictReader(open(SOURCE_PATH, encoding="utf-8-sig"))]
first_key = next(iter(input_file[0]))
list_mev = [x[first_key] for x in input_file]


def combinations(data: list, repeat: int = 2) -> list:
    logging.info("Executing combinations ...")
    result = itertools.combinations(data, repeat)
    return [*result]


def permutations(data: list, repeat: int = 2) -> list:
    logging.info("Executing permutations ...")
    result = itertools.permutations(data, repeat)
    return [*result]


def typical_permutation(data: list, repeat: int = 2) -> list:
    logging.info("Executing typical permutations ...")
    result = itertools.product(data, repeat)
    return [*result]


output = combinations(list_mev, 5)
columns = [n + 1 for n in range(len(output[0]))]

df = pd.DataFrame(output, columns=columns)
df.to_csv(OUTPUT_PATH, sep="|")

end_time = time.time()

logging.info(
    f"Done execution.")
logging.info(
    f"Duration execution {end_time - start_time} seconds.")
