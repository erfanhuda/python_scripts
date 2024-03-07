import pandas as pd
import itertools

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

def generate_with_scenario(mode: str, filename: str = None) -> None:
    result = itertools.product(scenario, products, tenor, mob, buckets)
    result = pd.DataFrame(list(result), columns=[
        "scenario", "product", "tenor", "period", "bucket"])

    result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)


def generate_without_scenario(mode: str, filename: str = None) -> None:
    result = itertools.product(products, tenor, mob, buckets)
    result = pd.DataFrame(list(result), columns=[
                          "product", "tenor", "period", "bucket"])

    result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)


if __name__ == "__main__":
    products = ["Digital_KPL"]
    scenario = ['BASE', 'BEST', 'WORST']
    tenor = range(1, 181)
    buckets = range(1, 6)
    mob = range(1, 277)
    FILENAME = "./file/csv_templates.csv"

    # Mode "a" for append, "w" for new writing
    generate_with_scenario(mode="a", filename=FILENAME)
