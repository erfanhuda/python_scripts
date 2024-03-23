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
    result = itertools.product(products, tenor, payment_freq, flow_rate_matrix)
    result = pd.DataFrame(list(result), columns=[
        "product", "tenor", "payment_freq", "matrix"])

    result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)


def generate_without_scenario(mode: str, filename: str = None) -> None:
    result = itertools.product(products, 0, mob, buckets)
    result = pd.DataFrame(list(result), columns=[
                          "product", "tenor", "period", "bucket"])

    result.to_csv(path_or_buf=filename, mode=mode, index=False, header=False)

if __name__ == "__main__":
    products = ["Digital_SME"]
    scenario = ['BASE', 'BEST', 'WORST']
    flow_rate_matrix = ['Current - Current', 'Current - M1', 'Current - M2', 'Disburse - Current', 'Disburse - M1', 'Disburse - M2', 'M1 - M2', 'M1 - M3', 'M2 - M3', 'M2 - M4', 'M3 - M4', 'M3 - M5', 'M4 - M5', 'M4 - M6', 'M5 - M6', 'M6 - WO']
    tenor = range(1,61)
    payment_freq = [1]
    buckets = range(1, 6)
    mob = range(1, 277)
    FILENAME = "./file/csv_templates_flow_rate.csv"

    # Mode "a" for append, "w" for new writing
    generate_with_scenario(mode="a", filename=FILENAME)
