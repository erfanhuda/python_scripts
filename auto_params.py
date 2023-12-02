import pandas as pd
import itertools

# date = [ x for x in pd.date_range(start="2021-07-31", end="2023-06-30", freq="M").strftime("%Y/%m/%d")]
# AKL 1,2,3,4,6,9
# APL 3
# EAL 2,4,6,9
# RCL 1,2,3,4
# DRL 1,2,3,6
# SME 12,24,36,60
# UDL 3,6,12,24,36
# SCF, PYL, EML, KPL


# DATE : 2023-11-28
# RCL : [1,2,3,4]
# AKL : [1,2,3,6,9]
# EAL : [2,4,6,9]
# PYL : [3,6,12,18]
# UDL : [3,6,12]
# APL : [3,0]
# SCF : [0]
# SME : [0]

products = ["SCF"]
tenor = [24]
buckets = range(1, 6)
mob = range(1, 37)

result = itertools.product(products, tenor, buckets, mob)
result = pd.DataFrame(list(result), columns=[
                      "product", "tenor", "bucket", "period"])

result.to_csv("./file/csv_templates_2.csv",
              mode="a", index=False, header=False)
