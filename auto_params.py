import pandas as pd
import itertools

date = [ x for x in pd.date_range(start="2021-07-31", end="2023-06-30", freq="M").strftime("%Y/%m/%d")]
scenario = ['BASE', 'BEST', 'WORST']
products = ["Digital_PYL"]
tenor = [6,12,18,24]
buckets = range(1, 6)
mob = range(1, 37)

result = itertools.product(scenario, products,tenor,mob,buckets)
result = pd.DataFrame(list(result), columns=["scenario","product","tenor","period","bucket"])
result.to_csv("./file/csv_templates_1.csv", mode="a", index=False,header=False)