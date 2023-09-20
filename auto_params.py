import pandas as pd
import itertools

# INPUT VARIABLES
date = [ x for x in pd.date_range(start="2021-07-31", end="2023-06-30", freq="M").strftime("%Y/%m/%d")]
scenario = ['BASE', 'BEST', 'WORST']
products = ["Digital_SPL"]
tenor = [18,24]
buckets = range(1, 6)
mob = range(1, 37)

result = itertools.product(scenario,products,tenor,mob, buckets)
result = pd.DataFrame(list(result), columns=["scenario","pd_segment", "tenor", "period", "ecl_bucket_client"])
result.to_csv("./file/csv_templates.csv", mode="a", index=False,header=False)