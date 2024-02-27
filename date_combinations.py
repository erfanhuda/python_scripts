from itertools import product
import pandas as pd


OUTPUT_PATH = 'C:\\Users\\muhammad.huda\\Documents\\python\\python_scripts\\file\\test_emi_dates.csv'
date_range = pd.date_range("2022-01-31", "2023-12-31",
                           freq="m").to_list()
final_date = [*date_range, 'null']
final = product(final_date, repeat=3)
output_data = [x for x in final]

df = pd.DataFrame(output_data, columns=[
                  'WO_MONTH', 'CLAWBACK_MONTH', 'RECOVERY_MONTH'])
df.to_csv(OUTPUT_PATH, sep='|')
