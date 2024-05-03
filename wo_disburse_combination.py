import logging
import pandas as pd
import csv
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

start_time = time.time()
logging.info(
    f"Start execution.")

SOURCE_PATH = "C:/Users/muhammad.huda/Downloads/wo_dates.csv"
OUTPUT_PATH = "C:/Users/muhammad.huda/Downloads/wo_disb_combs.csv"

dates = pd.date_range("2022-01-31", "2024-04-30", freq="m").to_list()
input_file = [*csv.DictReader(open(SOURCE_PATH, encoding="utf-8-sig"))]
wo_dates = [{"wo_dates": v.strftime("%Y-%m-%d")} for v in dates]
disb_dates = [{"disb_dates": v.strftime("%Y-%m-%d")} for v in dates]

output = []
for item in input_file:
    for date in wo_dates:
        item['wo_dates'] = date['wo_dates']
        output.append(dict(item))

    for date in disb_dates:
        item['disb_dates'] = date['disb_dates']
        output.append(dict(item))

df = pd.DataFrame(output)
df.to_csv(OUTPUT_PATH, sep="|")

end_time = time.time()

logging.info(
    f"Done execution.")
logging.info(
    f"Duration execution {end_time - start_time} seconds.")
