import logging
import pandas as pd
import csv
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

start_time = time.time()
logging.info(
    f"Start execution.")

START_DATE = {"BCL 2": pd.to_datetime("2021-11-30"), "BCL 3": pd.to_datetime("2021-11-30"), "BCL 6": pd.to_datetime("2021-11-30"), "BCL 12": pd.to_datetime("2021-11-30"), "SCL 3": pd.to_datetime("2022-05-31"), "SCL 6": pd.to_datetime("2022-05-31"), "SCL 12": pd.to_datetime("2022-05-31"), "SPL 1": pd.to_datetime("2021-10-31"),"SPL 3": pd.to_datetime("2021-06-30"), "SPL 6": pd.to_datetime("2021-06-30"),"SPL 12": pd.to_datetime("2021-06-30"), "SPL 18": pd.to_datetime("2023-07-31"),"SPL 24": pd.to_datetime("2023-07-31")}
SOURCE_INTERNAL = [{"product_type": str(k).split(" ")[0], "tenor": str(k).split(" ")[1],"disb_month_adj": v} for k, v in START_DATE.items()]
SOURCE_PATH = "C:/Users/muhammad.huda/Downloads/wo_dates.csv"
OUTPUT_PATH = "C:/Users/muhammad.huda/Downloads/wo_disb_combs.csv"

dates = pd.date_range("2022-01-31", "2024-05-31", freq="m").to_list()
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

# df = pd.DataFrame(output)
# df.to_csv(OUTPUT_PATH, sep="|")

end_time = time.time()

logging.info(
    f"Done execution.")
logging.info(
    f"Duration execution {end_time - start_time} seconds.")

# print(SOURCE_INTERNAL)
print(input_file)

SOURCE = [{x['disb_month_adj']: pd.date_range(x['disb_month_adj'], pd.to_datetime("2024-04-30"), freq="M")} for x in SOURCE_INTERNAL]

print(SOURCE[0])