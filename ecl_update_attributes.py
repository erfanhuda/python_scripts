# import psycopg2
# import time

# start = time.time()

# conn = psycopg2.connect(
#     host="localhost",
#     database="finance_project",
#     user="postgres",
#     password="erfan123")
# conn.autocommit = True

# myFile = "update_ecl_bucketcif_202309.txt"
# namaTable = "bv_update_ecl_bucket_cif_lfam"

# Flag = 0
# cur = conn.cursor()

# with open(myFile, 'r') as f:
#     next(f)
#     cur.copy_from(f, namaTable, sep='|', null="")  # null values become ''


# conn.commit()

# end = time.time()
# print(end - start)



from pathlib import Path
import pandas as pd

BASE = "D:\\Dev\\Seabank\\python_scripts\\file\\input\\model_Q124"
OUTPUT = "D:\\Dev\\Seabank\\python_scripts\\file\\input\\model_Q124\\output"
MEV_DATA = "seabank_data.csv"
ODR_MOM = "pd_odr_mom.csv"
ODR_QOQ = "pd_odr_qoq.csv"
ODR_HOH = "pd_odr_hoh.csv"
ODR_YOY = "pd_odr_yoy.csv"

input_path = Path().joinpath(BASE, MEV_DATA)
df = pd.read_csv(input_path, delimiter=",")
print(df)