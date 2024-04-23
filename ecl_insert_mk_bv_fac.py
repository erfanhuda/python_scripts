import psycopg2
import time

start = time.time()

conn = psycopg2.connect(
    host="localhost",
    database="Seabank",
    user="postgres",
    password="admin")
conn.autocommit = True

myFile = "MK BV FAC 2023-09.txt"
namaTable = "bv_ecl_data_mk_bv_fac"

Flag = 0
cur = conn.cursor()

with open(myFile, 'r') as f:
    next(f)
    cur.copy_from(f, namaTable, sep='|', null="")  # null values become ''


conn.commit()

end = time.time()
print(end - start)
