import pandas as pd

from sqlalchemy import create_engine
import psycopg2 
import io

import numpy as np

import time

start = time.time()

conn = psycopg2.connect (
    host="localhost",
    database = "finance_project",
    user = "postgres",
    password = "erfan123")
conn.autocommit = True 

myFile = "update_ecl_bucketcif_202309.txt"
namaTable = "bv_update_ecl_bucket_cif_lfam"

Flag = 0
cur = conn.cursor()

with open (myFile , 'r') as f:
    next(f)
    cur.copy_from(f, namaTable , sep = '|' , null="") # null values become ''
    
       
conn.commit()

end = time.time()
print(end - start)

