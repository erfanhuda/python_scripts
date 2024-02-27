import pandas as pd

from sqlalchemy import create_engine
import psycopg2 
import io

import numpy as np

import time

start = time.time()

conn = psycopg2.connect (
    host="localhost",
    database = "Seabank",
    user = "postgres",
    password = "admin")
conn.autocommit = True 

myFile = "MK BV FAC 2023-09.txt"
namaTable = "bv_ecl_data_mk_bv_fac"

Flag = 0
cur = conn.cursor()

with open (myFile , 'r') as f:
    next(f)
    cur.copy_from(f, namaTable , sep = '|' , null="") # null values become ''
    
       
conn.commit()

end = time.time()
print(end - start)

