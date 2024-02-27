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
    password = "Erfnhd123890")
conn.autocommit = True 

myFile = "bv_repayment_schedule_vFinpro.csv"
namaTable = "bv_ecl_data_repayment_schedule"

Flag = 0
cur = conn.cursor()

with open (myFile , 'r') as f:
    next(f)
    cur.copy_from(f, namaTable , sep = ',' , null="NULL") # null values become ''
    
       
conn.commit()

end = time.time()
print(end - start)
