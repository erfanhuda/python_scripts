import psycopg2
from pandas_datareader import wb
import pandas_datareader as pdr

LOCAL_PSQL = {
    'user': 'superadmin',
    'password': 'Erfnhd123',
    'host': 'localhost',
    'port': '5432',
    'dbname': 'workspace'
}

SEABANK_PSQL = {
    'user': 'dev_erfan',
    'password': 'G#e6HtXEJKM',
    'host': '10.162.36.231',
    'port': '6608',
    'dbname': ''
}

str_conn = str(LOCAL_PSQL)
conn = psycopg2.connect(str_conn.replace("\'","").replace(",","").replace("{","").replace("}","").replace(":","="))
conn.autocommit = True

cursor = conn.cursor()
cursor.execute("SELECT * FROM local_research.mev_header;")

data = pdr.DataReader('TUD', 'oecd')
print(data)