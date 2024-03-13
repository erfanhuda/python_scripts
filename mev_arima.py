import blpapi
from xbbg import blp
import pandas_datareader.data as web
from pandas_datareader import wb
from datetime import datetime
import requests
import urllib3
from dataclasses import dataclass, asdict
from typing import Optional

import psycopg2


session = requests.Session()
session.verify = False

@dataclass
class QueryAPI:
    url: Optional[str] = None
    platform: Optional[str] = 'oecd'
    start: Optional[datetime] = datetime(2014,1,1)
    end: Optional[datetime] = datetime.now()
    session: Optional[object] = session

class AuthBloomberg(blpapi.AuthUser):
    def __init__(self, name, ip):
        super().__init__(blpapi.AuthUser)
        self.name = name
        self.ip = ip
        self.createWithLogonName(self.name, self.ip)

def query_gdp_wb(file: str, tickers: list, ) -> None:
    urllib3.disable_warnings()
    DIRS = './file/'
    tickers = ['APPL US Equity']
    fields = ['High','Low','Last_Price']


def main():
    # auth = blpapi.AuthUser().createWithLogonName()

    urllib3.disable_warnings()

    DIRS = './file/'

    tickers = ['APPL US Equity']
    fields = ['High', 'Low', 'Last_Price']
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)

    # hist_tick_data = blp.bdh(tickers=tickers, flds=fields,
    #                          start_date=start_date, end_date=end_date)

    # filename = f'tick_data_{start_date}_{end_date}'

    # hist_tick_data.to_csv(DIRS + filename)
    wb_gdp = wb.download(indicator='NY.GDP.MKTP.KD.ZG', country=[
        'ID'], start=2005, end=2008, freq="Q", session=session)

    oecd_gdp = web.DataReader(
        'QNA/IDN.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+GCUMSA.Q/all?', 'oecd', start=start_date, end=end_date, session=session)

    print(oecd_gdp.to_dict)

def insert_to_local(data=None):
    conn = psycopg2.connect (host="localhost", database = "finance_project", user = "postgres", password = "Erfnhd123890")
    conn.autocommit = True 
    table_name = 'local.mev_variables_header'
    table_detail = 'local.mev_variables_detail'
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    print(cursor.fetchall())


if __name__ == '__main__':
    main()
    # insert_to_local()