import blpapi
from xbbg import blp
import pandas_datareader.data as web
from pandas_datareader import wb
from datetime import datetime
import requests
import urllib3


class AuthBloomberg(blpapi.AuthUser):
    def __init__(self, name, ip):
        super().__init__(blpapi.AuthUser)
        self.name = name
        self.ip = ip
        self.createWithLogonName(self.name, self.ip)


def main():
    # auth = blpapi.AuthUser().createWithLogonName()

    urllib3.disable_warnings()

    session = requests.Session()
    session.verify = False

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

    print(wb_gdp)


if __name__ == '__main__':
    main()
