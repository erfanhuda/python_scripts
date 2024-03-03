import pandas as pd
import blpapi
from xbbg import blp

BPS_API = '4c76a1866016bcc42c192db1517a9fdd'


DATA_DIR = './file/'
tickers = ['APPL US Equity']
fields = ['High', 'Low', 'Last Price']
start_date = '2023-01-01'
end_date = '2023-12-31'

hist_tick = blp.bdh(tickers=tickers, flds=fields,
                    start_date=start_date, end_date=end_date)

filename = f'tick_data_{start_date}_{end_date}.csv'
hist_tick.to_csv(DATA_DIR + filename)
