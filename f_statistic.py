from email.policy import default
import itertools
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
import pmdarima as pm
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import statsmodels.tsa.stattools as st_tools
import statsmodels.tsa.arima.model as tsa_model
import matplotlib as mpl
import matplotlib.pyplot as plt
import scipy.special as sp
import scipy.stats as stats

import warnings
import json
import logging

""""""
""" THE REAL IMPLEMENTATION OF SEABANK """
""""""
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')

BASE_CONFIG_FILE = "./file/fl_config.json"

class NodeCommand:
    def __init__(self, command):
        self.command = command
        self.next = None

    def insertAtBegin(self, data):
        new_node = NodeCommand(data)
        if self.head is None:
            self.head = new_node
            return
        else:
            new_node.next = self.head
            self.head = new_node

    def insertAtIndex(self, data, index):
        new_node = NodeCommand(data)
        current_node = self.head
        position = 0
        if position == index:
            self.insertAtBegin(data)
        else:
            while(current_node != None and position+1 != index):
                position = position+1
                current_node = current_node.next
 
            if current_node != None:
 
                new_node.next = current_node.next
                current_node.next = new_node
            else:
                print("Index not present")

    def inserAtEnd(self, data):
        new_node = NodeCommand(data)
        if self.head is None:
            self.head = new_node
            return
    
        current_node = self.head
        while(current_node.next):
            current_node = current_node.next
    
        current_node.next = new_node

    def updateNode(self, val, index):
        current_node = self.head
        position = 0
        if position == index:
            current_node.data = val
        else:
            while(current_node != None and position != index):
                position = position+1
                current_node = current_node.next
    
            if current_node != None:
                current_node.data = val
            else:
                print("Index not present")

    def remove_first_node(self):
        if(self.head == None):
            return
        
        self.head = self.head.next

    def remove_last_node(self):
        if self.head is None:
            return
    
        current_node = self.head
        while(current_node.next.next):
            current_node = current_node.next
    
        current_node.next = None

""" Handling missing data """
def fill_last_value(data):
    return pd.concat(data, axis=1).sort_values('Date').ffill().fillna(0)

def fill_interpolate(data):
    return pd.concat(data, axis=1).sort_values('Date').interpolate().fillna(0)

def fill_fwd_value(data):
    return pd.concat(data, axis=1).sort_values('Date').bfill().fillna(0)

""" Handling MEV Extended Variables """
def add_growth(data, k):
    pass

def add_delta(data, k):
    pass

def add_variances(data, k):
    pass

""" Handling plots """
def generate_plots():
    fig, ax = plt.subplots(nrows=6)
    mev_combine['CPI'].plot(ax=ax[0], title=mev_combine['CPI'].name, color="green")
    mev_combine['GDP'].plot(ax=ax[1], title=mev_combine['CPI'].name)
    mev_combine['BI7DR'].plot(ax=ax[2], title=mev_combine["BI7DR"].name)
    mev_combine['UNEMP'].plot(ax=ax[3], title=mev_combine["UNEMP"].name)
    mev_combine['USDIDR'].plot(ax=ax[4], title=mev_combine["USDIDR"].name)
    mev_combine['SGRIDR'].plot(ax=ax[5], title=mev_combine["SGRIDR"].name)

    fig.savefig("./file/input/mev/mev_combine.pdf")
    plt.show()

""" Handling ODR transformation"""
def transform_zscore(odrs):
    """ Z-Score for list of ODRS"""
    for odr in odrs:
        odr['zs_odr_balance'] = stats.zscore(odr['odr_balance'])
        odr['zs_odr_loan'] = stats.zscore(odr['odr_loan'])
        odr['zs_odr_client'] = stats.zscore(odr['odr_client'])
    # return [x.apply(stats.zscore) for x in odrs]

def transform_logit(odrs):
    """ Logit for list of ODRS"""
    for odr in odrs:
        odr['Logit_odr_balance'] = odr['odr_balance'].apply(sp.logit)
        odr['Logit_odr_loan'] = odr['odr_loan'].apply(sp.logit)
        odr['Logit_odr_client'] = odr['odr_client'].apply(sp.logit)

def transform_log10(odrs):
    """Transform data to logarithmic base 10"""
    for odr in odrs:
        odr['ln_odr_balance'] = np.log10(odr['odr_balance'])
        odr['ln_odr_loan'] = np.log10(odr['odr_loan'])
        odr['ln_odr_client'] = np.log10(odr['odr_client'])

def transform_log2(odrs):
    """Transform data to logarithmic base 2"""
    for odr in odrs:
        odr['ln_odr_balance'] = np.log2(odr['odr_balance'])
        odr['ln_odr_loan'] = np.log2(odr['odr_loan'])
        odr['ln_odr_client'] = np.log2(odr['odr_client'])

def transform_ln(odrs):
    """Transform data to logarithmic natural"""
    for odr in odrs:
        odr['ln_odr_balance'] = np.log(odr['odr_balance'])
        odr['ln_odr_loan'] = np.log(odr['odr_loan'])
        odr['ln_odr_client'] = np.log(odr['odr_client'])

def transform_sma(odrs, n=12):
    """ Simple Moving Average for list of ODRS"""
    for odr in odrs:
        odr[f'SMA{n}_odr_balance'] = odr['odr_balance'].rolling(n).mean().fillna(0)
        odr[f'SMA{n}_odr_loan'] = odr['odr_loan'].rolling(n).mean().fillna(0)
        odr[f'SMA{n}_odr_client'] = odr['odr_client'].rolling(n).mean().fillna(0)

def transform_cma(odrs, n=12):
    """ Cumulative Moving Average for list of ODRS"""
    for odr in odrs:
        odr[f'CMA{n}_odr_balance'] = odr['odr_balance'].expanding(n).mean().fillna(0)
        odr[f'CMA{n}_odr_loan'] = odr['odr_loan'].expanding(n).mean().fillna(0)
        odr[f'CMA{n}_odr_client'] = odr['odr_client'].expanding(n).mean().fillna(0)

def transform_ema(odrs, n=12):
    """ Exponential Moving Average for list of ODRS"""
    for odr in odrs:
        odr[f'EMA{n}_odr_balance'] = odr['odr_balance'].ewm(span=n).mean().fillna(0)
        odr[f'EMA{n}_odr_loan'] = odr['odr_loan'].ewm(span=n).mean().fillna(0)
        odr[f'EMA{n}_odr_client'] = odr['odr_client'].ewm(span=n).mean().fillna(0)

def transform_se(odrs):
    """Simple Exponential for ODR"""
    for odr in odrs:
        odr['SE_odr_balance'] = odr['odr_balance'].apply(np.exp).fillna(0)
        odr['SE_odr_loan'] = odr['odr_loan'].apply(np.exp).fillna(0)
        odr['SE_odr_client'] = odr['odr_client'].apply(np.exp).fillna(0)

# odrs = [odr.set_index(['pt_date', 'pd_segment', 'tenor']) for odr in odrs]
from scipy.stats import kendalltau, pearsonr, spearmanr

""" Handling correlation test """
def kendall_pval(x, y):
    return kendalltau(x,y)[1]

def pearsonr_pval(x, y):
    return pearsonr(x, y)[1]

def spearmanr_pval(x, y):
    return spearmanr(x,y)[1]

""" Handling assumption tests for combined variables 
    1. Linearity
    2. Constant Error Variance
    3. Independent Error Terms
    4. Normal Errors
    5. No multi-collinearity between predictors
    6. Exogeneity
"""



""" Handling OLS assumptions :
    1. The Error Term has Conditional Mean of Zero
    2. Independently and Identically Distributed Data
    3. Large Outliers are Unlikely
"""
def mean_absolute_percentage_error(y_true, y_pred): 
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / y_true))
    mape_rate = mape * 100

    logging.debug(f"MAPE of this model : {mape_rate}")
    return mape_rate

""" Handling Time Series Forecast.
Forecast Techniques:
    1. Autoregression (AR)
    2. Moving Average (MA)
    3. Autoregressive Moving Average (ARMA)
    4. Autoregressive Integrated Moving Average (ARIMA)
    5. Seasonal Autoregressive Integrated Moving-Average (SARIMA)
    6. Seasonal Autoregressive Integrated Moving-Average with Exogenous Regressors (SARIMAX)
    7. Vector Autoregression (VAR)
    8. Vector Autoregression Moving-Average (VARMA)
    9. Vector Autoregression Moving-Average with Exogenous Regressors (VARMAX)
    10. Simple Exponential Smoothing (SES)
    11. Holt Winter\'s Exponential Smoothing (HWES)
"""
@dataclass
class Order:
    p: int = field(default=10)
    q: int = field(default=10)
    d: int = field(default=2)
    m: int = field(default=0)

@dataclass
class TypeOrder:
    types: str = field(default="ARIMA")
    order: object = field(init=False,default=Order())


def set_orders(types="ARIMA", p=10, d=2, q=10, m=0):
    p = range(p)
    q = range(q)
    d = range(d)
    m = range(m)

    orders = {"AR": itertools.product(p), "ARMA": itertools.product(p, q), "ARIMA": itertools.product(p,d,q), "SARIMA": itertools.product(p,d,q,m)}
    logging.debug(f"Total length of orders : {len(list(orders.get(types)))}")
    
    return list(orders.get(types))

def split_data(data: list, nrows=0) -> tuple:
    # Split dataset to train set and test set
    msk = (data.index < len(data)-nrows)
    df_train = data[msk].copy()
    df_test = data[~msk].copy()

    logging.debug(f"Total length of training data model : {len(df_train)}")
    logging.debug(f"Total length of test data model : {len(df_test)}")

    return (df_train, df_test)

def do_acf_testing(data, lags):
    """Autocorrelation Function"""
    acf_org = plot_acf(data, lags=lags)
    plt.show()

    return acf_org

def do_pacf_testing(data, lags):
    """ Partial Autocorrelation Function"""
    pacf_org = plot_pacf(data, lags=lags)
    plt.show()

    return pacf_org

def do_dicky_fuller_test(data):
    """ADF testing or Augmented Dicky-Fuller to obtain the fitness of data given before ARIMA model step"""
    adf_test = st_tools.adfuller(data)
    result = {"ADF Statistics": adf_test[0], "p-value": adf_test[1], "Critical Values": [(k, v) for k,v in adf_test[4].items()]}
    return result

def manual_arima_test(data, p, d, q):
    """Perform MANUAL ARIMA by set p, q, d"""
    model = ARIMA(data, order=(p,d,q))
    model_fit = model.fit()
    model_fit.summary()

def auto_arima_test(data):
    """Perform AUTO ARIMA ordering and tests ARIMA"""
    auto_arima = pm.auto_arima(data, trace=True, step=True, seasonal=True, with_intercept=True)
    return (auto_arima, auto_arima.summary())

def forecast_arima(data, n=None):    
    """Perform forecast"""
    if n is None:
        n = len(data)
    
    forecast_auto = data.predict(n_periods=len(n))
    data['forecast_auto'] = [None] * len(data) + list(forecast_auto)

    return data['forecast_auto']



def AutoRegression():
    from statsmodels.tsa.ar_model import AutoReg
    from random import random
    # contrived dataset
    data = [x + random() for x in range(1, 100)]
    # fit model
    model = AutoReg(data, lags=1)
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.predict(len(data), len(data))
    print(yhat)

def MovingAverage():
    from statsmodels.tsa.arima.model import ARIMA
    from random import random
    # contrived dataset
    data = [x + random() for x in range(1, 100)]
    # fit model
    model = ARIMA(data, order=(0, 0, 1))
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.predict(len(data), len(data))
    print(yhat)

def ARMA():
    # ARMA example
    from statsmodels.tsa.arima.model import ARIMA
    from random import random
    # contrived dataset
    data = [random() for x in range(1, 100)]
    # fit model
    model = ARIMA(data, order=(2, 0, 1))
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.predict(len(data), len(data))
    print(yhat)

def ARIMA():
    # ARIMA example
    from statsmodels.tsa.arima.model import ARIMA
    from random import random
    # contrived dataset
    data = [x + random() for x in range(1, 100)]
    # fit model
    model = ARIMA(data, order=(1, 1, 1))
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.predict(len(data), len(data), typ='levels')
    print(yhat)

def SARIMA():
    # SARIMA example
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from random import random
    # contrived dataset
    data = [x + random() for x in range(1, 100)]
    # fit model
    model = SARIMAX(data, order=(1, 1, 1), seasonal_order=(0, 0, 0, 0))
    model_fit = model.fit(disp=False)
    # make prediction
    yhat = model_fit.predict(len(data), len(data))
    print(yhat)


def SARIMAX():
    # SARIMAX example
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from random import random
    # contrived dataset
    data1 = [x + random() for x in range(1, 100)]
    data2 = [x + random() for x in range(101, 200)]
    # fit model
    model = SARIMAX(data1, exog=data2, order=(1, 1, 1), seasonal_order=(0, 0, 0, 0))
    model_fit = model.fit(disp=False)
    # make prediction
    exog2 = [200 + random()]
    yhat = model_fit.predict(len(data1), len(data1), exog=[exog2])
    print(yhat)

def VAR():
    # VAR example
    from statsmodels.tsa.vector_ar.var_model import VAR
    from random import random
    # contrived dataset with dependency
    data = list()
    for i in range(100):
        v1 = i + random()
        v2 = v1 + random()
        row = [v1, v2]
        data.append(row)
    # fit model
    model = VAR(data)
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.forecast(model_fit.y, steps=1)
    print(yhat)

def VARMA():
    # VARMA example
    from statsmodels.tsa.statespace.varmax import VARMAX
    from random import random
    # contrived dataset with dependency
    data = list()
    for i in range(100):
        v1 = random()
        v2 = v1 + random()
        row = [v1, v2]
        data.append(row)
    # fit model
    model = VARMAX(data, order=(1, 1))
    model_fit = model.fit(disp=False)
    # make prediction
    yhat = model_fit.forecast()
    print(yhat)

def VARMAX():
    # VARMAX example
    from statsmodels.tsa.statespace.varmax import VARMAX
    from random import random
    # contrived dataset with dependency
    data = list()
    for i in range(100):
        v1 = random()
        v2 = v1 + random()
        row = [v1, v2]
        data.append(row)
    data_exog = [x + random() for x in range(100)]
    # fit model
    model = VARMAX(data, exog=data_exog, order=(1, 1))
    model_fit = model.fit(disp=False)
    # make prediction
    data_exog2 = [[100]]
    yhat = model_fit.forecast(exog=data_exog2)
    print(yhat)

def SES():
    # SES example
    from statsmodels.tsa.holtwinters import SimpleExpSmoothing
    from random import random
    # contrived dataset
    data = [x + random() for x in range(1, 100)]
    # fit model
    model = SimpleExpSmoothing(data)
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.predict(len(data), len(data))
    print(yhat)

def HWES():
    # Holt Winter's Exponential Smoothing (HWES) example
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    from random import random
    # contrived dataset
    data = [x + random() for x in range(1, 100)]
    # fit model
    model = ExponentialSmoothing(data)
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.predict(len(data), len(data))
    print(yhat)


# print(odrs)
# odrs[2][['odr_balance','zs_odr_balance']].plot(label="ODR", figsize=(10,8))
# plt.show()


""" Handling output operations """
def export_odr(odrs):
    """ Handling export to file """
    for i in range(len(odrs)):
        odrs[i].to_csv(f"./file/odr_python/py_odr_{odrs[i].index[0][2]}_{odrs[i].index[0][3]}.csv", mode="w")

from openpyxl import load_workbook
def export_corr_and_variables():

    for item in range(len(label)):
        path = f"./{dirs['output_dir']}/py_{label[item][0]}_{label[item][1]}.xlsx"

        with pd.ExcelWriter(path) as writer:
            odrs[item].to_excel(writer, sheet_name="variables")
            corr_odrs[item].to_excel(writer, sheet_name="correlation")
            pval_odrs[item].to_excel(writer, sheet_name="corr_pvalue")

# export_odr(variables[0][1])
# final_odrs = [odr.reset_index() for odr in odrs]
# variables = [pd.concat([x.add_prefix("Y_"), mev_combine.add_prefix("X_")]) for x in odrs]
# print(type(odrs), type(mev_combine), variables[0].index)
# final_odrs[0]['qoq_date'].rename("date")
# mev_combine['CPI_Lag3Q'] = mev_combine['CPI'].shift(3)

# prep_odr = odrs[0]['zs_odr_balance'].reset_index()[['qoq_date', 'zs_odr_balance']].set_index(['qoq_date']).squeeze()
# adf = st_tools.adfuller(prep_odr)
# adf_test = pm.arima.stationarity.ADFTest()
# p_val, should_diff = adf_test.should_diff()



# acf = st_tools.acf(prep_odr)
# pacf = st_tools.pacf(prep_odr)
# kpss = st_tools.kpss(prep_odr)
# model = tsa_model.ARIMA(prep_odr,order=(0,0,1))

# model_fit = model.fit()
# yhat = model_fit.predict(len(prep_odr), len(prep_odr), typ='levels')
# print(adf)
# mev_combine.to_csv(f"./file/input/mev_test.csv", mode="w")


""" Implement the JSON load configuration """
""" Implement the handling input of MEV variables and combine it. """
""" Parsing the input files/dirs and output files/dirs location """
with open(BASE_CONFIG_FILE) as f:
    config_file = json.load(f)

    try:
        _parse = [item for item in config_file['file'].items()]
        files = {f"{item[0]}_files": item[1] for item in config_file['file'].items() if isinstance(item[1], list)}
        dirs = {f"{item[0]}_dir": item[1] for item in config_file['file'].items() if isinstance(item[1], str)}

        for item in _parse:
            if isinstance(item[1], list): 
                logging.info("Found file configurations of {}. Setting up {} files".format(item[0], len(item[1])))
                logging.info("Working files : {}".format(", ".join(item[1])))
            elif isinstance(item[1], str):
                logging.info("Found directory configurations of {}. Setting up {} directories".format(item[0], item[1]))
                logging.info("Working directories : {}".format(item[1]))
            else:
                raise TypeError("No files or directories found")

        # logging.info(f"Found working files : {files.keys()} ({len(files)} files). " )
        # logging.info(f"Found working directories : {dirs.keys()} ({len(dirs)} directories).")
    except KeyError as e:
        logging.warning(f"Key configuration for {e} not found")

    except FileNotFoundError as e:
        logging.error(f"Location in {e} not found. Check again the files or directories in configuration file")

    except BaseException:
        logging.error("Sorry, cannot setup the configuration files. Please check the input and output section again.")


""" Parsing the stepwise and parameters configuration """
with open(BASE_CONFIG_FILE) as f:
    config_file = json.load(f)

    try:
        # _base_parse = [(item[0],item[1]['step']) for item in config_file['configuration'].items()]
        # base_step = [{f"{item[0]}_config": item[1]} for item in config_file['configuration'].items()]
        config = [x for x in config_file['configuration'].items()]
        # print(config)

    except: 

        logging.error("Coba cek lagi section untuk konfigurasi stepwise.")


proxy_odr = pd.read_excel("./file/test/ODR Tracking - OJK Buku 3.xlsx", sheet_name="OJK Historical ODR")
odr = pd.read_csv(files['odr_files'][0], index_col=["qoq_date", "pt_date","pd_segment", "tenor"],parse_dates=['qoq_date', 'pt_date'])

mev_combine = [pd.read_csv(file, low_memory=True, parse_dates=['Date']) for file in files['mev_files']]
mev_combine = [data.set_index("Date") for data in mev_combine]
mev_combine = fill_last_value(mev_combine)
mev_combine = mev_combine.loc[mev_combine.index == mev_combine.index.to_period('M').to_timestamp('M')]

"""Execution Proxy ODR"""
fill_odr = proxy_odr.iloc[:].ffill()

"""Execution ODR """
group = odr.groupby(level=["pd_segment", "tenor"])
odrs = [group.get_group(x) for x in group.groups]

"""Execution combination variables Between ODR and MEV"""
transform_zscore(odrs)
odrs = [odr.reset_index().set_index('qoq_date') for odr in odrs]
odrs = [pd.concat([odr, mev_combine], axis=1).ffill().fillna(0) for odr in odrs]
label = [(odr['pd_segment'].iloc[-1], odr['tenor'].iloc[-1]) for odr in odrs]
odrs = [x.drop(['pt_date', 'pd_segment', 'tenor'], axis=1) for x in odrs]
corr_odrs = [x.corr() for x in odrs]
pval_odrs = [x.corr(method=pearsonr_pval) for x in odrs]
export_corr_and_variables()