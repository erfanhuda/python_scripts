import pandas as pd
import numpy as np
import pmdarima as pm
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import statsmodels.tsa.stattools as st_tools
import statsmodels.tsa.arima.model as tsa_model
import matplotlib.pyplot as plt

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

input_mev_file = ["./file/input/mev/CPI_202306.csv", "./file/input/mev/GDP_202306.csv", "./file/input/mev/BI7D_202306.csv", "./file/input/mev/UNEMPLOYMENT_202306.csv", "./file/input/mev/USDIDR_202306.csv", "./file/input/mev/SGDIDR_202306.csv"]
input_odr_file = "./file/input/mev/ODR.csv"
proxy_odr_dir = "./file/proxy_odr/"
config_file = "./file/fl_config.json"
var_output_dir = "./file/output/var/"
arima_output_dir = "./file/output/arima/"

""" Implement the JSON load configuration """
class BaseFile(json.JSONDecoder):
    def __init__(self):
        self.config_file = None
        
""" Implement the handling input of MEV variables and combine it. """
proxy_odr = pd.read_excel("./file/test/ODR Tracking - OJK Buku 3.xlsx", sheet_name="OJK Historical ODR")
odr = pd.read_csv("./file/input/mev/ODR.csv", index_col=["qoq_date", "pt_date","pd_segment", "tenor"])
var1 = pd.read_csv("./file/input/mev/CPI_202306.csv", index_col="Date", parse_dates=True)
var2 = pd.read_csv("./file/input/mev/GDP_202306.csv", index_col="Date", parse_dates=True)
var3 = pd.read_csv("./file/input/mev/BI7D_202306.csv", index_col="Date", parse_dates=True)
var4 = pd.read_csv("./file/input/mev/UNEMPLOYMENT_202306.csv", index_col="Date", parse_dates=True)
var5 = pd.read_csv("./file/input/mev/USDIDR_202306.csv", index_col="Date", parse_dates=True)
var6 = pd.read_csv("./file/input/mev/SGDIDR_202306.csv", index_col="Date", parse_dates=True)

mev_combine = [var1,var2,var3,var4,var5]

""" Handling missing data """
def fill_last_value(data):
    return pd.concat(data).sort_values('Date').ffill().fillna(0)

def fill_interpolate(data):
    return pd.concat(data).sort_values('Date').interpolate().fillna(0)

def fill_fwd_value(data):
    return pd.concat(data).sort_values('Date').bfill().fillna(0)

mev_combine = fill_interpolate(mev_combine)

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

"""Handling Proxy ODR"""
fill_odr = proxy_odr.iloc[:].ffill()

"""Handling ODR """
group = odr.groupby(level=["pd_segment", "tenor"])
odrs = [group.get_group(x) for x in group.groups]


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

transform_zscore(odrs)
# odrs = odrs[:].reset_index(inplace=True)
# odrs = [odr.reset_index(inplace=True) for odr in odrs]
# odrs = [odr.rename(columns={"qoq_date":"date"}, inplace=True) for odr in odrs]

""" Handling combination variables Between ODR and MEV"""
# print(odrs)


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
import itertools

p = range(10)
q = range(10)
d = range(2)
orders = itertools.product(p, d, q)

logging.debug(f"Total length of orders : {len(list(orders))}")

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