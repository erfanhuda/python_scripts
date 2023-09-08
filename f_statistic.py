import pandas as pd
import numpy as np
import pmdarima as pm
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt

def split_data(data: list, nrows=0) -> tuple:
    # Split dataset to train set and test set
    msk = (data.index < len(data)-nrows)
    df_train = data[msk].copy()
    df_test = data[~msk].copy()

    return (df_train, df_test)

def do_data_interpolation(data):
    """Interpolate data in between"""
    df = data.interpolate()
    return df

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
    adf_test = adfuller(data)
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

""""""
""" THE REAL IMPLEMENTATION OF SEABANK """
""""""

import matplotlib as mpl
import matplotlib.pyplot as plt
import scipy.special as sp
import scipy.stats as stats
import warnings

warnings.filterwarnings("ignore")


""" Implement the handling input of MEV variables and combine it. """
proxy_odr = pd.read_excel("./file/test/ODR Tracking - OJK Buku 3.xlsx", sheet_name="OJK Historical ODR")
odr = pd.read_csv("./file/input/mev/ODR.csv", index_col=["qoq_date", "pt_date","pd_segment", "tenor"])
var1 = pd.read_csv("./file/input/mev/CPI_202306.csv", index_col="Date", parse_dates=True)
var2 = pd.read_csv("./file/input/mev/GDP_202306.csv", index_col="Date", parse_dates=True)
var3 = pd.read_csv("./file/input/mev/BI7D_202306.csv", index_col="Date", parse_dates=True)
var4 = pd.read_csv("./file/input/mev/UNEMPLOYMENT_202306.csv", index_col="Date", parse_dates=True)
var5 = pd.read_csv("./file/input/mev/USDIDR_202306.csv", index_col="Date", parse_dates=True)
var6 = pd.read_csv("./file/input/mev/SGDIDR_202306.csv", index_col="Date", parse_dates=True)

mev_combine = [var1, var2, var3, var4, var5, var6]

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


""" Handling ARIMA process"""


# print(odrs)
# odrs[2][['odr_balance','zs_odr_balance']].plot(label="ODR", figsize=(10,8))
# plt.show()

def export_odr(odrs):
    """ Handling export to file """
    for i in range(len(odrs)):
        odrs[i].to_csv(f"./file/odr_python/py_odr_{odrs[i].index[0][2]}_{odrs[i].index[0][3]}.csv", mode="w")

final_odrs = [odr.reset_index() for odr in odrs]
# variables = [pd.concat([x.add_prefix("Y_"), mev_combine.add_prefix("X_")]) for x in odrs]
# print(type(odrs), type(mev_combine), variables[0].index)
print(final_odrs[0]['qoq_date'].rename("date"))
# export_odr(variables[0][1])