import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import bisect
from scipy.stats import *
from six import BytesIO

import datetime
import math
import os

from rqdatac import *
import tushare as ts


#------------------------------ date ------------------------------
def get_report_quarter(today):
    # deprecated
    return get_report_latest_quarter(today)


def get_report_latest_quarter(today):
    quarter = ''
    year = today.year
    month = today.month

    if month < 5:
        quarter = str(year - 1) + 'q3'
    elif month <= 6:
        quarter = str(year - 1) + 'q4'
    elif month < 9:
        quarter = str(year) + 'q1'
    elif month < 11:
        quarter = str(year) + 'q2'
    elif month <= 12:
        quarter = str(year) + 'q3'

    return quarter


def get_report_latest_year(today):
    year = today.year
    month = today.month

    if month < 5:
        return str(year - 2) + 'q4'

    return str(year - 1) + 'q4'


def get_last_date():
    return datetime.date.today() + datetime.timedelta(days=-1)


def get_earliest_listed_date(stocks):
    listed_date = [ins.listed_date for ins in instruments(stocks)]
    return min(listed_date)


def get_max_listed_days(stocks):
    date = get_earliest_listed_date(stocks)
    return len(get_trading_dates(start_date=date, end_date=datetime.date.today()))


def get_date_by_week(start_date=datetime.date(2005, 1, 1), end_date=datetime.date.today()):
    new_dates = []
    dates = get_trading_dates(start_date=start_date, end_date=end_date)

    last_w = dates[0].strftime("%Y-%W")
    cur_w = ''
    for idx in range(0, len(dates)):
        cur_w = dates[idx].strftime("%Y-%W")
        if cur_w > last_w:
            last_w = cur_w
            new_dates.append(dates[idx - 1])
    if last_w == cur_w:
        new_dates.append(dates[idx - 1])

    return new_dates


def get_ir_3y(end_date):
    df = ts.get_deposit_rate()
    df = df[df['deposit_type'] == '定期存款整存整取(三年)']
    df1 = df[df.date <= end_date]
    return float(df1.iloc[0]['rate'])


#------------------------------ stock & index ------------------------------
def get_all_stocks(listed_date):
    insts = all_instruments(type='CS')
    insts = insts[insts.listed_date < listed_date]
    return insts[insts.status == 'Active']['order_book_id']


def get_instruments(stock_list):
    return instruments(stock_list)


def get_dividend_(order_book_id, end_date, days=-365):
    start_date = end_date + datetime.timedelta(days=days)
    div_df = get_dividend(order_book_id, start_date=start_date)
    if div_df is None:
        return 0

    div = div_df['dividend_cash_before_tax'].sum() / div_df['round_lot'].sum()
    price_df = get_price(
        order_book_id,
        start_date=end_date + datetime.timedelta(days=-10),
        end_date=end_date,
        frequency='1d',
        fields=['close'],
        adjust_type='internal',
        skip_suspended=False,
        country='cn')
    if price_df.size > 0:
        return trunc(div / price_df[-1] * 100, 3)
    return 0


def get_dividend_last_year(order_book_id, end_date, days=-365):
    last_year_end_date = datetime.date(end_date.year, 12, 31)
    last_year_start_date = last_year_end_date + datetime.timedelta(days=days)

    div_df = get_dividend(
        order_book_id,
        start_date=last_year_start_date,
        end_date=last_year_end_date)
    if div_df is None:
        return 0

    div = div_df['dividend_cash_before_tax'].sum() / div_df['round_lot'].sum()
    price_df = get_price(
        order_book_id,
        start_date=end_date + datetime.timedelta(days=-10),
        end_date=end_date,
        frequency='1d',
        fields=['close'],
        adjust_type='internal',
        skip_suspended=False,
        country='cn')
    if price_df.size > 0:
        return trunc(div / price_df[-1] * 100, 3)
    return 0


def get_ref_val(csv_file,
                index_code,
                today,
                n_quantile=None,
                percent=0.67,
                default=30):
    df_pe = get_csv(csv_file, index_col='date')
    df_pe = df_pe[df_pe.index <= today]

    if df_pe.size > 0:
        if n_quantile:
            result = get_quantile(df_pe[index_code], n_quantile)
        else:
            result = list(df_pe[index_code])[-1] * percent

        return trunc(result, 2)
    else:
        return default


def calc_volatility(order_book_id, end_date, days=-365):
    s = get_price(
        order_book_id,
        start_date=end_date + datetime.timedelta(days=days),
        end_date=end_date,
        frequency='1d',
        fields=['close'],
        adjust_type='pre',
        skip_suspended=False,
        country='cn')

    r = np.log(s / s.shift(1))
    e = np.std(r)

    return e * math.sqrt(252)


#------------------------------ calc temperature by pb & pe ------------------------------
def calc_normdist(x):
    # x is a series
    norm_s = []
    for idx in range(1, len(x)):
        split_x = x[:idx + 1]
        mean = split_x.mean()
        sigma = split_x.std(ddof=1)
        norm_s.append(norm.cdf(x[idx], mean, sigma))
    return norm_s


def calc_temperature_1(df, cols):
    length = len(cols)
    norm_s = {}
    for col in cols:
        arr = np.array(df[col].values)
        norm_s[col] = calc_normdist(arr)

    tmp_df = pd.DataFrame(index=df.index[1:])
    for col in cols:
        tmp_df[col] = (df.ix[1:, col]).astype(np.float64)
        if length > 1:
            tmp_df[col + '_tmp'] = np.array(norm_s[col]) * 100
        else:
            tmp_df['tmp'] = np.array(norm_s[col]) * 100

    if length > 1:
        tmp_df['tmp'] = np.sum(tmp_df[col + '_tmp'] for col in cols) / len(cols)

    return tmp_df


def calc_temperature(df, cols):
    col1 = np.array(df[cols[0]].values)
    pb_norm_s = calc_normdist(col1)

    col2 = df[cols[1]].values
    pe_norm_s = calc_normdist(col2)

    tmp_df = pd.DataFrame(index=df.index[1:])
    tmp_df['pb'] = col1[1:]
    tmp_df['pe'] = col2[1:]
    pb_tmp_narr = np.array(pb_norm_s) * 100
    pe_tmp_narr = np.array(pe_norm_s) * 100
    tmp_df['tmp'] = (pb_tmp_narr + pe_tmp_narr) / 2

    return tmp_df


def store_tmp_to_csv(df, stock, append=False, isResearch=False):
    df.index.name = 'date'
    if isResearch:
        file_name = "../temp/%s.csv" % (stock)
        df.to_csv(file_name)
    else:
        file_name = "temp/%s.csv" % (stock)
        print('store %s' % file_name)
        put_file(file_name, df.to_csv(), append=append)

def load_tmp_from_csv(stock, isResearch=False):
    if isResearch:
        file_name = "../temp/%s.csv" % (stock)
        if os.path.exists(file_name):
            return pd.read_csv(file_name, index_col='date')
    else:
        file_name = "temp/%s.csv" % (stock)
        # print(file_name)
        # if os.path.exists(file_name):
        try:
            body = get_file(file_name)
            if body is not None and len(body) > 0:
                # print('reading %s ...' % file_name)
                return pd.read_csv(BytesIO(body), index_col='date')
        except pd.io.common.EmptyDataError:
            pass
        except Exception as e:
            print('exceptions: %s' % e)
    return None


def get_current_temperature(stock_list, now, isResearch=False):
    series_dict = {}
    _now = now.strftime('%Y-%m-%d') if type(now) == datetime.datetime or type(now) == datetime.date else now
    interval = '2500d'  #str(len(get_trading_dates('2005-01-01', now))) + 'd'
    today = datetime.date.today()
    chunkSize = 10
    spliter = len(stock_list) / chunkSize if len(stock_list) % chunkSize == 0 else len(stock_list) // chunkSize + 1
    chunks = np.array_split(stock_list, spliter)
    for chunk in chunks:
        df = get_fundamentals(query(
                fundamentals.eod_derivative_indicator.pb_ratio,
                fundamentals.eod_derivative_indicator.pe_ratio
            ).filter(
                fundamentals.eod_derivative_indicator.stockcode.in_(list(chunk))
            ), entry_date=today, interval=interval)

        _temp_df = None
        tmp_df = None
        for order_book_id in list(df.minor_axis):
            tmp_df = load_tmp_from_csv(order_book_id, isResearch=isResearch)
            if tmp_df is None or tmp_df.empty:
                print('calculating %s...' % order_book_id)
                _temp_df = calc_temperature_1(df.minor_xs(order_book_id).sort_index(ascending=True).dropna(), ['pb_ratio', 'pe_ratio'])
                _temp_df = _temp_df.round({'pb_ratio_tmp': 3, 'pe_ratio_tmp': 3, 'tmp':3})
                store_tmp_to_csv(_temp_df, order_book_id, isResearch=isResearch)
            else:
                _temp_df = tmp_df

            _temp_df = _temp_df[_temp_df.index >= _now]
            if _temp_df.size > 0:
                series_dict[order_book_id] = _temp_df.ix[0, :]
            else:
                print('something wrong: %s is empty' % (order_book_id))

    new_df = pd.DataFrame(series_dict).T
    # new_df.sort_values(by='tmp', inplace=True)
    # new_df[new_df.tmp < 35]

    return new_df


#------------------------------ others ------------------------------
def get_quantile(data_serie, n_quantile):
    data_list = [data_serie.quantile(i / 10.0) for i in range(11)]
    return data_list[n_quantile - 1]


def trunc(num, digits):
    if math.isnan(num):
        return num

    return math.floor(num * math.pow(10, digits)) / math.pow(10, digits)


# http://stackoverflow.com/questions/61517/python-dictionary-from-an-objects-fields
# http://stackoverflow.com/questions/1305532/convert-Python-dict-to-object
def obj_dic(d):
    top = type('new', (object, ), d)
    seqs = tuple, list, set, frozenset
    for i, j in d.items():
        if isinstance(j, dict):
            setattr(top, i, obj_dic(j))
        elif isinstance(j, seqs):
            setattr(top, i, type(j)(obj_dic(sj) if isinstance(sj, dict) else sj for sj in j))
        else:
            setattr(top, i, j)
    return top
