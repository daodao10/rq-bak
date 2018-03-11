import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import bisect

import datetime
import math

from rqdatac import * 

import dao_my_index_util as dao_miu


# formula:
# PB(index) = n / SUM(1/PB(stock))
# n is the number of the stocks contains of index
def get_index_pb_by_date(index_code, date):
    if index_code.endswith('.DAO'):
        stocks = dao_miu.get_stocks_in_my_index(index_code, date)
    else:
        stocks = index_components(index_code, date)

    if stocks:
        q = query(fundamentals.eod_derivative_indicator.pb_ratio).filter(fundamentals.financial_indicator.stockcode.in_(stocks))
        df = get_fundamentals(q, entry_date = date)
        
        pbs = df.iloc[0,0]
        if len(pbs) > 0:
            pb = len(pbs)/sum([0 if math.isnan(_pb) else 1/_pb for _pb in pbs.values])
            return pb

    return float('NaN')

# formula:
# PE(index) = n / SUM(1/PE(stock))
# n is the number of the stocks contains of index
def get_index_pe_by_date(index_code, date):
    if index_code.endswith('.DAO'):
        stocks = dao_miu.get_stocks_in_my_index(index_code, date)
    else:
        stocks = index_components(index_code, date)

    if stocks:
        q = query(fundamentals.eod_derivative_indicator.pe_ratio).filter(fundamentals.financial_indicator.stockcode.in_(stocks))
        df = get_fundamentals(q, entry_date = date)
        pes = df.iloc[0,0]
        if len(pes) > 0:
            pe = len(pes)/sum([1/_pe if _pe > 0 else 0 for _pe in pes.values]) # exclude pe is negative
            return pe

    return float('NaN')

# get the historical PB of index
def get_index_pb(index_code, start_date = '2005-01-01'):
    end_date = pd.datetime.today()
    dates = []
    pbs = []
    for d in pd.date_range(start_date, end_date, freq='M'):
        dates.append(d)
        pbs.append(get_index_pb_by_date(index_code, d))
    return pd.Series(pbs, index = dates)

# get the historical PE of index
def get_index_pe(index_code, start_date = '2005-01-01'):
    end_date = pd.datetime.today()
    dates = []
    pes = []
    for d in pd.date_range(start_date, end_date, freq='M'):
        dates.append(d)
        pes.append(get_index_pe_by_date(index_code, d))
    return pd.Series(pes, index = dates)

def output_pb(date, index_list, csv_file = 'pb.csv'):
    df_pb = pd.read_csv(csv_file, index_col='date')
    df_pb = df_pb[df_pb.index <= date.strftime('%Y-%m-%d')]
    results = []
    for s in index_list:
        pb = get_index_pb_by_date(s, date)
        if math.isnan(pb):
            results.append([dao_miu.get_symbol(s)] + [float('NaN') for i in range(1, 15)])
            continue
        q_pbs = [df_pb.quantile(i/10.0)[s] for i in range(11)]
        idx = bisect.bisect(q_pbs, pb)
        quantile = 10 if idx == len(q_pbs) else (idx - (q_pbs[idx] - pb)/(q_pbs[idx] - q_pbs[idx-1]))
        results.append([dao_miu.get_symbol(s),'%.2f'% pb,'%.2f'% (quantile*10)] + ['%.2f'% q for q in q_pbs] + [df_pb[s].count()])
    columns=[u'名称',u'当前PB',u'分位点%',u'最小PB'] + ['%d%%'% (i*10) for i in range(1,10)] + [u'最大PB' , u"样本数"]
    return pd.DataFrame(data = results, index = index_list, columns = columns)

def write_to_csv(index_list, csv_file, func, start_date = '2005-01-01'):
    # func:
    # get_index_pb or get_index_pe
    df_pb= pd.DataFrame()
    for s in index_list:
        print('processing [ %s ] ...' % s)
        df_pb[s] = func(s, start_date = start_date)

    df_pb.index.name = 'date'
    df_pb.to_csv(csv_file)
    print('done')
    
def plot_pb(index_list, csv_file):
    df_pb_load = pd.read_csv(csv_file, index_col='date')
    df_pb_load.columns = [get_symbol(s) for s in index_list]
    df_pb_load.plot(figsize=(12,10))
