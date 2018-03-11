import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import bisect

import datetime
import math

from rqdatac import * 


def get_stocks_in_my_index(index_code, date):
    dic = {
        'BANK.DAO':['601988.XSHG', '601009.XSHG', '601398.XSHG', '601939.XSHG', '600036.XSHG', '000001.XSHE', '600016.XSHG', '601998.XSHG', '601288.XSHG', '002142.XSHE', '601818.XSHG', '600015.XSHG', '601169.XSHG', '600000.XSHG', '601328.XSHG', '601166.XSHG']
    }
    
    d = date.strftime('%Y-%m-%d')
    current_list = dic[index_code]
    
    return [i.order_book_id for i in instruments(current_list) if i.listed_date <= d]

def get_symbol(code):
    if code.endswith('.DAO'):
        return code

    return instruments(code).symbol

# formula:
# PE(index) = n / SUM(1/PE(stock))
# n is the number of the stocks contains of index
def get_index_pe_by_date(index_code, date):
    if index_code.endswith('.DAO'):
        stocks = get_stocks_in_my_index(index_code, date)
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
    
# get the historical PE of index
def get_index_pe(index_code, start_date = '2005-01-01'):
    end_date = pd.datetime.today()
    dates = []
    pes = []
    for d in pd.date_range(start_date, end_date, freq='M'):
        dates.append(d)
        pes.append(get_index_pe_by_date(index_code, d))
    return pd.Series(pes, index = dates)

def output_pe(date, index_list, csv_file = 'pe.csv'):
    df_pe = pd.read_csv(csv_file, index_col='date')
    df_pe = df_pe[df_pe.index <= date.strftime('%Y-%m-%d')]
    results = []
    for s in index_list:
        pe = get_index_pe_by_date(s, date)
        if math.isnan(pe):
            results.append([get_symbol(s)] + [float('NaN') for i in range(1, 15)])
            continue
        q_pes = [df_pe.quantile(i/10.0)[s] for i in range(11)]
        idx = bisect.bisect(q_pes, pe)
        quantile = 10 if idx == len(q_pes) else (idx - (q_pes[idx] - pe)/(q_pes[idx] - q_pes[idx-1]))
        results.append([get_symbol(s),'%.2f'% pe,'%.2f'% (quantile*10)] + ['%.2f'% q for q in q_pes] + [df_pe[s].count()])
    columns=[u'名称',u'当前PE',u'分位点%',u'最小PE'] + ['%d%%'% (i*10) for i in range(1,10)] + [u'最大PE' , u"样本数"]
    return pd.DataFrame(data = results, index = index_list, columns = columns)

def write_index_pe_to_csv(index_list, csv_file, start_date = '2005-01-01'):
    df_pe = pd.DataFrame()
    for code in index_list:
        print('processing [ %s ] ...' % code)
        df_pe[code] = get_index_pe(code, start_date = start_date)

    df_pe.index.name = 'date'
    df_pe.to_csv(csv_file)
    print('done')
    
def plot_pe(index_list, csv_file):
    df_pe_load = pd.read_csv(csv_file, index_col='date')
    df_pe_load.columns = [get_symbol(s) for s in index_list]
    df_pe_load.plot(figsize=(12,10))
