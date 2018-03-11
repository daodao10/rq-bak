import pandas as pd
import numpy as np

from rqdatac import * 

import dao_util as dao_u


def _get_roa_roe_quarter_report(stocks, today, n_report_quarters, is_roa):
    quarter = dao_u.get_report_quarter(today)
    
    fin_df = get_financials(
        (query(financials.financial_indicator.annual_return_on_asset_net_profit) if is_roa else query(financials.financial_indicator.annual_return_on_equity))
        .filter(
            financials.stockcode.in_(stocks)
        ), quarter = quarter, interval = str(n_report_quarters) + 'q'
    )
    
    roe_list = []
    if len(stocks) == 1:
        roe_list.append(fin_df.mean())
    else:
        for s in stocks:
            roe_list.append(fin_df[s].mean())

    return pd.Series(roe_list, index = stocks)

def get_roe_quarter_report(stocks, today, n_report_quarters = 20):
    return _get_roa_roe_quarter_report(stocks, today, n_report_quarters, False)

def get_roa_quarter_report(stocks, today, n_report_quarters = 20):
    return _get_roa_roe_quarter_report(stocks, today, n_report_quarters, True)

def calc_roe_pb(pb, roe):
    return np.log(pb)/np.log1p(roe)

def calc_roe_pb_lip(pb, roe):
    allowed_pb = 0
    if roe <= 10:
        allowed_pb = 1
    
    allowed_pb = 2 ** roe

    return pb / allowed_pb

# PB & PE
def get_stocks_by_pe(stock_list, today, pe_max):
    q = query().filter(
        fundamentals.eod_derivative_indicator.pe_ratio > 0,
        fundamentals.eod_derivative_indicator.pe_ratio < pe_max)
    if stock_list and len(stock_list) > 0:
        q = q.filter(fundamentals.eod_derivative_indicator.stockcode.in_(stock_list))
    
    df = get_fundamentals(q.order_by(fundamentals.eod_derivative_indicator.pe_ratio.asc()), entry_date = today, interval = '1d')
    
    if df is None:
        return []
    return list(df.minor_axis.values)

def get_stocks_by_pb(stock_list, today, pb_max = 1.5):
    q = query().filter(fundamentals.eod_derivative_indicator.pb_ratio < pb_max)
    if stock_list and len(stock_list) > 0:
        q = q.filter(fundamentals.eod_derivative_indicator.stockcode.in_(stock_list))
    
    df = get_fundamentals(q.order_by(fundamentals.eod_derivative_indicator.pb_ratio.asc()), entry_date = today, interval = '1d')
    
    if df is None:
        return []
    return list(df.minor_axis.values)
    
def get_stocks_by_pcf(stock_list, today):
    q = query().filter(fundamentals.eod_derivative_indicator.pcf_ratio_1 > 0)
    if stock_list and len(stock_list) > 0:
        q = q.filter(fundamentals.eod_derivative_indicator.stockcode.in_(stock_list))

    df = get_fundamentals(
        q.order_by(fundamentals.eod_derivative_indicator.pcf_ratio_1.asc()), entry_date = today, interval = '1d'
    )

    if df is None:
        return []
    return list(df.minor_axis.values)

# np.log(PB)/np.log1p(ROE)
def get_stocks1_by_eod(stock_list, today, use_roa = False):    
    if use_roa:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_asset_net_profit
        )
    else:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_equity
        )

    tmp_df = get_fundamentals(q.filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)    
        ), entry_date = today, interval = '1d'
    )

    if tmp_df is None or tmp_df.empty:
        return []
    
    tmp_df = tmp_df.iloc[:,0]

    tmp_df['calc_effeciency'] = list(map(calc_roe_pb, np.array(2* tmp_df['pb_ratio']), np.array(tmp_df['annual_return_on_asset_net_profit']) if use_roa else np.array(tmp_df['annual_return_on_equity'])))
    tmp_df.sort_values(by = 'calc_effeciency',ascending=True,inplace=True)
    
    return list(tmp_df.index)

def get_stocks1_by_quarter_report(stock_list, today, n_report_quarters = 20, use_roa = False):
    stock_list = get_fundamentals(
        query()
        .filter(fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)), entry_date = today, interval = '1d'
    ).minor_axis.values
    
    if use_roa:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_asset_net_profit
        )
    else:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_equity
        )

    tmp_df = get_fundamentals(q.filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)
        ), entry_date = today - datetime.timedelta(days = 1), interval = str(n_report_quarters) + 'q'
    )

    if tmp_df is None or tmp_df.empty:
        return []
    
    stock_list = list(tmp_df.iloc[:,0].index)
    
    # get roe & pb
    roe_list = []
    pb_list = []
    for s in stock_list:
        tmp_xs = tmp_df.minor_xs(s)
        roe_list.append(tmp_xs['annual_return_on_asset_net_profit'].mean() if use_roa else tmp_xs['annual_return_on_equity'].mean())
        pb_list.append(tmp_xs.ix[0, 'pb_ratio'])
    tmp_df.dropna(inplace=True,how = 'any')

    tmp_df = pd.DataFrame({'roe_avg': roe_list, 'pb': pb_list}, index = stock_list)
    tmp_df['calc_effeciency'] = list(map(calc_roe_pb, np.array(2* tmp_df['pb']), np.array(tmp_df['roe_avg'])))

    tmp_df.sort_values(by = 'calc_effeciency', ascending = True, inplace = True)
    return list(tmp_df.index)

# PB / (2 ** (10 * ROE)) <= 1
def get_stocks1_1_by_eod(stock_list, today, use_roa = False):    
    if use_roa:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_asset_net_profit
        )
    else:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_equity
        )

    tmp_df = get_fundamentals(q.filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)    
        ), entry_date = today, interval = '1d'
    )

    if tmp_df is None or tmp_df.empty:
        return []
    
    tmp_df = tmp_df.iloc[:,0]

    tmp_df['calc_effeciency'] = list(map(calc_roe_pb_lip, np.array(tmp_df['pb_ratio']), np.array(tmp_df['annual_return_on_asset_net_profit']) if use_roa else np.array(tmp_df['annual_return_on_equity'])))
    tmp_df.sort_values(by = 'calc_effeciency',ascending=True,inplace=True)
    
    tmp_df = tmp_df[tmp_df['calc_effeciency'] <= 1]
    return list(tmp_df.index)

def get_stocks1_1_by_quarter_report(stock_list, today, n_report_quarters = 20, use_roa = False):
    stock_list = get_fundamentals(
        query()
        .filter(fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)), entry_date = today, interval = '1d'
    ).minor_axis.values
    
    if use_roa:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_asset_net_profit
        )
    else:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_equity
        )

    tmp_df = get_fundamentals(q.filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)
        ), entry_date = today - datetime.timedelta(days = 1), interval = str(n_report_quarters) + 'q'
    )

    if tmp_df is None or tmp_df.empty:
        return []
    
    stock_list = list(tmp_df.iloc[:,0].index)
    
    # get roe & pb
    roe_list = []
    pb_list = []
    for s in stock_list:
        tmp_xs = tmp_df.minor_xs(s)
        roe_list.append(tmp_xs['annual_return_on_asset_net_profit'].mean() if use_roa else tmp_xs['annual_return_on_equity'].mean())
        pb_list.append(tmp_xs.ix[0, 'pb_ratio'])
    tmp_df.dropna(inplace=True,how = 'any')

    tmp_df = pd.DataFrame({'roe_avg': roe_list, 'pb': pb_list}, index = stock_list)
    tmp_df['calc_effeciency'] = list(map(calc_roe_pb_lip, np.array(tmp_df['pb']), np.array(tmp_df['roe_avg'])))

    tmp_df.sort_values(by = 'calc_effeciency', ascending = True, inplace = True)
    tmp_df = tmp_df[tmp_df['calc_effeciency'] <= 1]
    return list(tmp_df.index)

# (1 + ROA) ** N > PB
def get_stocks2_by_eod(stock_list, today, n_return_years, use_roa = True):
    if use_roa:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_asset_net_profit
        )
    else:
        q = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.financial_indicator.annual_return_on_equity
        )

    tmp_df = get_fundamentals(q.filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)    
        ), entry_date = today, interval = '1d'
    )
    
    if tmp_df is None or tmp_df.empty:
        return []
    
    tmp_df = tmp_df.iloc[:,0]

    tmp_df['calc_secure_pb'] = (1 + (tmp_df['annual_return_on_asset_net_profit'] if use_roa else tmp_df['annual_return_on_equity']) / 100) ** n_return_years
    tmp_df['calc_secure'] = tmp_df['calc_secure_pb'] / tmp_df['pb_ratio']

    tmp_df.sort_values(by = 'calc_secure', ascending = False, inplace = True)
    
    return list(tmp_df[tmp_df['calc_secure'] > 1].index)
    
def get_stocks2_by_quarter_report(stock_list, today, n_return_years, n_report_quarters = 20, use_roa = True):
    tmp_df = get_fundamentals(
        query(
            fundamentals.eod_derivative_indicator.pb_ratio
        )
        .filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)    
        ), entry_date = today, interval = '1d'
    )
    
    if tmp_df is None or tmp_df.empty:
        return []
    
    tmp_df = tmp_df.iloc[:,0]

    tmp_df['roa_avg'] = get_roa_quarter_report(list(tmp_df.index), today, n_report_quarters = n_report_quarters) if use_roa else get_roe_quarter_report(list(tmp_df.index), today, n_report_quarters = n_report_quarters)
    tmp_df.dropna(inplace=True, how = 'any')
    
    tmp_df['calc_secure_pb'] = (1 + tmp_df['roa_avg'] / 100) ** n_return_years
    tmp_df['calc_secure'] = tmp_df['calc_secure_pb'] / tmp_df['pb_ratio']

    tmp_df.sort_values(by = 'calc_secure', ascending = False, inplace = True)
    
    return list(tmp_df[tmp_df['calc_secure'] > 1].index)

# stock2 & stocks1
def get_stocks3_by_eod(stock_list, today, n_return_years):    
    stocks2 = get_stocks2_by_eod(stock_list, today, n_return_years, use_roa = True)
    s2 = pd.Series(data = range(1, len(stocks2) + 1), index = stocks2)
    
    stocks1 = get_stocks1_by_eod(stock_list, today, use_roa = False)
    s1 = pd.Series(data = range(1, len(stocks1) + 1), index = stocks1)
    
    s3 = s1 + s2
    s3.sort_values(ascending = True, inplace = True)

    return list(s3.index)

def get_stocks3_by_quarter_report(stock_list, today, n_return_years, n_report_quarters = 20):    
    stocks2 = get_stocks2_by_quarter_report(stock_list, today, n_return_years, n_report_quarters = n_report_quarters, use_roa = True)
    s2 = pd.Series(data = range(1, len(stocks2) + 1), index = stocks2)
    
    stocks1 = get_stocks1_by_quarter_report(stock_list, today, n_report_quarters = n_report_quarters, use_roa = False)
    s1 = pd.Series(data = range(1, len(stocks1) + 1), index = stocks1)
    
    s3 = s1 + s2
    s3.sort_values(ascending = True, inplace = True)

    return list(s3.index)
