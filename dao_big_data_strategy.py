import pandas as pd
import numpy as np
import sqlalchemy as sql

import math
import datetime

from rqdatac import * 

import dao_stock_pbpe_util as dao_spbpe
import dao_util as dao_u


Settings = {}

CONSTRAINT_PE = 20.2
CONSTRAINT_PB = 2.0
CONSTRAINT_MULTIPILER_PEPB = 11

BANK_STOCKS = ['600919.XSHG', '601988.XSHG', '601009.XSHG', '601997.XSHG', '601398.XSHG', '600926.XSHG', '601939.XSHG', '600036.XSHG', '002807.XSHE', '000001.XSHE', '600016.XSHG', '601229.XSHG', '601128.XSHG', '601998.XSHG', '601288.XSHG', '002142.XSHE', '601818.XSHG', '600908.XSHG', '603323.XSHG', '600015.XSHG', '601169.XSHG', '600000.XSHG', '601328.XSHG', '002839.XSHE', '601166.XSHG']

HIGH_WAY_STOCKS = ['600106.XSHG', '600350.XSHG', '600012.XSHG', '600020.XSHG', '000828.XSHE', '000900.XSHE', '600269.XSHG', '601107.XSHG', '601518.XSHG', '601188.XSHG', '600368.XSHG', '000886.XSHE', '600033.XSHG', '600548.XSHG', '600035.XSHG', '000429.XSHE', '200429.XSHE', '600377.XSHG', '600003.XSHG', '000916.XSHE']
IRON_STEEL_STOCKS = ['002478.XSHE','002318.XSHE','600782.XSHG','600295.XSHG','600507.XSHG','600019.XSHG','600282.XSHG','002423.XSHE','600117.XSHG','600808.XSHG','000898.XSHE','600022.XSHG','600569.XSHG','600307.XSHG','000655.XSHE','000825.XSHE','600231.XSHG','000761.XSHE','600399.XSHG','600126.XSHG','002593.XSHE','000708.XSHE','601969.XSHG','600010.XSHG','600581.XSHG','000959.XSHE','000932.XSHE','200761.XSHE','000709.XSHE','603878.XSHG','002756.XSHE','002110.XSHE','000717.XSHE','600532.XSHG','601005.XSHG','600102.XSHG','600005.XSHG','002075.XSHE','000629.XSHE','601003.XSHG','000409.XSHE']
COAL_MINING_STOCKS = ['601101.XSHG','601001.XSHG','601225.XSHG','601699.XSHG','600971.XSHG','000983.XSHE','000937.XSHE','600348.XSHG','601666.XSHG','600123.XSHG','601015.XSHG','600740.XSHG','600997.XSHG','600188.XSHG','601898.XSHG','600408.XSHG','600758.XSHG','600546.XSHG','600395.XSHG','000552.XSHE','600508.XSHG','601918.XSHG','600121.XSHG','600792.XSHG','600397.XSHG','000780.XSHE','000933.XSHE','600180.XSHG','601088.XSHG','000723.XSHE','600157.XSHG','601011.XSHG','600403.XSHG','002128.XSHE','000571.XSHE','002753.XSHE','603113.XSHG']

SPECIAL_STOCKS = { s: 1 for s in (HIGH_WAY_STOCKS + IRON_STEEL_STOCKS + COAL_MINING_STOCKS) }

SHENWAN_INDUSTRY = {'801210.INDX':'休闲服务','801130.INDX':'纺织服装','801720.INDX':'建筑装饰','801150.INDX':'医药生物','801050.INDX':'有色金属','801030.INDX':'化工','801200.INDX':'商业贸易','801710.INDX':'建筑材料','801020.INDX':'采掘','801230.INDX':'综合','801170.INDX':'交通运输','801010.INDX':'农林牧渔','801740.INDX':'国防军工','801750.INDX':'计算机','801760.INDX':'传媒','801880.INDX':'汽车','801040.INDX':'钢铁','801790.INDX':'非银金融','801110.INDX':'家用电器','801080.INDX':'电子','801780.INDX':'银行','801140.INDX':'轻工制造','801890.INDX':'机械设备','801180.INDX':'房地产','801160.INDX':'公用事业','801730.INDX':'电气设备','801120.INDX':'食品饮料','801770.INDX':'通信'}

def set_pre_define(style='neutral'):
    Settings["position"], Settings["special_position"], Settings["pb"], Settings["pe"] = zip(*get_pre_define(style))

def get_pre_define(style):
    pe = [i for i in range(25, 4, -1)]
    pb = [i/10 for i in range(25, 4, -1)]
        
    if style == 'neutral':
        position = [0, 0.07, 0.14, 0.21, 0.28, 0.35, 0.42, 0.49, 0.56, 0.63, 0.70, 0.77, 0.84, 0.91, 0.98, 1, 1, 1, 1, 1]
    elif style == 'optimistic':
        position = [0, 0.08, 0.16, 0.24, 0.32, 0.40, 0.48, 0.56, 0.64, 0.72, 0.80, 0.88, 0.96, 1, 1, 1, 1, 1, 1, 1]
    else: #style == 'conservative':
        position = [0, 0.06, 0.12, 0.18, 0.24, 0.30, 0.36, 0.42, 0.48, 0.54, 0.60, 0.66, 0.72, 0.78, 0.84, 0.90, 0.96, 1, 1, 1]
    
    special_position = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.30, 0.42, 0.54, .66, .78, .90, 1]
    return zip(position, special_position, pb, pe)

# POSITION, SPECIAL_POSITION, PB, PE = zip(*get_pre_define('neutral'))
set_pre_define()

def replace_industry(stock_list):
    ind_list = [
        {
            'symbol':inst.symbol,
            'industry': inst.shenwan_industry_code if inst.shenwan_industry_code is not None else None,
            'industry_name': inst.shenwan_industry_name
        } for inst in instruments(stock_list)]
    
    stocks_df = pd.DataFrame.from_records(ind_list, index = stock_list)   
    stocks_df['is_special'] = [stock in SPECIAL_STOCKS for stock in stock_list]
    
    return stocks_df

def get_market_pb(today = None, ignore_bank = False):
    if today == None:
        today = dao_u.get_last_date()
    quarter = dao_u.get_report_quarter(today)

    df_equity = get_financials(query(
        fundamentals.balance_sheet.equity_parent_company
    ).filter(fundamentals.balance_sheet.equity_parent_company > 0), quarter = quarter, interval = '1q')
        
    q = query(
        fundamentals.eod_derivative_indicator.market_cap,
        #fundamentals.eod_derivative_indicator.a_share_market_val,
        fundamentals.balance_sheet.equity_parent_company
    ).filter(fundamentals.balance_sheet.equity_parent_company > 0)
    if ignore_bank:
        q = q.filter(sql.not_(fundamentals.eod_derivative_indicator.stockcode.in_(BANK_STOCKS)))
    
    df_cap = get_fundamentals(q, entry_date = today, interval = '1d')
    df_x = df_cap.iloc[:,0]

    # fillna: how about df_x.combine_first(df_equity)? performance issue
    # df_x.combine_first(df_equity)
    null_equities = list(df_x[df_x['equity_parent_company'].isnull()].index)
    for s in null_equities:
        if hasattr(df_equity, s):
            df_x.ix[s,'equity_parent_company'] = df_equity.ix[0,s]#df_equity.ix[s,'equity_parent_company']

    df_x2 = df_x[df_x['market_cap'].notnull()]
    df_x2 = df_x2[df_x2['equity_parent_company'].notnull()]
    
    total_equity = df_x2['equity_parent_company'].sum()
    market_cap = df_x2['market_cap'].sum()
    return market_cap/total_equity
    #return (total_equity, market_cap, market_cap/total_equity)

def calc_X_pb_pe(pb, pe, is_special):
    p1 = calc_X_by_pb(pb, is_special)
    p2 = calc_X_by_pe(pe, is_special)
    return (p1 + p2) / 2

def calc_X_by_pb(pb, is_special):
    pb = round(pb,1)
    idx = 0
    for it in Settings["pb"]:
        if it <= pb:
            return Settings["special_position"][idx] if is_special else Settings["position"][idx]
        idx += 1
    return 0

def calc_X_by_pe(pe, is_special):
    if np.isnan(pe):
        return 0
    pe = round(pe)
    idx = 0
    for it in Settings["pe"]:
        if it <= pe:
            return Settings["special_position"][idx] if is_special else Settings["position"][idx]
        idx += 1
    return 0

def calc_X(pb, pe, is_special = False):
    if (pb <= 0.9 and pe <= 15) or (pb <= 1.2 and pe <= CONSTRAINT_MULTIPILER_PEPB * pb):
        return (calc_X_by_pb(pb, is_special), 'PB')
    if (pb > 1.5 and pe > 18) or (pb > 1.9 and pe > 15):
        if pb * 10 > pe:
            return (calc_X_by_pb(pb, is_special), 'PB')
        else:
            return (calc_X_by_pe(pe, is_special), 'PE')
    else:
        return (calc_X_pb_pe(pb, pe, is_special), 'PB-PE')
        
def calc_X_df(df):
    for idx in df.index:
        row = df.ix[idx]
        prob = calc_X(row['pb_ratio'], row['pe_ratio'], row['is_special'])
        df.ix[idx, 'X'] = prob[0]
        df.ix[idx, 'strategy'] = prob[1]

def get_last_price(order_book_ids, now, frequency = '1m', adjust_type = 'pre'):
    start_date = datetime.timedelta(days=-1) + now
    df = get_price(order_book_ids, start_date = start_date, end_date = now, frequency=frequency, fields=['close'], adjust_type=adjust_type)

    if df.size > 0:
        df = df[df.index <= now]
        if df.size > 0:
            price_series = df[df.index <= now].ix[-1,:]

            if len(order_book_ids) == 1:
                return pd.Series(data=[price_series], index = order_book_ids)
            else: 
                return price_series
            
    print('non-trading-date')
    return pd.Series(index=order_book_ids)
    
def _get_my_price(order_book_ids, start_date, end_date, frequency):
    last_price_series = get_last_price(order_book_ids, end_date, frequency = frequency)
    
    
    df = pd.DataFrame(columns=['low', 'last'], index = order_book_ids)
    if last_price_series.size > 0:
        for s in order_book_ids:
            price_min = np.min(get_price(s, start_date, end_date, frequency='1d', fields=['low'], adjust_type='pre'))
            price_last = last_price_series[s]

            df.ix[s, 'low'] = min(price_min, price_last)
            df.ix[s, 'last'] = price_last

    return df

def calc_pct(stock_list, start_date, end_date, frequency = '1m'):
    pct_df = _get_my_price(stock_list, start_date, end_date, frequency)
    if pct_df.size > 0:
        pct_df['pct'] = (pct_df['last'] / pct_df['low'] - 1) * 100

    return pct_df

# 仅供研究平台使用
def calc_pct_(stock_list, start_date, end_date):
    pct_df = get_price(stock_list, start_date=start_date, end_date=end_date, frequency='1d', fields=['close'], adjust_type='pre')
    if type(pct_df) == pd.core.series.Series:
        pct_df = pd.DataFrame([[(pct_df[-1] / np.min(pct_df) - 1) * 100, pct_df[-1]]], columns = ['pct','last'], index = stock_list)
    else:
        pct_df = pd.DataFrame([[(pct_df.ix[-1,i] / np.min(pct_df.ix[:,i]) - 1) * 100, pct_df.ix[-1, i]] for i in range(0, len(stock_list))], columns = ['pct', 'last'], index = stock_list)
        
    return pct_df

def is_stop_profit(stock_list, now):
    if stock_list == None or len(stock_list) <= 0:
        return None
    
    start_date = datetime.timedelta(days=-365) + now
    df_price = calc_pct(stock_list, start_date, now)
    df_price['sp1'] = df_price['pct'] > 50
    df_price['sp2'] = df_price['pct'] > 100
    
    return df_price

def is_speculative(stock_list, now):
    if stock_list == None or len(stock_list) <= 0:
        return None
    
    start_date = datetime.timedelta(days=-42) + now
    df_price = calc_pct(stock_list, start_date, now)
    df_price['speculative'] = df_price['pct'] > 50
    
    return df_price

def pick_stocks(today, max_pct, frequency = '1d'):
    q1 = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.eod_derivative_indicator.pe_ratio,
            #fundamentals.financial_indicator.annual_return_on_equity
        ).filter(
            fundamentals.eod_derivative_indicator.pb_ratio <= CONSTRAINT_PB,
            fundamentals.eod_derivative_indicator.pe_ratio > 0,
            fundamentals.eod_derivative_indicator.pe_ratio <= CONSTRAINT_PE
        )
    df1 = get_fundamentals(q1, entry_date = today, interval = '1d')
    
    src_df = df1.iloc[:,0]
    
    stock_list = list(src_df.index)
    start_date = datetime.timedelta(days=-365) + today
    pct_df = calc_pct(stock_list, start_date, today, frequency)
    # pct_df.dropna(inplace=True)
    
    final_df = pd.concat([src_df, pct_df], axis=1)
    final_df = final_df[final_df['pct'] < max_pct]
    ind_df = replace_industry(list(final_df.index))
    final_df = pd.concat([final_df, ind_df], axis=1)
    
    calc_X_df(final_df)
    final_df = final_df[final_df['X'] > 0]
    # patch
    final_df['last'] = get_last_price(list(final_df.index), today, frequency = frequency, adjust_type = 'none')
    
    # more factors ...
    #pb_df = dao_spbpe.load_from_hdf('../data/big_data_pb.h5')
    #pb_q_df = dao_spbpe.calc_quantile(pb_df)
    #final_df['pb_rank'] = pb_q_df['分位点%']

    grouped_df = final_df.groupby('industry')
    # ... rank
    
    return grouped_df
        
def get_rank(pb, pe, pct, is_special):
    return get_pb_pe_rank(pb, pe, is_special) + get_pct_rank(pct)

def get_pb_pe_rank(pb, pe, is_special):
    result = []
    
    for idx in pb.index:
        num = round(pb[idx],2)* 15 + round(pe[idx],1)
        if is_special[idx]:
            result.append((50 - num)*0.8)
        else:
            result.append(50 - num)
    return pd.Series(result, index = pb.index)

def get_pct_rank(pct):
    result = []
    for idx in pct.index:
        if pct[idx] < 5.1:
            result.append(1)
        elif pct[idx] > 5.1 and pct[idx] < 10.1:
            result.append(0)
        elif pct[idx] > 10.1 and pct[idx] < 15.1:
            result.append(-1)
        else:
            result.append(-2)
    return pd.Series(result, index = pct.index)

def get_shenwan_industry_name(index_code):
    return SHENWAN_INDUSTRY[index_code]

def get_valuation(stock_list, now):
    q1 = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.eod_derivative_indicator.pe_ratio
        ).filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list))
    df1 = get_fundamentals(q1, entry_date = now, interval = '1d')
    
    src_df = df1.iloc[:,0]
    
    stock_list = list(src_df.index)
    start_date = datetime.timedelta(days=-365) + now
    pct_df = calc_pct(stock_list, start_date, now)
    
    final_df = pd.concat([src_df, pct_df], axis=1)
    ind_df = replace_industry(stock_list)
    final_df = pd.concat([final_df, ind_df], axis=1)
    
    calc_X_df(final_df)
    
    return final_df

def get_valuation_by_ind(industry, now):
    q1 = query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.eod_derivative_indicator.pe_ratio
        ).filter(
            fundamentals.eod_derivative_indicator.pb_ratio <= CONSTRAINT_PB,
            fundamentals.eod_derivative_indicator.pe_ratio > 0,
            fundamentals.eod_derivative_indicator.pe_ratio <= CONSTRAINT_PE
        )
    df1 = get_fundamentals(q1, entry_date = now, interval = '1d')
    
    src_df = df1.iloc[:,0]
    
    stock_list = list(src_df.index)
    ind_df = replace_industry(stock_list)
    src_df = pd.concat([src_df, ind_df], axis=1)
    src_df = src_df[src_df.industry == industry]
    
    stock_list = list(src_df.index)
    start_date = datetime.timedelta(days=-365) + now
    pct_df = calc_pct(stock_list, start_date, now)
    
    final_df = pd.concat([src_df, pct_df], axis=1)
    
    calc_X_df(final_df)
    
    return final_df

# 仅供研究平台使用
def output(data_df, today):
    i = 1
    print('-------- the date [%s] -------pick [%d]-----' % (today.strftime('%Y-%m-%d'), sum([len(x[1].index) for x in data_df])))
    print('')
    for s in data_df:
        tmp = s[1].ix[:,'industry_name']
        print('%d. %s - %s (%d)' % (i, tmp.values[0], s[0], tmp.values.size))
        i += 1
        tmp = s[1].sort_values(by='pb_ratio')
        tmp.drop(['industry', 'industry_name'], axis=1, inplace=True)
        tmp['rank'] = get_pb_pe_rank(tmp['pb_ratio'], tmp['pe_ratio']) + get_pct_rank(tmp['pct'])
        print(tmp)
        
