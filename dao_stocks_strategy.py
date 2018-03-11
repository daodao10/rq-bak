import pandas as pd
import numpy as np
import datetime

import dao_util as dao_u

from rqdatac import * 

def roe_latest_handler(df, col):
    return df.ix[0, col]
def roe_mean_handler(df, col):
    return df.ix[:, col].mean()
ROE_HANDLER = roe_mean_handler

from enum import Enum
class Optimization(Enum):
    # 1: ROE, 2: PB, 3: SCORE
    ROE = 1
    PB = 2
    SCORE = 3

class Origin(Enum):
    ROE = 1
    INDEX = 2

def is_st_stock_(order_book_id,now):
    return is_st_stock(order_book_id,start_date=now,end_date=now).ix[-1,0]

def is_suspended_(order_book_id,now):
    return is_suspended(order_book_id,start_date=now,end_date=now).ix[-1,0]

# 连续7～10年ROE > 15%
def set_stocks_beauty_by_roe(context, options):
    year = context.now.year
    interval = '10y' if year >= 2011 else ('3y' if year < 2008 else '7y')
    # quarter = str(year - 1) + 'q4'
    quarter = dao_u.get_report_latest_year(context.now)

    data = get_stock_by_roe(context.now, quarter, interval)
    # print_stock_list(data.index, 'roe filter')

    # filter by pb
    stock_list = filter_by_pb(data, context.now + datetime.timedelta(days = -1), interval, Origin.ROE, options['optimization'])
    print_stock_list(stock_list, 'roe + pb filter')

    if 'temp_filter' in options:
        stock_list = is_allowed_temperature(stock_list, context.now, options['temp_filter'])
        print_stock_list(stock_list, 'temp filter')

    set_stocks(context, stock_list)

def set_stocks_beauty_by_r15(context, options):
    year = context.now.year
    interval = '10y' if year >= 2011 else ('3y' if year < 2008 else '7y')
    # quarter = str(year - 1) + 'q4'
    quarter = dao_u.get_report_latest_year(context.now)

    data = get_stock_by_roe(context.now, quarter, interval)
    
    stock_list = list(data[data>19.8].index)
    print_stock_list(stock_list, 'R15')
    
    if 'temp_filter' in options:
        stock_list = is_allowed_temperature(stock_list, context.now, options['temp_filter'])
        print_stock_list(stock_list, 'temp filter')

    set_stocks(context, stock_list)

def set_stocks_beauty_by_index(context, options):
    year = context.now.year
    interval = '10y' if year > 2014 else ('3y' if year < 2008 else '7y')
    
    data = get_stock_by_index(options['index'], context.now)

    # filter by pb
    stock_list = filter_by_pb(data, context.now + datetime.timedelta(days = -1), interval, Origin.INDEX, Optimization.PB)
    print_stock_list(stock_list, options['index'] + ' + pb filter')

    if 'temp_filter' in options:
        stock_list = is_allowed_temperature(stock_list, context.now, options['temp_filter'])
        print_stock_list(stock_list, 'temp filter')

    stock_list = limit_by_shenwan_industry(stock_list, options['limit_per_industry'], context.now)
    context.stocks = stock_list[0:(context.max_no_of_trade + context.no_of_candidates)]

def set_stocks_by_Graham_Num(context, options):
    end_date = context.now + datetime.timedelta(days=-1)
    df = get_fundamentals(
        query()
        .filter(
            fundamentals.eod_derivative_indicator.pb_ratio <= options['pb'],
            fundamentals.eod_derivative_indicator.pe_ratio > 0,
            fundamentals.eod_derivative_indicator.pe_ratio <= options['pe']
        )
        .filter(
            fundamentals.financial_indicator.inc_earnings_per_share > options['inc_eps'],
            fundamentals.financial_indicator.inc_profit_before_tax > options['inc_ebit']
        )
        .filter(
            fundamentals.financial_indicator.current_ratio > options['current_ratio'],
            fundamentals.financial_indicator.quick_ratio > options['quick_ratio']
        )
        .order_by(
            fundamentals.eod_derivative_indicator.market_cap.desc()
        )
        .limit(
            5 * (context.max_no_of_trade + context.no_of_candidates)
        ), entry_date = end_date, interval = '1d'
    )
    
    if df is None:
        return []
    
    stock_list = list(df.iloc[:,0].index)

    set_stocks(context, stock_list)
    print_stock_list(context.stocks, 'final')

def set_stocks_by_Gramham_PE(context, options):
    today = context.now

    df = get_fundamentals(query(
        fundamentals.eod_derivative_indicator.market_cap
    ), entry_date = today, interval = '1d')
  
    interval = str(options['years']) + 'y'
    quarter = dao_u.get_report_latest_year(today)
    df1 = get_financials(query(
        fundamentals.financial_indicator.adjusted_net_profit
    ), quarter = quarter, interval = interval)
    df1.dropna(axis=1, inplace=True)
    x_series = df1.iloc[0,:] > 0
    df1 = df1[x_series[x_series==True].index]

    df_final = df[:,0]
    df_final['avg_net_profit'] = df1.sum() / options['years']
    df_final['graham_pe'] = df_final['market_cap'] / df_final['avg_net_profit']
    df_final.dropna(inplace=True)

    xx = df_final[df_final['avg_net_profit']>0]
    xx.sort_values(by='graham_pe', ascending=True, inplace = True)
    industries = [[inst.symbol, inst.shenwan_industry_name, inst.shenwan_industry_code] for inst in instruments(list(xx.index))]
    yy = pd.DataFrame(data = industries, index = list(xx.index), columns = ['symbol','industry_name','industry_code'])
    df_final = pd.concat([xx,yy],axis=1)

    stock_list = limit_by_shenwan_industry(df_final.index, options['limit_per_industry'], today)
    print_stock_list(stock_list, 'Graham PE')
    
    set_stocks(context, stock_list)

def set_stocks_by_shenqigongshi(context, options):
    end_date = context.now
    df_x = _get_df_by_shenqigongshi(end_date)

    df_x_ = df_x.iloc[0:options['limit']]
    to_remove_stocks = []
    
    if options['industry_limitation']:
        stocks = list(df_x.index)
        industries = [[inst.symbol, inst.shenwan_industry_name, inst.shenwan_industry_code] for inst in instruments(stocks)]
        industry_df = pd.DataFrame(data = industries, index = stocks, columns = ['symbol','industry_name','industry_code'])
        industry_df['No.'] = range(1, len(stocks)+1)
        df_x = pd.concat([df_x, industry_df], axis=1)
        
        industry_inlist = {}
        for s in df_x_.index:
            if is_suspended_(s,end_date) or is_st_stock_(s,end_date) or df_x_.ix[s].industry_code in industry_inlist:
                to_remove_stocks.append(s)
            else:
                industry_inlist[df_x_.ix[s].industry_code] = 1
    
    else:
        for s in df_x_.index:
            if is_suspended_(s,end_date) or is_st_stock_(s,end_date):
                to_remove_stocks.append(s)
        
    df_x_ = df_x_.drop(to_remove_stocks)
    context.stocks = list(df_x_.index)[0:(context.max_no_of_trade + context.no_of_candidates)]
    # return df_x_

def _set_stocks_by_cheap_or_low_volatility(context, options, by_method):
    stock_count = 100 if 10 * (context.max_no_of_trade + context.no_of_candidates) < 100 else 10 * (context.max_no_of_trade + context.no_of_candidates)
    end_date = context.now + datetime.timedelta(days=-1)
    df = get_fundamentals(
        query(
            fundamentals.eod_derivative_indicator.pb_ratio,
            fundamentals.eod_derivative_indicator.pe_ratio
        )
        .filter(
            fundamentals.eod_derivative_indicator.pb_ratio <= options['pb'],
            fundamentals.eod_derivative_indicator.pe_ratio > 0,
            fundamentals.eod_derivative_indicator.pe_ratio <= options['pe']
            #, fundamentals.eod_derivative_indicator.dividend_yield >= options['div']
        )
        .order_by(
            fundamentals.eod_derivative_indicator.market_cap.desc()
            # fundamentals.eod_derivative_indicator.pb_ratio.asc()
        )
        .limit(
            stock_count
        ), entry_date = end_date, interval = '1d'
    )
    if df is None:
        print_stock_list([], by_method)
        return []

    if type(df) == pd.core.frame.DataFrame:
        df = df.T
    else:
        df = df.iloc[:,0]

    div_list = []
    for s in list(df.index):
        div_list.append(dao_u.get_dividend_(s, end_date))
    df['div'] = div_list
    df1 = df[df['div'] >= options['div']]
    
    if by_method == 'low volatility':
        volatility_list = []
        if df1.size > 0:
            days = -(options['period'] if 'period' in options else 365)
            for s in list(df1.index):
                volatility_list.append(dao_u.calc_volatility(s, end_date, days))
        df1['volatility'] = volatility_list
        df1 = df1[df1['volatility'] <= options['volatility']]

    stock_list = []
    if df1.size > 0:
        df1 = df1.sort_values(by='pb_ratio')
        df1['pb_score'] = range(1, len(df1.index)+1)
        
        df1 = df1.sort_values(by='pe_ratio')
        df1['pe_score'] = range(1, len(df1.index)+1)
        
        if by_method == 'cheap':
            # score of dividend
            df1 = df1.sort_values(by='div', ascending=False)
            df1['div_score'] = range(1, len(df1.index)+1)
            
            df1['score'] = df1['pb_score'] + df1['pe_score'] + df1['div_score']
        else:
            df1['score'] = df1['pb_score'] + df1['pe_score']
        df1.sort_values(by='score', inplace=True)
        
        stock_list = list(df1.index)
        print_stock_list(stock_list, by_method)

        if 'temp_filter' in options:
            stock_list = is_allowed_temperature(stock_list, context.now, options['temp_filter'])
            print_stock_list(stock_list, 'temp filter')

        stock_list = limit_by_shenwan_industry(stock_list, options['limit_per_industry'], context.now)
    
    print_stock_list(stock_list, 'final')
    context.stocks = stock_list[0:(context.max_no_of_trade + context.no_of_candidates)]

def set_stocks_by_cheap(context, options):
    _set_stocks_by_cheap_or_low_volatility(context, options, 'cheap')

def set_stocks_by_low_volatility(context, options):
    _set_stocks_by_cheap_or_low_volatility(context, options, 'low volatility')

def set_stocks(context, stock_list):
    context.stocks = []
    for s in stock_list:
        if is_suspended_(s,context.now) or is_st_stock_(s,context.now):
            continue
        if len(context.stocks) < context.max_no_of_trade + context.no_of_candidates:
            context.stocks.append(s)
        else:
            break

# ----------------------- beauty stocks ---------------------------------------
def get_stock_by_roe(now, quarter, interval):
    annual_report_df = get_financials(
    query(
        # fundamentals.financial_indicator.return_on_equity_diluted
        # fundamentals.financial_indicator.adjusted_return_on_equity_diluted
        # fundamentals.financial_indicator.adjusted_return_on_equity_weighted_average
        fundamentals.financial_indicator.adjusted_return_on_equity_average
    ), quarter = quarter, interval = interval)
    inst_list = dao_u.get_instruments([s for s in list(annual_report_df.columns.values) if (annual_report_df[s] >= 15).all()])
    
    stock_list = []
    roe_list = []
    for inst in inst_list:
        if inst.days_from_listed(now) > 250 and is_good_industry(inst.shenwan_industry_code):
            roe_list.append(ROE_HANDLER(annual_report_df, inst.order_book_id))
            stock_list.append(inst.order_book_id)

    roe_series = pd.Series(roe_list, index = stock_list)
    roe_series.sort_values(ascending = False, inplace = True)
    
    return roe_series

def get_stock_by_index(index_code, today):
    inst_list = dao_u.get_instruments(index_components(index_code, today))
    
    return [inst.order_book_id for inst in inst_list if is_good_industry(inst.shenwan_industry_code)]

def filter_by_pb(data, end_date, interval, origin, strategy):
    pb_ref = 8
    stock_list = list(data.index) if origin == Origin.ROE else data
    
    if len(stock_list) > 0:
        eod_panel = get_fundamentals(
        query(
            fundamentals.eod_derivative_indicator.pb_ratio
        )
        .filter(
            fundamentals.eod_derivative_indicator.stockcode.in_(stock_list)
        ), entry_date = end_date, interval = interval)
        
        if not(eod_panel.empty):
            eod_df = eod_panel.iloc[0]
            
            if origin == Origin.ROE:
                # 参考公式：PB <= 2 ^ (ROE% * 10)    
                data = np.round(data / 10.0, 2)
                if type(eod_df) == pd.core.series.Series:
                    stocks = [(col, eod_df[col]) for col in stock_list if eod_df[col] <= pb_ref and (2**data[col] >= eod_df[col])]
                else:
                    # stocks = [(col, eod_df.ix[0, col]) for col in stock_list if (eod_df[col] <= pb_ref).all() and (2**data[col] >= eod_df.ix[0, col])]
                    stocks = [(col, eod_df.ix[0, col]) for col in stock_list if (eod_df.ix[0, col] <= pb_ref) and (2**data[col] >= eod_df.ix[0, col])]
            else:
                if type(eod_df) == pd.core.series.Series:
                    stocks = [(col, eod_df[col]) for col in stock_list if eod_df[col] <= pb_ref]
                else:
                    stocks = [(col, eod_df.ix[0, col]) for col in stock_list if (eod_df[col] <= pb_ref).all()]
                    
            if len(stocks) > 0:
                idx, pb = zip(*stocks)

                if strategy == Optimization.ROE:
                    # 1) 返回列表：保持ROE的顺序
                    return [x for x in stock_list if x in idx]
                else:
                    # 2) or 3)
                    pb_score = pd.Series(pb, idx)
                    pb_score.sort_values(ascending = True, inplace = True)
                    
                    if strategy == Optimization.SCORE:
                        # 返回列表：按得分(score)顺序
                        pb_score = pd.Series(range(1, len(idx) + 1), pb_score.index)
                        roe_score = pd.Series(range(1, len(idx) + 1), idx)
                        
                        score = pb_score + roe_score
                        score.sort_values(ascending = True, inplace = True)
                        return list(score.index)
                    else:
                        # Optimization.PB：返回列表按PB的顺序
                        return list(pb_score.index)
            else:
                print(eod_df)

    return []

# ------------------------------- shenqigongshi ---------------------------------------------
def _get_df_by_shenqigongshi(end_date):
    rotc_df = calc_rotc(end_date)
    yield_df = calc_ebit_to_ev(end_date)

    df_final = pd.concat([rotc_df['rotc'], rotc_df['score_1'], yield_df['yield'], yield_df['score_2']], axis=1)
    df_final['score'] = df_final['score_1'] + df_final['score_2']
    df_final.sort_values(by='score', ascending=True, inplace=True)

    return df_final

def _calc_rotc_1(ebit, tc):
    if ebit <= 0 or tc == 0:
        return float('nan')
    if tc < 0:
        return abs(ebit/tc) * 100 + 100000
    return ebit/tc * 100

def _calc_rotc_2(ebit, tc):
    if ebit <= 0 or tc == 0:
        return float('nan')

    return ebit/tc * 100

def calc_rotc(end_date):
    #end_date = context.now
    quarter = dao_u.get_report_quarter(end_date)
    q = query(
        financials.financial_indicator.ebit,
        financials.financial_indicator.net_working_capital,
        # financials.financial_indicator.working_capital,
        financials.balance_sheet.accts_receivable,
        financials.balance_sheet.other_accts_receivable,
        financials.balance_sheet.prepayment,
        financials.balance_sheet.inventory,
        financials.financial_indicator.non_interest_bearing_current_debt,
        financials.balance_sheet.long_term_equity_investment,
        financials.balance_sheet.net_fixed_assets
    )
    rotc_df = get_financials(q, quarter = quarter, interval = '1q')
    # rotc_df.fillna({'accts_receivable':0,'other_accts_receivable':0,'prepayment':0,'inventory':0,'non_interest_bearing_current_debt':0,'long_term_equity_investment':0}, inplace=True)

    # shengqigongshi 版
    # 净营运资本 = 应收账款 + 其他应收款 + 预付账款 + 存货 - 无息流动负债 + 长期股权投资
    rotc_df['tangible_capital_2'] = rotc_df['accts_receivable'] + rotc_df['other_accts_receivable'] + rotc_df['prepayment'] + rotc_df['inventory'] - rotc_df['non_interest_bearing_current_debt'] + rotc_df['long_term_equity_investment'] + rotc_df['net_fixed_assets']
    # 海外版
    rotc_df['tangible_capital'] = rotc_df['net_working_capital'] + rotc_df['net_fixed_assets']

    # rotc_df['rotc'] = rotc_df['ebit']/rotc_df['tc'] * 100
    rotc_df['rotc'] = list(map(_calc_rotc_1, rotc_df['ebit'], rotc_df['tangible_capital']))
    # 参考
    rotc_df['rotc_2'] = rotc_df['ebit']/rotc_df['tangible_capital_2'] * 100

    rotc_df.sort_values(by='rotc', ascending=False, inplace=True)
    rotc_df['score_1'] = range(1, len(rotc_df.index) + 1)

    rotc_df['rotc'] = list(map(_calc_rotc_2, rotc_df['ebit'], rotc_df['tangible_capital']))

    return rotc_df

def calc_ebit_to_ev(end_date):
    #end_date = context.now
    quarter = dao_u.get_report_quarter(end_date)
    q = query(
        financials.financial_indicator.ebit,
        financials.balance_sheet.minority_interest,
        financials.financial_indicator.interest_bearing_debt,
        financials.balance_sheet.total_liabilities,
        financials.financial_indicator.non_interest_bearing_current_debt,
        financials.financial_indicator.non_interest_bearing_non_current_debt,
        financials.balance_sheet.cash
    )
    yield_df = get_financials(q, quarter = quarter, interval = '1q')

    market_cap_df = get_fundamentals(
        query(
            fundamentals.eod_derivative_indicator.market_cap,
            fundamentals.eod_derivative_indicator.ev_2
        ), entry_date = end_date + datetime.timedelta(days=-1), interval = '1d')

    market_cap_df = market_cap_df.iloc[:,0]
    yield_df = pd.concat([yield_df, market_cap_df], axis=1)

    # shengqigongshi 版
    # interest_bearing_debt = total_liabilities - non_interest_bearing_current_debt - non_interest_bearing_non_current_debt
    yield_df['ev_my'] = yield_df['market_cap'] + yield_df['minority_interest'] + (yield_df['total_liabilities'] - yield_df['non_interest_bearing_current_debt'] - yield_df['non_interest_bearing_non_current_debt'])
    # # 长投版
    # yield_df['ev_my'] = yield_df['market_cap'] + yield_df['interest_bearing_debt'] - yield_df['cash']
    # # 海外版
    # yield_df['ev_my'] = yield_df['market_cap'] + yield_df['interest_bearing_debt']
    yield_df['yield'] = yield_df['ebit']/yield_df['ev_my'] * 100
    # RQ 的参考
    yield_df['yield_2'] = yield_df['ebit']/yield_df['ev_2']

    yield_df.sort_values(by='yield', ascending=False, inplace=True)
    yield_df['score_2'] = range(1, len(yield_df.index) + 1)

    return yield_df

# ------------------ others ---------------------
def limit_by_shenwan_industry(stocks, limit_per_industry, now):
    if limit_per_industry == 0:
        return stocks
    
    stock_list = []
    
    inst_list = dao_u.get_instruments(stocks)
    counter = {}
    for inst in inst_list:
        if is_suspended_(inst.order_book_id, now) or is_st_stock_(inst.order_book_id, now):
            continue

        if inst.shenwan_industry_code in counter:
            counter[inst.shenwan_industry_code] += 1
        else:
            counter[inst.shenwan_industry_code] = 1
        
        if counter[inst.shenwan_industry_code] <= limit_per_industry:
            stock_list.append(inst.order_book_id)

    return stock_list

def is_allowed_temperature(stocks, now, temp_filter):
    if stocks and len(stocks) > 0:
        temp_df = dao_u.get_current_temperature(stocks, now)
        col = 'pb_ratio_tmp' if temp_filter['col'] == 'pb' else 'pe_ratio_tmp'
        temp_df = temp_df[temp_df[col] <= temp_filter['degree']]
        if temp_filter['sorting'] == 'origin':
            # 1) don't change the original order of the stock list
            return [s for s in stocks if s in list(temp_df.index)]
        else:
            # 2) sorting by tmp
            return list(temp_df.sort_values(by=col).index)
    else:
        return stocks

def is_good_industry(ind):
    return {
        '801010.INDX':True, #'农林牧渔'
        '801020.INDX':False, #'采掘'
        '801030.INDX':True, #'化工'
        '801040.INDX':False, #'钢铁'
        '801050.INDX':True, #'有色金属'
        '801080.INDX':True, #'电子'
        '801110.INDX':True, #'家用电器'
        '801120.INDX':True, #'食品饮料'
        '801130.INDX':True, #'纺织服装'
        '801140.INDX':True, #'轻工制造'
        '801150.INDX':True, #'医药生物'
        '801160.INDX':True, #'公用事业'
        '801170.INDX':True, #'交通运输'
        '801180.INDX':False, #'房地产'
        '801200.INDX':True, #'商业贸易'
        '801210.INDX':True, #'休闲服务'
        '801230.INDX':True, #'综合'
        '801710.INDX':True, #'建筑材料'
        '801720.INDX':True, #'建筑装饰'
        '801730.INDX':True, #'电气设备'
        '801740.INDX':True, #'国防军工'
        '801750.INDX':True, #'计算机'
        '801760.INDX':True, #'传媒'
        '801770.INDX':True, #'通信'
        '801780.INDX':True, #'银行'
        '801790.INDX':True, #'非银金融'
        '801880.INDX':True, #汽车'
        '801890.INDX':True, #'机械设备'
    }.get(ind)

def set_allocation_ratio(context):
    ratio = context.portfolio.unit_net_value / context.unit_net_value
    if ratio > 3:
        context.allocation_ratio = 0.3
    elif ratio > 2:
        context.allocation_ratio = 0.5
    elif ratio > 1.8:
        context.allocation_ratio = 0.6
    elif ratio > 1.5:
        context.allocation_ratio = 0.75
    else:
        context.allocation_ratio = 1.0
    context.unit_net_value = context.portfolio.unit_net_value
    logger.info('==> allocation ratio: %.2f' % context.allocation_ratio)

def print_stock_list(stock_list, msg = ''):
    logger.info('==> %s stock list:' % msg)
    if len(stock_list) > 0:
        inst_list = dao_u.get_instruments(stock_list)
        for inst in inst_list:
            logger.info('%s(%s) - %s' % (inst.order_book_id, inst.symbol, inst.shenwan_industry_name))
    else:
        logger.info('   empty   ')

def print_order_list(order_list):
    if len(order_list) > 0:
        logger.info('==> order list:')
        for s in order_list.keys():
            logger.info('%s: %s' % (s, order_list[s]))

def print_open_order(context, bar_dict):
    open_orders = get_open_orders()
    if len(open_orders) > 0:
        logger.info('==> open order:')
        for x in open_orders:
            logger.info('%s %s@%.3f: %d, %d, %d' % (x.order_book_id, x.side, x.price, x.quantity, x.filled_quantity, x.unfilled_quantity))

