import datetime
import pandas as pd
import numpy as np
import dao_order_tools as dao_ot
import dao_bank_strategy as dao_bs
import dao_util as dao_u

def init(context):
    context.listed_stocks = ['600919.XSHG', '601988.XSHG', '601009.XSHG', '601997.XSHG', '601398.XSHG', '600926.XSHG', '601939.XSHG', '600036.XSHG', '002807.XSHE', '000001.XSHE', '600016.XSHG', '601229.XSHG', '601128.XSHG', '601998.XSHG', '601288.XSHG', '002142.XSHE', '601818.XSHG', '600908.XSHG', '603323.XSHG', '600015.XSHG', '601169.XSHG', '600000.XSHG', '601328.XSHG', '002839.XSHE', '601166.XSHG']
    
    context.max_no_of_trade = 2
    context.no_of_candidates = 1
    
    context.ref_index_pe = 100
    
    context.market_order_enalbed = False
    
    context.market_value = 0
    context.cut_loss_enalbed = False
    context.cut_loss_trigger = 0.9

    scheduler.run_monthly(rebalance, -1, time_rule=market_open(minute=13))

    
def before_trading(context):
    context.cut_loss_fired = False
    context.ref_index_pe = dao_u.get_ref_val('note/pe.csv', '000001.XSHG', context.now.strftime('%Y-%m-%d'), percent = 0.67, default = 30)
    # context.ref_index_pe = dao_u.get_ref_val('note/pe.csv', '000001.XSHG', context.now.strftime('%Y-%m-%d'), n_quantile = 6, default = 30)
    # logger.info(context.ref_index_pe)


def handle_bar(context, bar_dict):
    if context.cut_loss_enalbed:
        dao_ot.cut_loss(context, bar_dict)
    else:
        pass


def rebalance(context, bar_dict):
    # month = context.now.month
    # if month == 4 or month == 8 or month == 10:
    if get_direction(context) == SIDE.SELL:
        dao_ot.clear_positions(context, bar_dict)
        return
    
    get_stocks(context)
    dao_ot.rebalance(context, bar_dict)


def get_stocks(context):
    stocks = [s for s in context.listed_stocks if not(is_suspended(s) or is_st_stock(s))]
    year = context.now.year

    entry_date = context.now + datetime.timedelta(days=-1)
    # logger.info(entry_date)
    
    if year >= 2010:
        ''' 
        (pb+策略1)在beta和sharp方面由于单纯的pb策略
        (pb+策略2+策略1) = (pb+策略3) 在 alpha, beta 和 sharp 方面都优胜
        策略2的表现略逊
        '''
        # # 稳健策略
        # stocks = dao_bs.get_stocks_by_pb(stocks, entry_date)
        
        # # # 激进策略
        # # stocks = dao_bs.get_stocks_by_pcf(stocks, entry_date)

        # # logger.info('1. %s' % stocks)
        
        # # 结合以下：增强策略
        # # stocks = dao_bs.get_stocks1_by_eod(stocks, entry_date, use_roa = False)
        # # stocks = dao_bs.get_stocks1_by_quarter_report(stocks, entry_date, n_report_quarters = 20, use_roa = False)
        
        # # stocks = dao_bs.get_stocks2_by_eod(stocks, entry_date, 8, use_roa = True)
        # # stocks = dao_bs.get_stocks2_by_quarter_report(stocks, entry_date, 8, n_report_quarters = 13, use_roa = True)
        
        # stocks = dao_bs.get_stocks3_by_eod(stocks, entry_date, 8)
        # # stocks = dao_bs.get_stocks3_by_quarter_report(stocks, entry_date, 8, n_report_quarters = 20)
        
        stocks = dao_bs.get_stocks_by_temperature(stocks, entry_date)
        
        # logger.info('2. %s' % stocks)
    else:
        stocks = dao_bs.get_stocks_by_pe(stocks, entry_date, context.ref_index_pe)
    
    context.stocks = []
    for c in stocks:
        if len(context.stocks) < context.max_no_of_trade + context.no_of_candidates:
            context.stocks.append(c)
        else:
            break


def get_direction(context):
    return SIDE.BUY
    # 000300.XSHG,000002.XSHG,000016.XSHG,000951.XSHG,
    # return dao_ot.get_direction_by_regress(context,'000300.XSHG', 21)
