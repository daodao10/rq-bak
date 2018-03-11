import pandas as pd
import numpy as np
import datetime

# import dao_util as dao_u
import dao_stock_strategy as dao_ss

CUTLOSS_ENABLED = False
FORCED_REBALANCE_ENABLED = True
AUTO_RUN_ENABLED = True
ROUND = { 'month': False, 'quarter': False, 'half': True, 'annual': False, 'half_year': False, 'annual_year': False }
''' -------------------------------------------- '''

# 便宜(PB <= 1.5, PE <= 10) + 高股息(>= 3%, context.max_no_of_trade = 10)
# 便宜(PB <= 2.5, PE <= 15) + 高股息(>= 3.5%) + Low Volatility(252个交易日波动率 <= 20%, context.max_no_of_trade = 8)
# Graham_Num : run monthly
# Gramham_PE: run annually, cover more industries better
def set_stocks(context):
    # 便宜 (年化18%)
    # ir = dao_u.get_ir_3y(context.now.strftime('%Y%m%d'))
    ir = 3
    context.max_no_of_trade = 5
    dao_ss.set_stocks_by_cheap(context, {'pb':1.5, 'pe':10, 'div':ir, 'limit_per_industry':3, 'temp_filter':{"col":"pb", "degree": 50, "sorting": "temp"}})
    # dao_ss.set_stocks_by_cheap(context, {'pb':2, 'pe':20, 'div':ir, 'limit_per_industry':3, 'temp_filter':{"col":"pb", "degree": 50, "sorting": "origin"}})
    # dao_ss.set_stocks_by_cheap(context, {'pb':2.5, 'pe':25, 'div':ir, 'limit_per_industry':3, 'temp_filter':{"col":"pb", "degree": 50, "sorting": "origin"}})
     
    stocks = context.stocks
    # 便宜 ＋ 低波动 (年化18%)
    ir = 3.5
    context.max_no_of_trade = 3
    dao_ss.set_stocks_by_low_volatility(context, {'pb':2, 'pe':20, 'div':ir, 'volatility':20, 'period':180, 'limit_per_industry':3 })
    stocks.extend(context.stocks)
    
    # stocks = context.stocks
    # Graham's
    # 收益不稳定，每月平衡(年化20%+)
    # dao_ss.set_stocks_by_Graham_Num(context, {'pb':1.5, 'pe':15, 'current_ratio':2,'quick_ratio':1, 'inc_eps':0, 'inc_ebit': 0, 'limit_per_industry':3})
    # (年化18%)
    # dao_ss.set_stocks_by_Gramham_PE(context, {'years':7, 'limit_per_industry':2})
    # stocks.extend(context.stocks)

    context.stocks = list(set(stocks))

    # logger.info(context.stocks)
    

def init(context):
    
    context.no_of_candidates = 0

    context.allocation_ratio = 1.0
    context.unit_net_value = 1.0
    
    context.auto_run = AUTO_RUN_ENABLED
    
    reset_variables(context)
    
    scheduler.run_daily(place_orders, time_rule=market_open(minute=3))
    # scheduler.run_daily(dao_ss.print_open_order, time_rule=market_close(minute=0))
    scheduler.run_monthly(rebalance, 2, time_rule=market_open(minute=13))

def before_trading(context):
    if CUTLOSS_ENABLED:
        stock_list = []
        order_list = {}
        total_value = 0
        for s in context.portfolio.positions:
            position = context.portfolio.positions[s]
            if position.quantity > 0:
                if position.pnl < 0:
                    ratio = -position.pnl / position.market_value
                    if ratio > 0.33:
                        # close = history_bars(s, 1, '1d', 'close')
                        # print('==> loss %s: %.2f, %.2f' % (s, -ratio / (1 + ratio), close / position.avg_price))
                        print('==> loss %s: %.2f' % (s, -ratio / (1 + ratio)))
                        total_value += position.market_value
                    else:
                        stock_list.append(s)
                else:
                    stock_list.append(s)
    
        if total_value > 0:
            allocation_per_stock = total_value * context.allocation_ratio / len(stock_list)
            
            for s in stock_list:
                close = history_bars(s, 1, '1d', 'close')[0]
                lots_changed = allocation_per_stock / close / 100
                order_list[s] = [close, context.portfolio.positions[s].quantity / 100 + round(lots_changed - 0.4, 0)]
    
            context.stocks = stock_list
            context.orders = order_list
            context.to_summary = True
            context.attempt_times = 3
    else:
        pass

def handle_bar(context, bar_dict):
    pass

def after_trading(context):
    # 汇总
    if context.to_summary:
        cash = context.portfolio.cash
        market_value = context.portfolio.market_value
        total_value = context.portfolio.total_value
        unit_net_value = context.portfolio.unit_net_value
        logger.info('==> portfolio summary:')
        logger.info('cash: %.2f, market value: %.2f, total value: %.2f, unit net value: %.2f' % (cash, market_value, total_value, unit_net_value))
        
        positions = context.portfolio.positions
        if len(positions) > 0:
            logger.info('==> position:')
            for s in positions.keys():
                position = positions[s]
                logger.info('%s, quantity: %d, pnl: %.2f, market value: %.2f' % (position.order_book_id,position.quantity,position.pnl,position.market_value))

        # reset order_list:
        for s in positions.keys():
            if s in context.orders and positions[s].quantity / 100 == context.orders[s][1]:
                del context.orders[s]
 
        # context.to_summary = False
        logger.info('====================================')

def reset_variables(context):
    context.stocks = []
    context.orders = {}
    
    context.to_summary = False
    context.attempt_times = 0

def rebalance(context, bar_dict):
    month = context.now.month

    if context.auto_run:
        rebalance_round(context, bar_dict)
        return
    
    if ROUND['quarter'] and (month == 1 or month == 4 or month == 7 or month == 10):
        rebalance_round(context, bar_dict)
    elif ROUND['half'] and (month == 5 or month == 9):
        rebalance_round(context, bar_dict)
    elif ROUND['annual'] and month == 5:
        rebalance_round(context, bar_dict)
    elif ROUND['half_year'] and (month == 1 or month == 7):
        rebalance_round(context, bar_dict)
    elif ROUND['annual_year'] and month == 1:
        rebalance_round(context, bar_dict)
    elif ROUND['month']:
        rebalance_round(context, bar_dict)

def rebalance_round(context, bar_dict):
    
    context.auto_run = False
    
    set_stocks(context)

    dao_ss.set_allocation_ratio(context)
    
    # rebalance ---------------------
    
    not_found_stocks = True if len(context.stocks) == 0 else False
    context.to_summary = True
    context.attempt_times = 3
    
    bbb(context, bar_dict, not_found_stocks)
        
def aaa(context, bar_dict, not_found_stocks):
    order_list = {}
    total_value = context.portfolio.total_value
    for s in context.portfolio.positions:
        if is_suspended(s):
            # 需要扣除停牌无法交易的仓位
            total_value -= context.portfolio.positions[s].market_value
        elif FORCED_REBALANCE_ENABLED and not_found_stocks:
            if context.portfolio.positions[s].quantity > 0:
                context.stocks.append(s)
        
    if len(context.stocks) > 0:
        allocation_per_stock = total_value * context.allocation_ratio / len(context.stocks)
        
        for s in context.stocks:
            lots = allocation_per_stock / bar_dict[s].close / 100
            lots = round(lots - 0.4, 0)
            order_list[s] = [bar_dict[s].close, lots]
        
        context.orders = order_list
        place_orders(context, bar_dict)

def bbb(context, bar_dict, not_found_stocks):
    order_list = {}
    if not(not_found_stocks):
        total_value = context.portfolio.total_value
        for s in context.portfolio.positions:
            if is_suspended(s):
                # 需要扣除停牌无法交易的仓位
                total_value -= context.portfolio.positions[s].market_value
        
        allocation_per_stock = total_value * context.allocation_ratio / len(context.stocks)
        
        for s in context.stocks:
            lots = allocation_per_stock / bar_dict[s].close / 100
            lots = round(lots - 0.4, 0)
            order_list[s] = [bar_dict[s].close, lots]

    context.orders = order_list
    place_orders(context, bar_dict)

def place_orders(context, bar_dict):
    stock_list = context.stocks
    order_list = context.orders
    
    # if context.attempt_times > 0 and len(order_list) > 0:# aaa()
    if context.attempt_times > 0:# bbb()
        context.attempt_times -= 1
            
        dao_ss.print_order_list(order_list)
        
        positions = context.portfolio.positions
        position_stocks = {}
        lots_changed = {}
        
        # sell first
        for s in positions.keys():
            if positions[s].quantity > 0:
                position_stocks[s] = True
                if s in order_list:
                    lots = positions[s].quantity / 100
                    add_lots = order_list[s][1] - lots
                    if add_lots != 0:
                        lots_changed[s] = add_lots

                if s not in stock_list:
                    order_target_value(s, 0, style=LimitOrder(bar_dict[s].close))
        
        # open position: new buy
        for s in order_list:
            if s not in position_stocks:
                order_lots(s, order_list[s][1], style=LimitOrder(bar_dict[s].close))

        # adjust position: lots changed
        for s in lots_changed:
            order_lots(s, lots_changed[s], style=LimitOrder(bar_dict[s].close))
    else:
        reset_variables(context)

