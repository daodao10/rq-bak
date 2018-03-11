import dao_order_tools as dao_ot

'''
 周模式注意的问题：需要提前储存4周的历史数据，也就是说要么提前获取4周历史数据；要么把测试的起始时间提前四周
 对于日模式，回测显示20日优于21日
'''
ICASH_ENABLED = False
WEEKLY_MODE = False
if WEEKLY_MODE:
    DELTA = 4
else:
    DELTA = 20
XUEQIU_ALGORITHM = False

def init(context):
    context.icash = '511880.XSHG' #511880.XSHG,511990.XSHG,511010.XSHG
    
    # # trade index
    # # 1) weekly best pick
    # context.i1 = {"id":"CSI300.INDX", "trade_ids":["000300.XSHG"], "price":[]} # 000016：红利指数, 000925：基本面50
    # context.i2 = {"id":"CSI500.INDX", "trade_ids":["000905.XSHG"], "price":[]}
    # # 2) daily best pick
    # context.i1 = {"id":"CSI300.INDX", "trade_ids":["000300.XSHG","000925.XSHG"], "price":[]}
    # context.i2 = {"id":"399006.XSHE", "trade_ids":["000905.XSHG","399006.XSHE"], "price":[]}
    # # others
    # context.i2 = {"id":"399005.XSHE", "trade_ids":["399005.XSHE"], "price":[]}
    # context.i2 = {"id":"399008.XSHE", "trade_ids":["399008.XSHE"], "price":[]}
   
    # # trade ETF
    # 50可选：160716.XSHE(基本面50),510680.XSHG
    # 300可选：159919.XSHE, 159924.XSHE(300等权), 166007.XSHE, 000172/000311(指数增强)
    # 500可选：159922.XSHE, 160119.XSHE, 000478(指数增强)
    # 创业板：160223.XSHE
    # # 1)
    # context.i1 = {"id":"CSI300.INDX", "trade_ids":["510300.XSHG"], "price":[]}
    # context.i2 = {"id":"CSI500.INDX", "trade_ids":["510500.XSHG"], "price":[]}
    # # 2)
    context.i1 = {"id":"CSI300.INDX", "trade_ids":["510300.XSHG","510050.XSHG"], "price":[]}
    # context.i1 = {"id":"CSI300.INDX", "trade_ids":["510300.XSHG","160716.XSHE"], "price":[]}
    context.i2 = {"id":"399006.XSHE", "trade_ids":["159915.XSHE","510500.XSHG"], "price":[]}
    # # others
    # context.i2 = {"id":"CSI500.INDX", "trade_ids":["510500.XSHG","159915.XSHE"], "price":[]}
    # context.i1 = {"id":"SSE50.INDX", "trade_ids":["510050.XSHG"], "price":[]}
    # context.i2 = {"id":"399006.XSHE", "trade_ids":["159915.XSHE"], "price":[]}
    # context.i2 = {"id":"399005.XSHE", "trade_ids":["159902.XSHE"], "price":[]}
    # context.i2 = {"id":"399008.XSHE", "trade_ids":["159907.XSHE"], "price":[]}
    
    context.market_order_enalbed = False
    
    context.market_value = 0
    context.cut_loss_enalbed = True
    context.cut_loss_trigger = 0.90
    
    if WEEKLY_MODE:
        scheduler.run_weekly(hist_weekly, tradingday=-1, time_rule=market_close(minute=0))
        scheduler.run_weekly(trade_by_weekly, tradingday=-1, time_rule=market_close(minute=5))
    else:
        scheduler.run_daily(trade, time_rule=market_close(minute=5))
    
    if ICASH_ENABLED:
        logger.info('To make the profit max, please feel free to buy cash instruments like: 511880.XSHG,511990.XSHG,511010.XSHG')
    logger.info('---------------------------------')

def before_trading(context):
    context.cut_loss_fired = False

def handle_bar(context, bar_dict):
    if context.cut_loss_enalbed:
        dao_ot.cut_loss(context, bar_dict)
    else:
        pass

def after_trading(context):
    logger.info('===================')
    pass

'''-------------------------- DAILY --------------------------------'''
def trade(context, bar_dict):
    # there is some difference from history_bars running by day-mode and running by minute-mode
    if context.run_info.frequency == '1m':
        count = DELTA
    else:
        count = DELTA + 1
    
    i1_price = history_bars(context.i1["id"], count, '1d', 'close')
    i2_price = history_bars(context.i2["id"], count, '1d', 'close')
    if len(i2_price) > count - 1:
        if XUEQIU_ALGORITHM:
            delta_i1 = bar_dict[context.i1["id"]].last / ((i1_price[0] + i1_price[1] + i1_price[2]) / 3.0)
            delta_i2 = bar_dict[context.i2["id"]].last / ((i2_price[0] + i2_price[1] + i2_price[2]) / 3.0)
        else:
            delta_i1 = bar_dict[context.i1["id"]].last / i1_price[0]
            delta_i2 = bar_dict[context.i2["id"]].last / i2_price[0]
        logger.info('2.[%s: %.2f], 8.[%s: %.2f]' % (context.i1['id'], (delta_i1 - 1), context.i2['id'], (delta_i2 - 1)))
        if delta_i1 > 1 and delta_i1 > delta_i2:
            # to buy i1
            trade_by_singal(context, bar_dict, context.i1["trade_ids"])
        elif delta_i2 > 1 and delta_i1 < delta_i2:
            # to buy i2
            trade_by_singal(context, bar_dict, context.i2["trade_ids"])
        else:
            trade_by_singal(context, bar_dict)
            
'''-------------------------- WEEKLY --------------------------------'''            
def hist_weekly(context, bar_dict):
    context.i1["price"].append(bar_dict[context.i1["id"]].close)
    context.i2["price"].append(bar_dict[context.i2["id"]].close)
    
def trade_by_weekly(context, bar_dict):
    if len(context.i1["price"]) > DELTA - 1:
        delta_i1 = bar_dict[context.i1["id"]].last / context.i1["price"][-DELTA]
        delta_i2 = bar_dict[context.i2["id"]].last / context.i2["price"][-DELTA]
        logger.info('2.[%s: %.2f], 8.[%s: %.2f]' % (context.i1['id'], (delta_i1 - 1), context.i2['id'], (delta_i2 - 1)))
        
        if delta_i1 > 1 and delta_i1 > delta_i2:
            # to buy i1
            trade_by_singal(context, bar_dict, context.i1["trade_ids"])
        elif delta_i2 > 1 and delta_i1 < delta_i2:
            # to buy i2
            trade_by_singal(context, bar_dict, context.i2["trade_ids"])
        else:
            trade_by_singal(context, bar_dict)
            

   
def trade_by_singal(context, bar_dict, signal=None):
    context.market_value = 0
    current_stocks = [k for k in context.portfolio.positions.keys() if context.portfolio.positions[k].quantity > 0]
    
    if signal == None:
        if len(current_stocks) > 0:
            if ICASH_ENABLED and context.icash in current_stocks:
                logger.info('hold icash %s' % context.icash)
            else:
                # clear positions
                logger.info('sold out and wait to enter again')
                dao_ot.clear_positions(context, bar_dict)
                
                if ICASH_ENABLED:
                    logger.info('buy in icash %s' % context.icash)
                    open_positions(context, bar_dict, [context.icash])
        else:
            if ICASH_ENABLED:
                logger.info('buy in icash %s' % context.icash)
                open_positions(context, bar_dict, [context.icash])
            else:
                logger.debug('do nothing')
            
    elif len(signal) > 0:
        if ICASH_ENABLED and context.icash in current_stocks:
            logger.info('sell icash [%s]' % context.icash)
            dao_ot.clear_positions(context, bar_dict)
            
        if len(current_stocks) == 0:
            # open position
            logger.info('buy in %s' % signal)
            open_positions(context, bar_dict, signal)
        else:
            if set(current_stocks).issubset(signal):
                # buy some those are not in position now: signal - current_stocks
                to_buy_list = list(set(signal) - set(current_stocks))
                if len(to_buy_list) == 0:
                    logger.info('hold %s' % current_stocks)
                else: 
                    logger.info('buy in %s' % to_buy_list)
                    open_positions(context, bar_dict, to_buy_list)
            else:
                # switch position
                logger.info('sell %s -> buy %s' % (current_stocks, signal))
                dao_ot.clear_positions(context, bar_dict)
                open_positions(context, bar_dict, signal)
        
        context.market_value = context.portfolio.portfolio_value
        
def open_positions(context, bar_dict, stocks):
    avg_weight = 0.99 / len(stocks)
    for s in stocks:
        dao_ot.place_an_order(context, bar_dict, s, SIDE.BUY, avg_weight)

