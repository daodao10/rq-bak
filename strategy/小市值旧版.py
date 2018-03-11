import datetime
import dao_order_tools as dao_ot


def init(context):
    context.max_no_of_trade = 3
    context.no_of_candidates = 2
    
    context.market_order_enalbed = False
    
    context.market_value = 0
    context.cut_loss_enalbed = False
    context.cut_loss_trigger = 0.9
    
    context.next_running_days = 1
    
    scheduler.run_daily(trade, time_rule = market_open(minute = 3))

def before_trading(context):
    context.cut_loss_fired = False
    
    df = get_fundamentals(
        query(fundamentals.eod_derivative_indicator.market_cap)
        .order_by(fundamentals.eod_derivative_indicator.market_cap.asc())
        .limit(context.max_no_of_trade * 10))
    
    stocks = list(df.columns.values)
    
    context.stocks = []
    for c in stocks:
        his = history_bars(c, 1, '1d', 'total_turnover')
        if len(his) == 1 and his[0] > 10000000 and not(is_suspended(c) or is_st_stock(c)):
            if len(context.stocks) < context.max_no_of_trade + context.no_of_candidates:
                context.stocks.append(c)
            else:    
                break

def handle_bar(context, bar_dict):
    if context.cut_loss_enalbed:
        dao_ot.cut_loss(context, bar_dict)
    else:
        pass

def after_trading(context):
    pass

def trade(context, bar_dict):
    context.next_running_days -= 1
    if context.next_running_days > 0:
        return

    # special process: remove delisted
    context.stocks = dao_ot.filter_delisted(context.stocks, bar_dict)
    
    dao_ot.rebalance(context, bar_dict)

    context.next_running_days = get_interval(context)

def get_interval(context):
    year = context.now.strftime('%Y')
    # refer to CSI300 monthly MA30
    dict = {
        '2006':11,
        '2007':11,
        '2008':11,
        '2009':9,
        '2010':11,#stand up MA
        '2011':11,
        '2012':9,
        '2013':9,
        '2014':9,
        '2015':11,
        '2016':11,
        '2017':9
    }
    return dict[year]
