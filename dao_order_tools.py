import math

from scipy.stats import linregress

import dao_util as dao_u


def rebalance(context, bar_dict):
    logger.info('stocks listed: {0}', context.stocks)       
    
    to_sell = []
    to_buy = []
    count = 0
    
    to_adjust = []
    
    temp_list = context.stocks[0:context.max_no_of_trade]
    for s in context.portfolio.positions:
        if context.portfolio.positions[s].quantity == 0:
                continue
        if s not in temp_list:
            if not(is_suspended(s)) and is_effective_order(bar_dict[s], SIDE.SELL):
                to_sell.append(s)
            else:
                count += 1
                logger.debug('{0} cannot sell [suspended|limit_down]', s)
        else:
                count += 1
                to_adjust.append(s)

    for s in context.stocks:
        if count >= context.max_no_of_trade:
                break
        if s not in to_adjust:
            if is_effective_order(bar_dict[s], SIDE.BUY):
                to_buy.append(s)
                count += 1
            else:
                logger.debug('{0} cannot buy [suspended|limit_up]', s)
        
    to_buy.extend(to_adjust)
    
    # order
    for s in to_sell:
        place_an_order(context, bar_dict, s, SIDE.SELL, 0)
    if len(to_buy) > 0:
        # logger.info('buy stocks listed: {0}', to_buy)
        weight = update_weight(count, context.total_weight if hasattr(context,'total_weight') and context.total_weight > 0 else 0.99)
        # logger.info(weight)
        for s in to_buy:
            place_an_order(context, bar_dict, s, SIDE.BUY, weight)
    
    context.market_value = context.portfolio.market_value

def cut_loss(context, bar_dict):
    if not context.cut_loss_fired and context.market_value > 0 and len(context.portfolio.positions) > 0:
        if context.portfolio.market_value / context.market_value < context.cut_loss_trigger:
            logger.debug('PL: [{0}]', context.portfolio.market_value / context.market_value)
            logger.debug('cut loss: {0}', list(context.portfolio.positions))
            clear_positions(context, bar_dict)
            context.cut_loss_fired = True
            context.market_value = context.portfolio.market_value

def get_direction_by_regress(context, regress_reference, regress_days):
    regress_df = linregress(range(regress_days), history_bars(regress_reference, regress_days, '1d', 'close'))
    # logger.debug(regress_df)
    
    # 1)
    if regress_df.slope > 0:
        logger.debug('BUY IN')
        return SIDE.BUY
    else:
        logger.debug('SELL OUT')
        return SIDE.SELL
    # # 2)
    # if regress_df[0]/regress_df[1]<0:
    #   return SIDE.SELL
    # else:
    #     return SIDE.BUY
    
def clear_positions(context, bar_dict):
    for s in context.portfolio.positions:
        place_an_order(context, bar_dict, s, SIDE.SELL, 0)

def place_an_order(context, bar_dict, order_book_id, side, weight):
    if bar_dict[order_book_id].close > 0:
        if context.market_order_enalbed:
            order_target_percent(order_book_id, weight)
            pass
        else:
            order_target_percent(order_book_id, weight, style=LimitOrder(get_limit_price(bar_dict[order_book_id], side)))
    else:
        logger.error('stock[%s] is not in trading' % order_book_id)

def is_effective_order(bar, side = SIDE.BUY):
    if side == SIDE.BUY:
        return bar.last < dao_u.trunc(bar.limit_up, 2)
    else:
        return bar.last > dao_u.trunc(bar.limit_down, 2)
    
def get_limit_price(bar, side = SIDE.BUY):
    price = 0
    if side == SIDE.BUY:
        price = bar.last * 1.005
        if price > bar.limit_up:
            return bar.limit_up
        else:
            return price
    else:
        price = bar.last * 0.995
        if price < bar.limit_down:
            return bar.limit_down
        else:
            return price

def filter_delisted(stocks, bar_dict):
    temp_list = []
    for s in stocks:
        if not is_delisted(bar_dict[s]):
            temp_list.append(s)
    return temp_list
    
# this is patch for delisted stocks
def is_delisted(bar):
    return bar.symbol.startswith('退') or bar.symbol.endswith('退') or bar.symbol.startswith('*')

def update_weight(total_num, total_weight = 0.99):
    if total_num <= 0:
        return 0 
    else:
        return total_weight/total_num

