import math
from dao_strategy_base import *
from dao_stop_profit_or_loss_impl import *
from dao_adjust_condition_impl import *
from dao_query_impl import *
from dao_adjust_position_impl import *

# 创建一个规则执行器，并初始化一些通用事件
def create_rule(class_type, params, memo):
    obj = class_type(params)
    obj.on_open_position = open_position
    obj.on_close_position = close_position
    obj.on_clear_positions = clear_positions
    obj.memo = memo
    return obj

# 根据规则配置创建规则执行器
def create_rules(config):
    # config : list [0.规则是否启用，1.规则描述，2.规则实现类名，3.规则传递参数(dict)]
    return [create_rule(c[2], c[3], c[1]) for c in config if c[0]]


def open_position(sender, context, bar, weight):
    if weight > 0:
        order = place_an_order(bar, SIDE.BUY, weight, market_order_enabled = False)
        if order != None and order.filled_quantity > 0:
            for rule in context.rules_:
                rule.when_buy_stock(order)
        return True
    return False

def close_position(sender, context, bar, is_normal=True):
    order = place_an_order(bar, SIDE.SELL, 0, market_order_enabled = False)
    if order != None and order.filled_quantity > 0:
        for rule in context.rules_:
            rule.when_sell_stock(context.portfolio.positions[bar.order_book_id], order, is_normal)
        return True
    return False

def clear_positions(sender, context, bars):
    if context.portfolio.portfolio_value == context.portfolio.cash:
        return
    if context.portfolio.positions:
        sender.log_info("==> 清仓，卖出所有股票")
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            close_position(sender, context, bars[stock], False)
    for rule in context.rules_:
        rule.when_clear_positions()
        
def place_an_order(bar, side, weight, market_order_enabled = False):
    if market_order_enabled:
        return order_target_percent(bar.order_book_id, weight)
    else:
        return order_target_percent(bar.order_book_id, weight, style=LimitOrder(get_limit_price(bar, side)))

def get_limit_price(bar, side = SIDE.BUY):
    price = 0
    slipage = 0.005
    if side == SIDE.BUY:
        price = bar.last * (1 + slipage)
        if price > bar.limit_up:
            return bar.limit_up
        else:
            return price
    else:
        price = bar.last * (1 - slipage)
        if price < bar.limit_down:
            return bar.limit_down
        else:
            return price
    
# 显示策略组成
def log_rules(rules, keys):
    def get_rules_str(rule_list):
        if len(rule_list) > 0:
            return '\n'.join(['   %d.%s' % (i + 1, str(r)) for i, r in enumerate(rule_list)])
        return '    <无>'

    s = []
    s.append('\n---------------------策略一览：规则组合与参数---------------------')
    s.append('一、持仓股票处理规则:')
    s.append(get_rules_str(rules[keys[0]]))
    s.append('二、调仓条件规则:')
    s.append(get_rules_str(rules[keys[1]]))
    s.append('三、股票池选股规则:')
    s.append(get_rules_str(rules[keys[2]]))
    s.append('四、股票池过滤规则:')
    s.append(get_rules_str(rules[keys[3]]))
    s.append('五、调仓规则:')
    s.append(get_rules_str(rules[keys[4]]))
    s.append('六、其它规则:')
    s.append(get_rules_str(rules[keys[5]]))
    s.append('----------------------------------------------------------------')
    print('\n'.join(s))
    

def is_3_black_crows(stock):
    # talib.CDL3BLACKCROWS

    # 三只乌鸦说明来自百度百科
    # 1. 连续出现三根阴线，每天的收盘价均低于上一日的收盘
    # 2. 三根阴线前一天的市场趋势应该为上涨
    # 3. 三根阴线必须为长的黑色实体，且长度应该大致相等
    # 4. 收盘价接近每日的最低价位
    # 5. 每日的开盘价都在上根K线的实体部分之内；
    # 6. 第一根阴线的实体部分，最好低于上日的最高价位
    #
    # 算法
    # 有效三只乌鸦描述众说纷纭，这里放宽条件，只考虑1和2
    # 根据前4日数据判断
    # 3根阴线跌幅超过4.5%（此条件忽略）

    h = history_bars(stock, 4, '1d', ['close', 'open'])
    h_close = list(h['close'])
    h_open = list(h['open'])

    if len(h_close) < 4 or len(h_open) < 4:
        return False

    # 一阳三阴
    if h_close[-4] > h_open[-4] \
            and (h_close[-1] < h_open[-1] \
                and h_close[-2] < h_open[-2] \
                and h_close[-3] < h_open[-3]):
        return True
    return False


# 获取股票n日以来涨幅，根据当前价计算
def get_growth_rate(stock, n=20):
    cn = get_close_price(stock, n)
    c = history_bars(stock, 1, '1m', fields='close', skip_suspended=False, include_now=True)[0]

    if not(math.isnan(cn) or math.isnan(c) or cn == 0):
        return c / cn - 1.0
    else:
        logger.error("数据非法, stock: %s, %d日收盘价: %f, 当前价: %f" % (stock, n, cn, c))
        return 0


# 获取前n个单位时间当时的收盘价
def get_close_price(stock, n, unit='1d'):
    return history_bars(stock, n, unit, 'close')[0]

def get_last_day_turnover(stock):
    h = history_bars(stock, 1, '1d', 'total_turnover')
    if len(h) == 1:
        return h[0]
    return 0
    
def count_positions(positions):
    count = 0
    for k in positions.keys():
        if positions[k].quantity > 0:
            count += 1
    return count

def get_avg_order_weight(count):
    if count <= 0:
        return 0 
    else:
        return 0.99/count
    
def get_adjustment_interval(year):
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

def trunc(num, digits):
    if math.isnan(num):
        return num
        
    return math.floor(num * math.pow(10, digits)) / math.pow(10, digits)
