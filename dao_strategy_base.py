import dao_strategy_util as dutil
DEBUG = True
WARNING = False

'''==============================所有规则的基类=============================='''    
class Rule(object):
    # 持仓操作的事件
    on_open_position = None  # 买股调用外部函数
    on_close_position = None  # 卖股调用外部函数
    on_clear_positions = None  # 清仓调用外部函数
    memo = ''  # 对象简要说明

    def __init__(self, params):
        pass

    def initialize(self, context):
        pass

    def handle_bar(self, context, bar_dict):
        pass

    def before_trading_start(self, context):
        pass

    def after_trading_end(self, context):
        pass

    # 买入股票时调用的函数
    # price为当前价，amount为发生的股票数
    def when_buy_stock(self, order):
        pass
    
    # 卖出股票时调用的函数
    # price为当前价，amount为发生的股票数,is_normail正常规则卖出为True，止损卖出为False
    def when_sell_stock(self, position, order, is_normal):
        pass

    # 清仓时调用的函数
    def when_clear_positions(self):
        pass

    # 调仓前调用
    def before_adjust_start(self, context, bar_dict):
        pass

    # 调仓后调用用
    def after_adjust_end(slef, context, bar_dict):
        pass

    # 更改参数
    def update_params(self, context, params):
        pass

    # 持仓操作事件的简单判断处理，方便使用。
    #def open_position(self, context, security, value):
    def open_position(self, context, bar, weight):
        if self.on_open_position != None:
            #return self.on_open_position(self, context, security, value)
            return self.on_open_position(self, context, bar, weight)

    #def close_position(self, context, position, is_normal=True):
    def close_position(self, context, bar, is_normal=True):
        if self.on_close_position != None:
            #return self.on_close_position(self, context, position, is_normal=True)
            return self.on_close_position(self, context, bar, is_normal=is_normal)

    #def clear_positions(self, context):
    def clear_positions(self, context, bars):
        if self.on_clear_positions != None:
            self.on_clear_positions(self, context, bars)

    # 为日志显示带上是哪个规则器输出的
    def log_info(self, msg):
        logger.info('%s: %s' % (self.memo, msg))

    def log_warn(self, msg):
        if WARNING:
            logger.warn('%s: %s' % (self.memo, msg))

    def log_debug(self, msg):
        if DEBUG:
            logger.debug('%s: %s' % (self.memo, msg))


'''==============================调仓条件基类=============================='''    
class Adjust_condition(Rule):
    # 返回能否进行调仓
    @property
    def can_adjust(self):
        return True

'''==============================选股 Query 基类=============================='''
class Query_stock_list(Rule):
    def select(self, context, bar_dict, q):
        return None

'''==============================选股 Filter 基类=============================='''
class Filter_stock_list(Rule):
    def filter(self, context, bar_dict, stock_list):
        return None

'''==============================调仓操作基类=============================='''
class Adjust_position(Rule):
    def adjust(self, context, bar_dict, buy_stocks):
        pass

    
'''==============================统计模块类=============================='''
class Stats(Rule):
    def __init__(self, params):
        # 加载统计模块
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.stats = {'win': [], 'loss': []}

    def after_trading_end(self, context):
        self.report(context)

    def when_sell_stock(self, position, order, is_normal):
        if order.filled_quantity > 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            self.watch(position.order_book_id, order.filled_quantity, position.avg_price, order.avg_price)

    def reset(self):
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.stats = {'win': [], 'loss': []}

    # 卖出成功后,针对卖出的量进行盈亏统计: 记录交易次数, 盈利次数
    def watch(self, stock, quantity, avg_cost, cur_price):
        self.trade_total_count += 1
        #current_value = quantity * cur_price
        #cost = quantity * avg_cost
        percent = round((cur_price / avg_cost - 1) * 100, 2)
        if cur_price > avg_cost:
            self.trade_success_count += 1
            win = [stock, percent]
            self.stats['win'].append(win)
        else:
            loss = [stock, percent]
            self.stats['loss'].append(loss)

    def report(self, context):
        cash = context.portfolio.cash
        total_value = context.portfolio.portfolio_value
        position = 1 - cash / total_value
        self.log_info("收盘后持仓:%s" % str(list(context.portfolio.positions)))
        self.log_info("仓位:%.2f" % position)
        self.print_win_rate(context)

    # 打印胜率
    def print_win_rate(self, context):
        win_rate = 0
        if self.trade_total_count > 0 and self.trade_success_count > 0:
            win_rate = round(self.trade_success_count / float(self.trade_total_count), 3)

        most_win = self.stats_most_win_percent()
        most_loss = self.stats_most_loss_percent()
        
        if len(most_win) == 0 or len(most_loss) == 0:
            return
        starting_cash = context.portfolio.starting_cash
        total_value = context.portfolio.portfolio_value
        total_profit = total_value - starting_cash

        s = []
        s.append('------------绩效报表------------')
        s.append('交易次数: {0}, 盈利次数: {1}, 胜率: {2}%'.format(self.trade_total_count, self.trade_success_count, dutil.trunc(win_rate * 100, 2)))
        s.append('单次盈利最高: {0}, 盈利比例: {1}%'.format(most_win['stock'], dutil.trunc(most_win['value'],2)))
        s.append('单次亏损最高: {0}, 亏损比例: {1}%'.format(most_loss['stock'], dutil.trunc(most_loss['value'],2)))
        s.append('总资产: %.2f, 本金: %.2f, 盈利: %.2f, 盈亏比率：%.2f%%' % (total_value, starting_cash, total_profit, total_profit / starting_cash * 100))
        #s.append('--------------------------------')
        self.log_info('\n'.join(s))

    # 统计单次盈利最高的股票
    def stats_most_win_percent(self):
        result = {}
        for stats in self.stats['win']:
            if {} == result:
                result['stock'] = stats[0]
                result['value'] = stats[1]
            else:
                if stats[1] > result['value']:
                    result['stock'] = stats[0]
                    result['value'] = stats[1]

        return result

    # 统计单次亏损最高的股票
    def stats_most_loss_percent(self):
        result = {}
        for stats in self.stats['loss']:
            if {} == result:
                result['stock'] = stats[0]
                result['value'] = stats[1]
            else:
                if stats[1] < result['value']:
                    result['stock'] = stats[0]
                    result['value'] = stats[1]

        return result

    def __str__(self):
        return '策略绩效统计'
