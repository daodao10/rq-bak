import numpy as np
from pandas import Series
from dao_strategy_base import *
import dao_strategy_util as dutil

class Stop_profit_or_loss(Rule):
    def __init__(self, params):
        self.auto_threshold = params.get('auto_threshold', False)
        if self.auto_threshold:
            self.period = params.get('period', 3)
        else:
            self.threshold = params.get('threshold', 0)

        self.ignore = {}
        self.pct_change = {}

    def update_params(self, context, params):   
        self.auto_threshold = params.get('auto_threshold', self.auto_threshold)
        if self.auto_threshold:
            self.period = params.get('period', self.period)
        else:
            self.threshold = params.get('threshold', self.threshold)
        
    # 获取个股前n天的m日涨跌幅值序列
    # 增加缓存避免当日多次获取数据
    def _get_pct_change(self, security, n, m):
        pct_change = None
        if security in self.pct_change.keys():
            pct_change = self.pct_change[security]
        else:
            h = history_bars(security, n, '1d', 'close')
            pct_change = Series(h[0]).pct_change(m)  # m日的百分比变比（即m日涨跌幅）
            self.pct_change[security] = pct_change
        return pct_change
    
    # 计算个股止盈阈值
    # 算法：个股一年(250天)内最大的n日涨幅
    # 返回正值
    def _get_stop_profit_threshold(self, security):
        if self.auto_threshold:
            n = self.period
            pct_change = self._get_pct_change(security, 250, n)
            max_pct = pct_change.max()

            # 默认配置止盈阈值最大涨幅为(10% * n)
            if np.isnan(max_pct) or max_pct == 0:
                return 0.10 * n
            return abs(max_pct)
        else:
            return self.threshold

    # 计算个股回撤止损阈值, 即个股在持仓n天内能承受的最大跌幅
    # 算法：(个股250天内最大的n日跌幅 + 个股250天内平均的n日跌幅)/2
    # 返回正值
    def _get_stop_loss_threshold(self, security):
        if self.auto_threshold:
            n = self.period
            pct_change = self._get_pct_change(security, 250, n)
            max_pct = pct_change.min() 
            avg_pct = pct_change.mean()
            # max_pct 和 avg_pct 可能为正，表示这段时间内一直在增长，比如新股
            final_pct = (max_pct + avg_pct) / 2

            if not np.isnan(final_pct):
                if final_pct != 0:
                    return abs(final_pct)
                else:
                    if max_pct < 0:
                        # 此时取最大跌幅
                        return abs(max_pct)

            # 默认配置回测止损阈值最大跌幅为(-3.3% * n)
            return 0.033 * n
        else:
            return self.threshold

    def before_trading_start(self, context):
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.quantity > 0:
                if is_suspended(stock):
                    self.ignore[stock] = True
                else:
                    self.ignore[stock] = False

    def when_buy_stock(self, order):
        self.ignore[order.order_book_id] = True
        
    def after_trading_end(self, context):
        self.pct_change = {}
        
'''---------------个股止损--------------'''
class Stop_loss_stocks(Stop_profit_or_loss):        
    def handle_bar(self, context, bar_dict):
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.quantity > 0:
                if self.ignore[stock]:
                    continue
                
                cur_price = bar_dict[stock].last
                avg_price = position.avg_price
                threshold = self._get_stop_loss_threshold(stock)
                #self.log_debug("个股止损阈值, stock: %s, threshold: %f" %(stock, threshold))
                if cur_price < avg_price * (1 - threshold):
                    self.ignore[stock] = True
                    self.log_info("==> stock: %s, cur_price: %f, avg_cost: %f, threshold: %f"
                                  % (stock, cur_price, avg_price, threshold))
                    context.filter_list.append(stock)
                    self.close_position(context, bar_dict[stock], False)

    def __str__(self):
        return '个股止损:'


''' ----------------------个股止盈------------------------------'''
class Stop_profit_stocks(Stop_profit_or_loss):
    def handle_bar(self, context, bar_dict):
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.quantity > 0:
                if self.ignore[stock]:
                    continue

                cur_price = bar_dict[stock].last
                avg_price = position.avg_price
                threshold = self._get_stop_profit_threshold(stock)
                #logger.debug("个股止盈阈值, stock: %s, threshold: %f" %(stock, threshold))
                if cur_price > avg_price * (1 + threshold):
                    self.ignore[stock] = True
                    self.log_info("==> stock: %s, cur_price: %f, avg_cost: %f, threshold: %f"
                                  % (stock, cur_price, avg_price, threshold))
                    context.filter_list.append(stock)
                    self.close_position(context, bar_dict[stock], False)
                
    def __str__(self):
        return '个股止盈:'

''' ----------------------个股回撤退出策略------------------------------'''
class Stop_loss_profit_by_drawdown(Stop_profit_or_loss):
    def __init__(self, params):
        self.drawdown_max = params.get('drawdown_max', 0)
        
        self.ignore = {}
        self.last_price = {}

    def update_params(self, context, params):
        self.drawdown_max = params.get('drawdown_max', self.drawdown_max)
    
    def before_trading_start(self, context):
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.quantity > 0:
                if is_suspended(stock):
                    self.ignore[stock] = True
                else:
                    self.ignore[stock] = False
            else:
                self.last_price.pop(stock)
                self.ignore.pop(stock)

    def when_buy_stock(self, order):
        Stop_profit_or_loss.when_buy_stock(self, order)
        self.last_price[order.order_book_id] = order.avg_price
    
    def after_trading_end(self, context):
        pass
        
    def handle_bar(self, context, bar_dict):        
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.quantity > 0:

                if (stock not in self.ignore) or is_suspended(stock):
                    self.ignore[stock] = True
                    self.last_price[stock] = position.avg_price
                    
                if self.ignore[stock]:
                    continue
 
                bar = bar_dict[stock]
                
                cur_price = bar.close
                if cur_price > self.last_price[stock]:
                    self.last_price[stock] = cur_price
                
                high_price = self.last_price[stock]
                if cur_price < high_price * (1 - self.drawdown_max):
                    self.ignore[stock] = True
                    self.log_info("==> stock: %s, cur_price: %f, high: %f, drawdown: %f"
                                    % (stock, cur_price, high_price, self.drawdown_max))
                    context.filter_list.append(stock)
                    self.close_position(context, bar, False)

    def __str__(self):
        return '个股回撤退出:'
