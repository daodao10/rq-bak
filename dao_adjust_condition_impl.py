import dao_strategy_util as dutil
from dao_strategy_base import *
import numpy as np
from scipy.stats import linregress

'''=============调仓控制器============='''


'''-------------------------调仓时间控制器-----------------------'''
class Time_condition(Adjust_condition):
    def __init__(self, params):
        # 配置调仓时间（24小时分钟制）
        self.hour = params.get('hour', 14)
        self.minute = params.get('minute', 50)
        
        self.t_can_adjust = False
        
    def update_params(self, context, params):
        self.hour = params.get('hour', self.hour)
        self.minute = params.get('minute', self.minute)

    @property
    def can_adjust(self):
        return self.t_can_adjust

    def before_trading_start(self, context):
        self.t_can_adjust = False
        
    def handle_bar(self, context, data):
        hour = context.now.hour
        minute = context.now.minute
        self.t_can_adjust = hour == self.hour and minute == self.minute

    def __str__(self):
        return '调仓时间控制器: [调仓时间: %d:%d]' % (self.hour, self.minute)


'''-------------------------调仓日计数器-----------------------'''
class Period_condition(Adjust_condition):
    def __init__(self, params):
        # 调仓日计数器，单位：日
        self.period = params.get('period', 3)
        
        self.day_counter = 0
        self.t_can_adjust = False

    def update_params(self, context, params):
        self.period = params.get('period', self.period)

    @property
    def can_adjust(self):
        return self.t_can_adjust

    def before_trading_start(self, context):
        self.t_can_adjust = False
        
    def handle_bar(self, context, bar_dict):
        self.log_info("调仓日计数 [%d]" % (self.day_counter))
        self.t_can_adjust = self.day_counter % self.period == 0
        self.day_counter += 1

    def when_sell_stock(self, position, order, is_normal):
        if not is_normal:
            # 个股止损止盈时，即非正常卖股时，重置计数
            self.day_counter = 0

    def when_clear_position(self):
        self.day_counter = 0

    def __str__(self):
        return '调仓日计数器: [调仓频率: %d 日] [调仓日计数: %d]' % (self.period, self.day_counter)


'''-------------------------指数线性回归止损----------------------'''
class Stop_loss_by_index_regress(Adjust_condition):
    def __init__(self, params):
        self.index = params.get('index', '000001.XSHG')
        self.period = params.get('period', 130)
        #self.offset = params.get('offset', 0.35)
        
        self.is_day_stop_loss_fired = False

    def update_params(self, context, params):
        self.index = params.get('index', self.index)
        self.period = params.get('period', self.period)
        #self.offset = params.get('offset', self.offset)
    
    @property
    def can_adjust(self):
        return not self.is_day_stop_loss_fired
    
    def before_trading_start(self, context):
        self.is_day_stop_loss_fired = False
        
    def handle_bar(self, context, bar_dict):
        if not self.is_day_stop_loss_fired:
            log2close = np.log2(history_bars(self.index, self.period, '1d', 'close'))
            regress_struct = linregress(range(len(log2close)), log2close)
            
            if regress_struct.slope < 0:
                self.is_day_stop_loss_fired = True
                self.log_info("==> %s 指数 %d 日线性回归斜率为负, 执行止损" % (instruments(self.index).symbol, self.period))
                self.clear_positions(context, bar_dict)

    def __str__(self):
        return '指数线性回归止损: [指数: %s %s] [判定调仓: %s 日指数线性回归斜率为负]' % (
            self.index, instruments(self.index).symbol, self.period)
    
    
''' ----------------------指数回撤止损------------------------------'''
class Stop_loss_by_index_price(Adjust_condition):
    def __init__(self, params):
        self.index = params.get('index', '000001.XSHG')
        self.period = params.get('period', 90)
        self.drawdown_min = params.get('drawdown_min', 0.30)
        self.drawdown_max = params.get('drawdown_max', 0.55)
        
        self.is_day_stop_loss_fired = False

    def update_params(self, context, params):
        self.index = params.get('index', self.index)
        self.period = params.get('period', self.period)
        self.drawdown_min = params.get('drawdown_min', self.drawdown_min)
        self.drawdown_max = params.get('drawdown_max', self.drawdown_max)
    
    @property
    def can_adjust(self):
        return not self.is_day_stop_loss_fired
    
    def before_trading_start(self, context):
        self.is_day_stop_loss_fired = False
    
    def handle_bar(self, context, bar_dict):
        if not self.is_day_stop_loss_fired:
            h = history_bars(self.index, self.period, str(self.period) + 'd', ['high'])
            
            last_price = current_snapshot(self.index).last
            high_price = h['high'].max()
            drawdown = 1 - last_price / high_price
            
            if drawdown > self.drawdown_min and drawdown < self.drawdown_max :
                self.is_day_stop_loss_fired = True
                self.log_info("==> %s 指数 %d 日回撤 %.2f%% [HIGH: %f, LAST: %f], 执行止损" % (
                instruments(self.index).symbol, self.period, drawdown * 100, high_price, last_price))
                self.clear_positions(context, bar_dict)

    def __str__(self):
        return '指数回撤止损: [指数: %s %s] [判定调仓: %s 日回撤 %.2f%% ~ %.2f%%]' % (
            self.index, instruments(self.index).symbol, self.period, self.drawdown_min * 100, self.drawdown_max * 100)


''' ----------------------指数三乌鸦止损------------------------------'''
class Stop_loss_by_3_black_crows(Adjust_condition):
    def __init__(self, params):
        self.index = params.get('index', '000001.XSHG')
        self.minute_counter_index_drop = params.get('minute_counter_index_drop', 60)

        self.minute_counter = 0
        self.is_day_stop_loss_fired = False
        self.is_last_day_3_black_crows = False

    def update_params(self, context, params):
        self.index = params.get('index', self.index)
        self.minute_counter_index_drop = params.get('minute_counter_index_drop', self.minute_counter_index_drop)

    @property
    def can_adjust(self):
        return not self.is_day_stop_loss_fired

    def before_trading_start(self, context):
        self.minute_counter = 0
        self.is_day_stop_loss_fired = False
        self.is_last_day_3_black_crows = dutil.is_3_black_crows(self.index)
        if self.is_last_day_3_black_crows:
            self.log_info("==> %s指数前4日已经构成三黑鸦形态" % (instruments(self.index).symbol))
        
    def handle_bar(self, context, bar_dict):
        # 前日三黑鸦，累计当日每分钟涨幅<0的分钟计数
        # 如果分钟计数超过一定值，则开始进行三黑鸦止损
        # 避免无效三黑鸦乱止损
        if self.is_day_stop_loss_fired:
            return
        
        if self.is_last_day_3_black_crows:
            if dutil.get_growth_rate(self.index, 1) < 0:
                self.minute_counter += 1

            if self.minute_counter >= self.minute_counter_index_drop:
                self.is_day_stop_loss_fired = True
                if self.minute_counter == self.minute_counter_index_drop:
                    self.log_info("==> 已持续 %d 分钟, 执行止损" % (self.minute_counter))

                self.clear_positions(context, bar_dict)
        else:
            self.is_day_stop_loss_fired = False

    def __str__(self):
        return '指数三乌鸦止损: [指数: %s %s] [判定调仓: 连续跌 %d 分钟] [当前状态: %s]' % (
            self.index, instruments(self.index).symbol, self.minute_counter_index_drop, self.is_last_day_3_black_crows)


''' ----------------------28指数择时/止损------------------------------'''
class Stop_loss_by_28_index(Adjust_condition):
    def __init__(self, params):
        self.index2 = params.get('index2', '000300.XSHG')
        self.index8 = params.get('index8', '000905.XSHG')
        self.period = params.get('period', 21)
        self.index_growth_rate = params.get('index_growth_rate', 0.01)
        self.minute_counter_28index_drop = params.get('minute_counter_28index_drop', 60)

        self.is_day_stop_loss_fired = False
        self.minute_counter = 0

    def update_params(self, context, params):
        self.index2 = params.get('index2', self.index2)
        self.index8 = params.get('index8', self.index8)
        self.period = params.get('period', self.period)
        self.index_growth_rate = params.get('index_growth_rate', self.index_growth_rate)
        self.minute_counter_28index_drop = params.get('minute_counter_28index_drop', self.minute_counter_28index_drop)

    @property
    def can_adjust(self):
        return not self.is_day_stop_loss_fired

    def before_trading_start(self, context):
        self.minute_counter = 0
        self.is_day_stop_loss_fired = False
        
    def handle_bar(self, context, bar_dict):
        gr_index2 = dutil.get_growth_rate(self.index2, self.period)
        gr_index8 = dutil.get_growth_rate(self.index8, self.period)
    
        if gr_index2 <= self.index_growth_rate and gr_index8 <= self.index_growth_rate:
            if (self.minute_counter == 0):
                self.log_info("28指数的 %d 日涨幅同时低于[%.2f%%], %s指数: [%.2f%%], %s指数: [%.2f%%]" % (
                    self.period,
                    self.index_growth_rate * 100,
                    instruments(self.index2).symbol, gr_index2 * 100,
                    instruments(self.index8).symbol, gr_index8 * 100))

            self.minute_counter += 1
        else:
            # 不连续状态归零
            if self.minute_counter < self.minute_counter_28index_drop:
                self.minute_counter = 0

        if self.minute_counter >= self.minute_counter_28index_drop:
            self.is_day_stop_loss_fired = True
            if self.minute_counter == self.minute_counter_28index_drop:
                self.log_info("==> 当日已持续 %d 分钟, 执行止损" % (self.minute_counter))

            self.clear_positions(context, bar_dict)
        else:
            self.is_day_stop_loss_fired = False

    def __str__(self):
        return '28指数择时/止损: [大盘指数: %s %s] [小盘指数: %s %s] [判定调仓: 连续 %d 分钟 %d 日涨幅低于 %.2f%%]' % (
            self.index2, instruments(self.index2).symbol,
            self.index8, instruments(self.index8).symbol,
            self.minute_counter_28index_drop,
            self.period,
            self.index_growth_rate * 100)

