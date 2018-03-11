'''=============调仓制器============='''
from dao_strategy_base import *
import dao_strategy_util as dutil

'''---------------再平衡规则--------------'''
class Rebalance(Adjust_position):
    def __init__(self, params):
        self.max_position_count = params.get('max_position_count', 3)
        self.max_weight_per_position = params.get('max_weight_per_position', 0.5)
        self.unlimited_selling = params.get('unlimited_selling', False)
        self.rebalance_enabled = params.get('rebalance_enabled', True)
        
    def update_params(self, context, params):
        self.max_position_count = params.get('max_position_count', self.max_position_count)
        self.max_weight_per_position = params.get('max_weight_per_position', self.max_weight_per_position)
        self.candidates_enabled = params.get('candidates_enabled', self.candidates_enabled)
        self.rebalance_enabled = params.get('rebalance_enabled', self.rebalance_enabled)
        
    def adjust(self, context, bar_dict, buy_stocks):
        to_sell = []
        to_buy = []      
        count = 0
        
        to_adjust = []
        
        temp_list = buy_stocks if self.unlimited_selling else buy_stocks[0:self.max_position_count]
        for s in context.portfolio.positions:
            if context.portfolio.positions[s].quantity == 0:
                continue
            if s not in temp_list:
                if not(is_suspended(s)) and self.is_effective_order(bar_dict[s], SIDE.SELL):
                    to_sell.append(s)
                else:
                    count += 1
                    logger.debug('{0} cannot sell [suspended|limit_down]', s)
            else:
                count += 1
                to_adjust.append(s)

        for s in buy_stocks:
            if count >= self.max_position_count:
                break
            if s not in to_adjust:
                if self.is_effective_order(bar_dict[s], SIDE.BUY):
                    to_buy.append(s)
                    count += 1
                else:
                    logger.debug('{0} cannot buy [suspended|limit_up]', s)

        if self.rebalance_enabled:
            to_buy.extend(to_adjust)
        
        # place order
        for s in to_sell:
            self.close_position(context, bar_dict[s])
        if len(to_buy) > 0:
            # logger.info('buy stocks listed: {0}', to_buy)
            weight = dutil.get_avg_order_weight(count)
            weight = weight if weight <= self.max_weight_per_position else self.max_weight_per_position
            #logger.info('weight: [%.2f%%]' % weight)
            for s in to_buy:
                self.open_position(context, bar_dict[s], weight)
    
    def is_effective_order(self, bar, side = SIDE.BUY):
        if side == SIDE.BUY:
            return bar.last < dutil.trunc(bar.limit_up, 2)
        else:
            return bar.last > dutil.trunc(bar.limit_down, 2)
    
    def __str__(self):
        return '再平衡调仓法: 卖出不在<股票池>的股票, 平均%%买入<股票池>里的股票, [持仓股票数目: %d], [单股票仓位 <= %.1f%%], [非限制性卖出: %s], [再平衡: %s]' % (self.max_position_count, self.max_weight_per_position * 100, self.unlimited_selling, self.rebalance_enabled)
        

'''---------------卖出股票规则--------------'''
class Sell_stocks(Adjust_position):
    def adjust(self, context, bar_dict, buy_stocks):
        # 卖出不在待买股票列表中的股票
        # 对于因停牌等原因没有卖出的股票则继续持有
        for stock in context.portfolio.positions.keys():
            if stock not in buy_stocks:
                self.log_debug("stock [%s] won't in buy_stocks" % (stock))
                position = context.portfolio.positions[stock]
                self.close_position(context, bar_dict[stock])
            else:
                self.log_debug("stock [%s] is still in new position" % (stock))

    def __str__(self):
        return '股票调仓规则: 卖出不在<股票池>的股票'


'''---------------买入股票规则--------------'''
class Buy_stocks(Adjust_position):
    def __init__(self, params):
        self.max_position_count = params.get('max_position_count', 3)

    def update_params(self, context, params):
        self.max_position_count = params.get('max_position_count', self.max_position_count)

    def adjust(self, context, bar_dict, buy_stocks):
        # 买入股票: 始终保持持仓数目为 max_position_count
        # 根据股票数量分仓,可用金额平均分配购买,不能保证每个仓位平均分配
        position_count = dutil.count_positions(context.portfolio.positions)
        if self.max_position_count > position_count:
            weight = dutil.get_avg_order_weight(self.max_position_count)
            for stock in buy_stocks:
                if self.open_position(context, bar_dict[stock], weight):
                    position_count += 1
                    if position_count == self.max_position_count:
                        break

    def __str__(self):
        return '股票调仓买入规则: 现金平分式买入<股票池>的股票'
        