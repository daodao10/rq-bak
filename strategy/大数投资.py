import datetime
import pandas as pd
import numpy as np

import operator
import math

# import dao_order_tools as dao_ot
import dao_big_data_strategy as dao_bds
import dao_stock_pbpe_util as dao_spbpe

''' 策略限制：
 大盘市净率小于1.95
 最多30个行业
 最多40只个股
 每行业最多2只个股
 每行业最大仓位3%
'''
GOOD_MARKET_PB = 1.95
MIN_RANK = 7
MAX_INDUSTRY_NO = 30
MAX_STOCK_NO = 40
MAX_STOCK_NO_PER_IND = 2
MAX_POSTION_PER_IND = 3

DEBUG_BY_DAO = False

def class MarketInfo:
    def __init_(self):
        self._pb = 0
        self._close = 3000
        self._total_position = 0
        self._allowed_position = 0
        self._pct = 0
    
    @property
    def PB(self, val):
        return self.pb
    @PB.setter
    def PB(self, val):
        self._pb = val
    
    @property
    def Close(self):
        return self._close
    @Close.setter
    def Close(self, val):
        self._close = val
    
    @property
    def TotalPosition(self):
        return self._total_position
    @TotalPosition.setter
    def TotalPosition(self, val):
        self._total_position = val
    
    @property
    def AllowedPosition(self):
        return self._allowed_position
    @AllowedPosition.setter
    def AllowedPosition(self, val):
        self._allowed_position = val
    
    @property
    def Pct(self):
        return self._pct
    @Pct.setter
    def Pct(self, val):
        self._pct = val

def init(context):

    # context.cash_per_pect = context.run_info.stock_starting_cash / 100
    context.cash_per_pect = 3000
    
    # context.max_allowed_position = 0
    context.current_industry_no = 0
    context.current_stock_no = 0
    # context.current_position = 0
    context.market = MarketInfo()
    # {"pb":2, "close": 3000, "total_position": 0, "allowed_position": 0, "pct": 0}
    
    # stocks_in_industry
    # {'order_book_id_1': [prob, pb, pe], 'order_book_id_2': [prob, pb, pe], 'total_position': number }
    # context.stocks_in_industry = {}
    # context.selected_stocks = []
    context.position_stock = {}
    context.position_ind = {}
    
    scheduler.run_daily(buy_trade, time_rule=market_open(minute=13))
    scheduler.run_daily(sell_trade, time_rule=market_close(minute=41))
    
    context.market_pb_df = get_csv('data/market_pb.csv', index_col='date')
    
def before_trading(context):
    logger.info('-------------------------')
    
    market = context.market
    
    context.selected_stocks = []
    
    pb = get_market_pb(context.market_pb_df, context.now)
    # pb = dao_bds.get_market_pb(context.now)
    # dao_bds.get_market_pb(ignore_bank = True)
    market.AllowedPosition = dao_bds.calc_X_by_pb(pb, False) * 100
    logger.info('(%.3f, %.2f, %.2f)' % (pb, market.AllowedPosition, market.TotalPosition))
    
    # 总仓位超额？
    if pb < GOOD_MARKET_PB and market.TotalPosition < market.AllowedPosition:
            
        x_list = get_stocks(context)
    
        if len(x_list) > 0:
            if len(x_list) > MAX_INDUSTRY_NO:
                x_list = x_list[0:MAX_INDUSTRY_NO]
            
            outstanding_position = 0
            remaining_industry_no = MAX_INDUSTRY_NO - market.current_industry_no
            remaining_stock_no = MAX_STOCK_NO - market.current_stock_no
            
            # if DEBUG_BY_DAO:
            print('remaining_industry_no:%d, remaining_stock_no:%d' % (remaining_industry_no, remaining_stock_no))
                
            for x in x_list:
                stocks_in_ind = position_stock[position_stock['ind'] == x['ind']]
                stocks_df = x['df']
                if not(stocks_in_ind.empty):
                    # 已有仓位的行业：此处需要判断
                    # 行业内仓位超额？
                    total_position_ind = stocks_in_ind['position'].sum()
                    if total_position_ind >= MAX_POSTION_PER_IND:
                        logger.info('max position already in [%s]' % x['ind_name'])
                        continue
                    
                    remaining_stock_no_ind = MAX_STOCK_NO_PER_IND - stocks_in_ind.index.size
                    for s in list(stocks_df.index):
                        if s in stocks_in_ind.index
                            # 调仓
                            # 仅需要考虑仓位：行业股票仓位 和 总仓位
                            
                            lots, add_position = calc_position(outstanding_position, total_position_ind, s, stocks_in_ind[s][0], stocks_df, context)
                            if lots > 0:
                                outstanding_position += add_position
                                total_position_ind += add_position
                                
                                context.selected_stocks.append((x['ind'], s, add_position, stocks_df.ix[s, 'pb_ratio'], stocks_df.ix[s, 'pe_ratio'], lots))
                        else:
                            # 行业内开仓
                            # 要考虑
                            # 1, 股票数目：总股票数目 及 行业内股票数目
                            # 2, 仓位：行业股票仓位 和 总仓位
                            
                            if remaining_stock_no_ind - 1 < 0 or remaining_stock_no - 1 < 0:
                                # exceed number of stocks
                                break
                           
                            lots, add_position = calc_position(outstanding_position, total_position_ind, s, 0, stocks_df, context)
                        
                            if lots > 0:
                                outstanding_position += add_position
                                total_position_ind += add_position
                                
                                remaining_stock_no -= 1
                                remaining_stock_no_ind -= 1
                                
                                context.selected_stocks.append((x['ind'], s, add_position, stocks_df.ix[s, 'pb_ratio'], stocks_df.ix[s, 'pe_ratio'], lots))
                                # hard code first
                                # context.current_stock_no += 1
                else:
                    # 新行业开仓
                    # 需要考虑：
                    # 1, 行业数目
                    # 2, 股票数目：总股票数目 及 行业内股票数目
                    # 3, 仓位：行业股票仓位 和 总仓位
                    
                    if remaining_industry_no - 1 < 0:
                        # exceed number of industry
                        continue
                    
                    position_changed = False
                    total_position_ind = 0
                    remaining_stock_no_ind = MAX_STOCK_NO_PER_IND
                    for s in list(stocks_df.index):
                        if remaining_stock_no - 1 < 0 or remaining_stock_no_ind - 1 < 0:
                            # exceed number of stocks
                            break
                        
                        lots, add_position = calc_position(outstanding_position, total_position_ind, s, 0, stocks_df, context)
                        if lots > 0:
                            outstanding_position += add_position
                            total_position_ind += add_position
                            
                            remaining_stock_no -= 1
                            remaining_stock_no_ind -= 1
                            
                            context.selected_stocks.append((x['ind'], s, add_position, stocks_df.ix[s, 'pb_ratio'], stocks_df.ix[s, 'pe_ratio'], lots))
                            position_changed = True
                            # hard code first
                            # context.current_stock_no += 1
                        #     print('%d, %.2f' % (lots, add_position))
                        # else:
                        #     print('no position: %s' %s)
                    
                    if position_changed:
                        remaining_industry_no -= 1
        else:
            logger.info('cannot find the stocks')
    elif pb < GOOD_MARKET_PB:
        logger.info('exceed max_allowed_position, current_position: %.2f' % context.current_position)

    
def handle_bar(context, bar_dict):
    # 这里处理止盈 和 投机情况
    pass
    # if context.cut_loss_enalbed:
    #     dao_ot.cut_loss(context, bar_dict)
    # else:
    #     pass

def after_trading(context):
    # 根据现有仓位的信息，重新计算 position, total_position, stock_no, industry_no
    
    if context.position_stock.empty:
        # 。。。
        pass
    
    portfolio_value = context.portfolio.portfolio_value
    positions = context.portfolio.positions
    position_stock = context.position_stock
    
    context.market.TotalPosition = context.portfolio.market_value / portfolio_value * 100
    context.current_industry_no = position_stock['ind'].unique().size
    context.current_stock_no = position_stock.index.size
    
    for s in position_stock.index:
        if s in positions.keys() and positions[s].quantity > 0:
            position_stock.ix[s, 'position'] = positions[s].market_value / portfolio_value
            if position_stock.ix[s, 'lots'] == positions[s].quantity / 100:
                continue
            else:
                logger.error('something wrong: %s %d lots, lots_postions: %d' % (s, position_stock.ix[s, 'lots'], positions[s].quantity / 100))
        else:
            logger.error('something wrong: %s not in positions' % s)

def sell_trade(context, bar_dict):
    stock_list = []
    for s in context.portfolio.positions.keys():
        if context.portfolio.positions[s].quantity > 0:
            stock_list.append(s)
    speculative_df = dao_bds.is_speculative(stock_list, context.now)
    if speculative_df is not None:
        speculative_df = speculative_df[speculative_df.speculative]
        if not(speculative_df.empty):
            for s in list(speculative_df.index):
                logger.info('sell the speculative stock [%s]' % s)
                order_target_percent(s, 0)
        
def buy_trade(context, bar_dict):
    if len(context.selected_stocks):
        logger.info('buy in ...')
        for (ind, s, lots, series) in context.selected_stocks:
            if not(is_suspended(s)):
                order_lots(s, lots, style=LimitOrder(bar_dict[s].close))
                if s in context.position_stock.index:
                    total_lots = context.position_stock.ix[s, 'lots'] + lots
                else:
                    total_lots = lots
                context.position_stock.ix[s] = pd.DataFrame({'ind': ind, 'pb': series['pb_ratio'], 'pe': series['pe_ratio'],'X': series['X'], 'lots': totalots})
                logger.info('%s(%s): %d lots' % (instruments(s).symbol, s, lots))
            else:
                logger.info('%s is not in trading' % s)
    else:
        logger.info('don\'t have any trade')

def get_stocks(context):
    x_list = []   
    grouped_df = dao_bds.pick_stocks(context.now)

    for s in grouped_df:
        ind = s[1].ix[:,'industry_name']
        
        tmp = s[1].sort_values(by='X', ascending = False)
        tmp.drop(['industry', 'industry_name'], axis=1, inplace=True)
    
        if tmp.index.size >= 1:
            min_prob = tmp.ix[0, 'X'] - 0.06
            tmp = tmp[tmp['X'] >= min_prob]
            
            tmp['rank'] = dao_bds.get_rank(tmp['pb_ratio'], tmp['pe_ratio'], tmp['pct'], tmp['is_special'])
            tmp.sort_values(by='rank', ascending = False, inplace=True)
            tmp = tmp[tmp['rank'] > MIN_RANK]
            
            x_tmp = tmp[tmp['X'] >= 0.9]
            if x_tmp.size > 1:
                x_list.append({'ind': s[0], 'ind_name': ind.values[0], 'df': x_tmp.head(MAX_STOCK_NO_PER_IND), 'rank': np.mean(x_tmp.head(MAX_STOCK_NO_PER_IND)['rank'])})
            elif tmp.size > 0:
                x_list.append({'ind': s[0], 'ind_name': ind.values[0], 'df': tmp.head(1), 'rank': tmp.head(1)['rank'].values[0]})
    
    if len(x_list):
        x_list = sorted(x_list, key=operator.itemgetter("rank"), reverse = True)
    
    if DEBUG_BY_DAO:
        debug_display_stock_list(x_list)
    
    return x_list

def calc_position(outstanding_position, total_position_ind, s, current_position, stocks_df, context):
    cash_per_pect = context.cash_per_pect
    allowed_position = context.max_allowed_position - context.current_position
    
    if total_position_ind == 0 and stocks_df.index.size >= MAX_STOCK_NO_PER_IND:
        ratio = 1.0 / min(stocks_df.index.size, MAX_STOCK_NO_PER_IND)
    else:
        ratio = 1.0
    
    add_postion = (stocks_df.ix[s,'X'] * MAX_POSTION_PER_IND - current_position) * ratio
    if add_postion > 0:
        add_postion = min(add_postion, MAX_POSTION_PER_IND - total_position_ind, allowed_position - outstanding_position)
        if add_postion > 0:
            lots = math.floor(add_postion * cash_per_pect / stocks_df.ix[s,'price'] / 100)
            if lots > 0:
                return (lots, round(lots * 100 * stocks_df.ix[s,'price'] / cash_per_pect, 3))
    
    return (0,0)
    
def debug_display_stock_list(x_list):
    i = 1
    for x in x_list:
        print('---------------')
        print('%d. %s - %s (%.2f)' % (i, x['ind_name'], x['ind'], (x['df']['X'] * 3).mean()))
        print(x['df'])
        
        i += 1
 
def get_market_pb(df_x, date):
    return df_x[df_x.index <= date.strftime('%Y-%m-%d')].ix[-1,'pb']
    
def update_stock_position():
    for s in context.portfolio.positions.keys():
        if context.portfolio.positions[s].quantity == 0:
           pass
    
    total_position = 0
    stock_list = []
    for ind in context.stocks_in_industry.keys():
        obj_ind = context.stocks_in_industry[ind]
        for s in obj_ind.keys():
            if s != 'total_position':
                stocks_list.append(obj_ind[s])
    
