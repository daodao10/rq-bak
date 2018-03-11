import dao_strategy_util as dutil
from dao_strategy_base import *

'''=============选股============='''
class Query_by_index(Query_stock_list):    
    def __init__(self, params):
        self.index = params.get('index', '000300.XSHG')

    def update_params(self, context, params):
        self.index = params.get('index', self.index)

    def select(self, context, bar_dict, q):
        stock_list = index_components(self.index, context.now)
        # logger.info(len(stock_list))
        return q.filter(fundamentals.eod_derivative_indicator.stockcode.in_(stock_list))

    def __str__(self):
        return '选取股票在 指数 %s - %s' % (self.index, instruments(self.index).symbol)

class Query_by_market_cap(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)
        self._max = params.get('_max', 200)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)
        self._max = params.get('_max', self._max)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.eod_derivative_indicator.market_cap > self._min * 100000000,
            fundamentals.eod_derivative_indicator.market_cap <= self._max * 100000000,
        ).order_by(fundamentals.eod_derivative_indicator.market_cap.asc())

    def __str__(self):
        return '按市值倒序选取股票: [ %d < 市值 <= %d] 亿' % (self._min, self._max)

class Query_by_A_share_market_cap(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)
        self._max = params.get('_max', 200)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)
        self._max = params.get('_max', self._max)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.eod_derivative_indicator.a_share_market_val > self._min * 100000000,
            fundamentals.eod_derivative_indicator.a_share_market_val <= self._max * 100000000,
        ).order_by(fundamentals.eod_derivative_indicator.a_share_market_val.asc())

    def __str__(self):
        return '按A股市值倒序选取股票: [ %d < 市值 <= %d] 亿' % (self._min, self._max)

class Query_by_pe(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)
        self._max = params.get('_max', 200)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)
        self._max = params.get('_max', self._max)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.eod_derivative_indicator.pe_ratio > self._min,
            fundamentals.eod_derivative_indicator.pe_ratio <= self._max
        )

    def __str__(self):
        return '按PE范围选取股票: [ %d < pe <= %d]' % (self._min, self._max)
    
class Query_by_pb(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)
        self._max = params.get('_max', 20)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)
        self._max = params.get('_max', self._max)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.eod_derivative_indicator.pb_ratio > self._min,
            fundamentals.eod_derivative_indicator.pb_ratio <= self._max
        )

    def __str__(self):
        return '按PB范围选取股票: [ %.2f < pb <= %.2f ]' % (self._min, self._max)
    
class Query_by_adjusted_eps(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)
        self._max = params.get('_max', 20)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)
        self._max = params.get('_max', self._max)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.financial_indicator.adjusted_earnings_per_share > self._min,
            fundamentals.financial_indicator.adjusted_earnings_per_share <= self._max
        )

    def __str__(self):
        return '按扣非EPS范围选取股票: [ %.2f < adjusted eps <= %.2f ]' % (self._min, self._max)
    
class Query_by_eps(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)
        self._max = params.get('_max', 20)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)
        self._max = params.get('_max', self._max)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.financial_indicator.earnings_per_share > self._min,
            fundamentals.financial_indicator.earnings_per_share <= self._max
        )

    def __str__(self):
        return '按EPS范围选取股票: [ %.2f < eps <= %.2f ]' % (self._min, self._max)

class Query_by_roic(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.financial_indicator.return_on_invested_capital > self._min
        )

    def __str__(self):
        return '按ROIC范围选取股票: [ roic > %.2f ]' % (self._min)

class Query_by_pcf(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.eod_derivative_indicator.pcf_ratio_1 > self._min
        )

    def __str__(self):
        return '按PCF范围选取股票: [ pcf > %.2f ]' % (self._min)

class Query_by_ocf_to_profit(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 1.5)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.financial_indicator.operating_profit_to_profit_before_tax > self._min
            #fundamentals.cash_flow_statement.cash_flow_from_operating_activities / fundamentals.income_statement.net_profit > self.ratio_min
        )

    def __str__(self):
        return '按OCF_to_Profit范围选取股票: [ OCF_to_Profit > %.2f ]' % (self._min)

# 企业自由现金流量FCFF
class Query_by_fcff(Query_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 0)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.financial_indicator.fcff > self._min
        )

    def __str__(self):
        return '按FCFF范围选取股票: [ FCFF > %.2f ]' % (self._min)

class Query_by_solvency(Query_stock_list):
    def __init__(self, params):
        self.debt_to_asset_ratio_max = params.get('debt_to_asset_ratio_max', 0.6)
        self.current_ratio_min = params.get('current_ratio_min', 1.5)
        self.quick_ratio_min = params.get('quick_ratio_min', 0.9)
        self.time_interest_earned_ratio_min = params.get('time_interest_earned_ratio_min', 1)

    def update_params(self, context, params):
        self.debt_to_asset_ratio_max = params.get('debt_to_asset_ratio_max', self.debt_to_asset_ratio_max)
        self.current_ratio_min = params.get('current_ratio_min', self.current_ratio_min)
        self.quick_ratio_min = params.get('quick_ratio_min', self.quick_ratio_min)
        self.time_interest_earned_ratio_min = params.get('ratio_min', self.time_interest_earned_ratio_min)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.financial_indicator.debt_to_asset_ratio <= self.debt_to_asset_ratio_max,
            # 注释了，因为这些财务指标目前ricequant没有数据
            #fundamentals.financial_indicator.current_ratio > self.current_ratio_min,
            #fundamentals.financial_indicator.quick_ratio > self.quick_ratio_min,
            #fundamentals.financial_indicator.time_interest_earned_ratio > self.time_interest_earned_ratio_min
        )

    def __str__(self):
        return '按SOLVENCY范围选取股票: [ \
        Debt_to_Asset_Ratio <= %.2f,\
        Current_Ratio > %.2f,\
        Quick_Ratio > %.2f,\
        time_interest_earned_Ratio > %.2f ]' % \
    (self.debt_to_asset_ratio_max, self.current_ratio_min, self.quick_ratio_min, self.time_interest_earned_ratio_min)

class Query_by_inc_(Query_stock_list):
    def __init__(self, params):
        self.revenue_ratio_min = params.get('revenue_ratio_min', 0)
        self.net_profit_ratio_min = params.get('net_profit_ratio_min', 0)
        self.gross_profit_margin_min = params.get('gross_profit_margin_min', 0)
        self.return_on_equity_min = params.get('return_on_equity_min', 0)

    def update_params(self, context, params):
        self.revenue_ratio_min = params.get('revenue_ratio_min', self.revenue_ratio_min)
        self.net_profit_ratio_min = params.get('net_profit_ratio_min', self.net_profit_ratio_min)
        self.gross_profit_margin_min = params.get('gross_profit_margin_min', self.gross_profit_margin_min)
        self.return_on_equity_min = params.get('return_on_equity_min', self.return_on_equity_min)

    def select(self, context, bar_dict, q):
        return q.filter(
            fundamentals.financial_indicator.inc_revenue > self.revenue_ratio_min,
            fundamentals.financial_indicator.inc_net_profit > self.net_profit_ratio_min,
            fundamentals.financial_indicator.gross_profit_margin > self.gross_profit_margin_min,
            #fundamentals.financial_indicator.net_profit_margin > 10,
            fundamentals.financial_indicator.annual_return_on_equity > self.return_on_equity_min
        )

    def __str__(self):
        return '按INC_范围选取股票: [ \
        INC_REVENUE > %.2f,\
        INC_NET_PROFIT > %.2f,\
        GROSS_PROFIT_MARGIN > %.2f,\
        ROE > %.2f ]' % \
    (self.revenue_ratio_min, self.net_profit_ratio_min, self.gross_profit_margin_min, self.return_on_equity_min)

class Query_by_limit(Query_stock_list):
    def __init__(self, params):
        self.stock_count = params.get('stock_count', 0)

    def update_params(self, context, params):
        self.stock_count = params.get('stock_count', self.stock_count)

    def select(self, context, data, q):
        return q.limit(self.stock_count)

    def __str__(self):
        return '初选股票数量: [ %d ]' % (self.stock_count)


'''=============过滤============='''
class Filter_gem(Filter_stock_list):
    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if stock[0:1] != '3']

    def __str__(self):
        return '过滤创业板股票'


class Filter_suspended(Filter_stock_list):
    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if not is_suspended(stock)]

    def __str__(self):
        return '过滤停牌股票'


class Filter_limit_up(Filter_stock_list):
    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if bar_dict[stock].last < bar_dict[stock].limit_up]

    def __str__(self):
        return '过滤涨停股票'


class Filter_limit_down(Filter_stock_list):
    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if bar_dict[stock].last > bar_dict[stock].limit_down]

    def __str__(self):
        return '过滤跌停股票'


class Filter_st(Filter_stock_list):
    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if not(is_st_stock(stock) or self.is_delisted(bar_dict[stock]))]
    
    def is_delisted(self, bar):
        return bar.symbol.startswith('退') or bar.symbol.endswith('退') or bar.symbol.startswith('*')

    def __str__(self):
        return '过滤ST股票'


class Filter_turnover(Filter_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 10000000)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)

    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if dutil.get_last_day_turnover(stock) > self._min]
    
    def __str__(self):
        return '过滤成交额较低的股票: [ > %d ]' % self._min
    
    
class Filter_listed_days(Filter_stock_list):
    def __init__(self, params):
        self._min = params.get('_min', 250)

    def update_params(self, context, params):
        self._min = params.get('_min', self._min)

    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if instruments(stock).days_from_listed() > self._min]
    
    def __str__(self):
        return '过滤次新股: [ 上市日期 > %d 日 ]' % self._min


class Filter_growth_is_down(Filter_stock_list):
    def __init__(self, params):
        self.period = params.get('period', 20)

    def update_params(self, context, params):
        self.period = params.get('period', self.period)

    def filter(self, context, bar_dict, stock_list):
        return [stock for stock in stock_list if dutil.get_growth_rate(stock, self.period) > 0]

    def __str__(self):
        return '过滤增长率为负的股票: [ %d 日 ]' % self.period
    

class Filter_by_limit(Filter_stock_list):
    def __init__(self, params):
        self.stock_count = params.get('stock_count', 3)

    def update_params(self, context, params):
        self.stock_count = params.get('stock_count', self.stock_count)

    def filter(self, context, bar_dict, stock_list):
        if len(stock_list) > self.stock_count:
            return stock_list[:self.stock_count]
        else:
            return stock_list

    def __str__(self):
        return '终选股票数量: [ %d ]' % (self.stock_count)

