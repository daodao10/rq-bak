import dao_strategy_util as dutil
from dao_strategy_base import *
from dao_stop_profit_or_loss_impl import *
from dao_adjust_condition_impl import *
from dao_query_impl import *
from dao_adjust_position_impl import *

def init(context):
    '''
    1.持仓股票的处理规则
    2.调仓条件判断规则
    3.选股规则
    4.股票池过滤规则
    5.调仓规则
    6.其它规则
    '''
    context.keys = ['0.position_stock', '1.adjust_condition','2.query_stock','3.filter_stock','4.adjust_position','5.other']
    
    strategies = get_strategies(context.keys)
    
    # create
    rules = {}
    for k in context.keys:
        rules[k] = dutil.create_rules(strategies[k])
    context.rules = rules
    
    # merge
    tmp = []
    for k in context.keys:
        tmp.extend(rules[k])
    context.rules_ = set(tmp)

    for rule in context.rules_:
        rule.initialize(context)

    dutil.log_rules(context.rules, context.keys)

    # # special process:
    # for r in context.rules[context.keys[1]]:
    #     if r.__class__.__name__ == 'Period_condition':
    #         scheduler.run_monthly(update_adjustment_interval, 1)
    #         break


def before_trading(context):
    logger.info("===============================================")
    context.cut_loss_fired = False
    context.filter_list = []
    for rule in context.rules_:
        rule.before_trading_start(context)


def handle_bar(context, bar_dict):
    keys = context.keys
    rules = context.rules
    
    # 其它辅助规则
    for rule in rules[keys[5]]:
        rule.handle_bar(context, bar_dict)

    # 持仓股票: 个股止损止盈
    for rule in rules[keys[0]]:
        rule.handle_bar(context, bar_dict)

    # -----------------------------------------------------------
    # 调仓器的分钟处理
    for rule in rules[keys[4]]:
        rule.handle_bar(context, bar_dict)
    
    # 调仓条件,所有规则以 and 逻辑执行
    for rule in rules[keys[1]]:
        rule.handle_bar(context, bar_dict)
        if not rule.can_adjust:
            return
    
    # ---------------------调仓--------------------------
    logger.info("handle_bar: ==> 满足条件进行调仓")
    # 调仓前预处理
    for rule in context.rules_:
        rule.before_adjust_start(context, bar_dict)

    # 选股
    q = query()
    for rule in rules[keys[2]]:
        q = rule.select(context, bar_dict, q)
    stock_list = get_fundamentals(q).columns.values if q != None else []
    
    # if len(context.filter_list) > 0:
    #     stock_list = [x for x in stock_list if x not in context.filter_list]
    # 过滤
    for rule in rules[keys[3]]:
        stock_list = rule.filter(context, bar_dict, stock_list)
    logger.info("股票池: %s" % (stock_list))
    
    # 调仓
    for rule in rules[keys[4]]:
        rule.adjust(context, bar_dict, stock_list)

    # 调仓后处理
    for rule in context.rules_:
        rule.after_adjust_end(context, bar_dict)
    # ----------------------------------------------------


def after_trading(context):
    for rule in context.rules_:
        rule.after_trading_end(context)

    # 未完成订单
    orders = get_open_orders()
    for _order in orders:
        logger.info("canceled uncompleted order: %s" % (_order.order_id))


def get_strategies(keys):
    strategies = {}
    
    # 配置list: 
    # [0.规则是否启用，1.规则描述，2.规则实现类名，3.规则传递参数(dict)]
    
    # 0.position_stock
    strategies[keys[0]] = [
        [False, '个股止损', Stop_loss_stocks, {
            'auto_threshold': False,
            'period': 3,
            'threshold': 0.05 # don't set if auto_threshold
        }],
        [False, '个股止盈', Stop_profit_stocks, {
            'auto_threshold': False,
            'period': 3,
            'threshold': 0.15 # don't set if auto_threshold 
        }],
        [False, '个股退出策略', Stop_loss_profit_by_drawdown, {
            'drawdown_max': 0.1
        }]
    ]
    # 1.adjust_condition
    strategies[keys[1]] = [
        [False, '指数回撤止损', Stop_loss_by_index_price, {
            'index': '000001.XSHG',
            'period': 60,
            'drawdown_min': 0.30,
            'drawdown_max': 0.55
        }],
        [True, '指数三乌鸦止损', Stop_loss_by_3_black_crows, {
            'index': '000001.XSHG',
            'minute_counter_index_drop': 60
        }],
        [True, '28指数止损', Stop_loss_by_28_index, {
            'index2': '000300.XSHG',#'000016.XSHG',
            'index8': '000905.XSHG',#'399333.XSHE',
            'period': 20,
            'index_growth_rate': 0.01,
            'minute_counter_28index_drop': 55
        }],
        [True, '调仓时间', Time_condition, {
            'hour': 14,
            'minute': 50
        }],
        [False, '指数回归线择时', Stop_loss_by_index_regress, {
            'index': '000001.XSHG',
            'period': 5
        }],
        [True, '调仓日计数器', Period_condition, {
            'period': 3,
        }],
        [True, '28择时', Stop_loss_by_28_index, {
            'index2': '000300.XSHG',
            'index8': '000905.XSHG',
            'period': 20,
            'index_growth_rate': 0.01,
            'minute_counter_28index_drop': 1
        }]
    ]
    # 2.query_stock
    strategies[keys[2]] = [
        [False, '选取指数', Query_by_index, {
            'index': '000905.XSHG'
        }],
        [True, '选取小市值', Query_by_market_cap, {
            '_min':0,
            '_max':200 # 亿
        }],
        [False, '选取A股小市值', Query_by_A_share_market_cap, {
            '_min':0,
            '_max':200 # 亿
        }],
        [True, '过滤PE', Query_by_pe, {
            '_min': 0,
            '_max': 70
        }],
        [False, '过滤PB', Query_by_pb, {
            '_min': 0,
            '_max': 1.5
        }],
        [False, '过滤EPS', Query_by_eps, {
            '_min': 0,
            '_max': 20
        }],
        [False, '过滤扣非EPS', Query_by_adjusted_eps, {
            '_min': 0,
            '_max': 20
        }],
        [False, '过滤ROIC', Query_by_roic, {
            '_min': 0.01
        }],
        [False, '过滤PCF', Query_by_pcf, {
            '_min': 0
        }],
        [False, '过滤OCF_to_Profit', Query_by_ocf_to_profit, {
            '_min': 1
        }],
        [False, '过滤FCFF', Query_by_fcff, {
            '_min': 0
        }],
        [False, '过滤SOLVENCY', Query_by_solvency, {
            'debt_to_asset_ratio_max': 40,
            'current_ratio_min': 1.5,
            'quick_ratio_min': 0.9,
            'time_interest_earned_ratio_min': 1.5
        }],
        [True, '过滤INC_', Query_by_inc_, {
            'revenue_ratio_min': 20,
            'net_profit_ratio_min': 20,
            'gross_profit_margin_min': 20
        }],
        [True, '初选股票数量', Query_by_limit, {
            'stock_count': 100
        }]
    ]
    # 3.filter_stock
    strategies[keys[3]] = [
        [True, '过滤创业板', Filter_gem, {}],
        [True, '过滤停牌', Filter_suspended, {}],
        [True, '过滤ST', Filter_st, {}],
        [False, '过滤成交额', Filter_turnover, {
            '_min': 30000000
        }],
        [True, '过滤次新股', Filter_listed_days, {
            '_min': 200
        }],
        [True, '过滤涨停', Filter_limit_up, {}],
        [True, '过滤跌停', Filter_limit_down, {}],
        [False, '过滤N日增长率为负的股票', Filter_growth_is_down, {
            'period': 5
        }],
        [True, '终选股票数量', Filter_by_limit, {
            'stock_count': 12
        }]
    ]
    # 4.adjust_position
    strategies[keys[4]] = [
        [False, '卖出股票', Sell_stocks, {}],
        [False, '买入股票', Buy_stocks, {
            'max_position_count': 3
        }],
        [True, '再平衡调仓', Rebalance, {
            'max_weight_per_position': 0.35,
            'max_position_count': 10,
            'unlimited_selling': False,
            'rebalance_enabled': True
        }]
    ]
    # 5.other
    strategies[keys[5]] = [
        [True, '统计', Stats, {}]
    ]
    
    return strategies

    
def update_adjustment_interval(context, bar_dict):
    for r in context.rules[context.keys[1]]:
        if r.__class__.__name__ == 'Period_condition':
            interval = dutil.get_adjustment_interval(context.now.strftime('%Y'))
            if (r.day_counter % r.period > 0) and r.period != interval:
                logger.debug('patch ...')
                r.day_counter = r.day_counter % r.period + interval - r.period
            r.log_info('更新<调仓频率>: %d日' % interval)
            r.update_params(context, {'period':interval})
            break
        
