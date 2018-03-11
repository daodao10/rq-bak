import pandas as pd
import datetime

from rqdatac import * 

def query_tricks(stocks, quarter, interval):
    q = query(
        financials.income_statement.operating_revenue,    
        financials.balance_sheet.net_inventory,
        financials.balance_sheet.net_accts_receivable,
        financials.financial_indicator.working_capital
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_profit(stocks, quarter, interval):
    q = query(
        financials.income_statement.net_profit_parent_company,
        financials.financial_indicator.inc_net_profit,
        financials.financial_indicator.adjusted_net_profit,
        financials.financial_indicator.inc_adjusted_net_profit,
        financials.financial_indicator.adjusted_profit_to_total_profit
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_revenue(stocks, quarter, interval):
    q = query(
        financials.income_statement.operating_revenue,
        financials.financial_indicator.inc_operating_revenue,
        financials.financial_indicator.net_profit_margin,
        financials.balance_sheet.net_inventory,
        financials.balance_sheet.net_accts_receivable
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_operating(stocks, quarter, interval):
    q = query(
        financials.financial_indicator.return_on_asset_net_profit,
        financials.financial_indicator.return_on_equity,
        financials.financial_indicator.return_on_invested_capital,
        financials.financial_indicator.gross_profit_margin,
        financials.financial_indicator.net_profit_margin,
        financials.financial_indicator.total_asset_turnover,
        financials.financial_indicator.inventory_turnover,
        financials.financial_indicator.account_receivable_turnover_rate
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_debt(stocks, quarter, interval):
    q = query(
        financials.financial_indicator.debt_to_asset_ratio,
        financials.financial_indicator.current_ratio,
        financials.financial_indicator.quick_ratio,
        financials.balance_sheet.short_term_loans,
        financials.balance_sheet.long_term_loans,
        financials.balance_sheet.notes_payable,
        financials.balance_sheet.accts_payable,
        financials.balance_sheet.advance_from_customers,
        financials.balance_sheet.other_payable,
        financials.balance_sheet.total_liabilities,
        financials.financial_indicator.interest_bearing_debt     
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_expense(stocks, quarter, interval):
    q = query(
        financials.income_statement.selling_expense,
        financials.income_statement.ga_expense,
        financials.income_statement.financing_expense,
        financials.income_statement.period_cost,
        financials.income_statement.operating_revenue
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_working_capital(stocks, quarter, interval):
    q = query(
        financials.balance_sheet.inventory,
        financials.balance_sheet.accts_receivable,
        financials.balance_sheet.bill_receivable,
        financials.balance_sheet.prepayment,        
        financials.balance_sheet.accts_payable,
        financials.balance_sheet.notes_payable,
        financials.balance_sheet.advance_from_customers,
        financials.income_statement.operating_revenue
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_cash_flow(stocks, quarter, interval):
    q = query(
        financials.cash_flow_statement.cash_flow_from_operating_activities,
        financials.financial_indicator.inc_cash_from_operations,
        financials.cash_flow_statement.cash_flow_from_investing_activities,
        financials.cash_flow_statement.cash_paid_for_asset,
        financials.cash_flow_statement.cash_flow_from_financing_activities
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    return get_financials(q, quarter = quarter, interval = interval)

def query_valuation(stocks):
    yesterday = datetime.date.today() + datetime.timedelta(days=-1)
    q = query(
        fundamentals.eod_derivative_indicator.market_cap,
        # TTM
        fundamentals.eod_derivative_indicator.pb_ratio,
        fundamentals.eod_derivative_indicator.pe_ratio,
        fundamentals.eod_derivative_indicator.pcf_ratio,
        # 静态的
        #fundamentals.eod_derivative_indicator.pe_ratio_1,     
        # 根据当前的乘以比例~加权~预测
        fundamentals.eod_derivative_indicator.peg_ratio,
        fundamentals.eod_derivative_indicator.ps_ratio,
        #fundamentals.eod_derivative_indicator.pe_ratio_2,
        #fundamentals.eod_derivative_indicator.pcf_ratio_1
    ).filter(financials.financial_indicator.stockcode.in_(stocks))
    
    df = get_fundamentals(q, entry_date = yesterday, interval = '1d')

    return df.ix[:,0]

def output_tricks(df, stock):
    stock_tricks = df.minor_xs(stock)
    stock_tricks = stock_tricks.reindex(index = stock_tricks.index[::-1])
    
    stock_tricks['operating_revenue'] = stock_tricks['operating_revenue'] / 10000
    stock_tricks['net_inventory'] = stock_tricks['net_inventory'] / 10000
    stock_tricks['net_accts_receivable'] = stock_tricks['net_accts_receivable'] / 10000
    stock_tricks['working_capital'] = stock_tricks['working_capital'] / 10000
    
    stock_tricks['营业收入增幅'] = stock_tricks['operating_revenue'] - stock_tricks['operating_revenue'].shift(1)
    stock_tricks['存货增幅'] = stock_tricks['net_inventory'] - stock_tricks['net_inventory'].shift(1)
    stock_tricks['应收账款增幅'] = stock_tricks['net_accts_receivable'] - stock_tricks['net_accts_receivable'].shift(1)
    
    return stock_tricks.T

def output_profit(df, stock):
    #print('盈利能——利润分析<%s>' % instruments(stock).symbol)
    stock_profit = df.minor_xs(stock)
    stock_profit = stock_profit.reindex(index = stock_profit.index[::-1])

    stock_profit['net_profit_parent_company'] = stock_profit['net_profit_parent_company'] / 10000
    stock_profit['adjusted_net_profit'] = stock_profit['adjusted_net_profit'] / 10000

    return stock_profit.T
    
def output_revenue(df, stock):
    #print('盈利能力——营收分析<%s>' % instruments(stock).symbol)
    stock_revenue = df.minor_xs(stock)
    stock_revenue = stock_revenue.reindex(index = stock_revenue.index[::-1])

    stock_revenue['存货增长率'] = stock_revenue['net_inventory'].pct_change() * 100
    #TODO: 应收款合计
    stock_revenue['应收帐款增长率'] = stock_revenue['net_accts_receivable'].pct_change() * 100
    stock_revenue['operating_revenue'] = stock_revenue['operating_revenue'] / 10000
    
    stock_revenue.drop(['net_inventory','net_accts_receivable'], axis=1, inplace=True)

    return stock_revenue.T
    
def output_operating(df, stock):
    #print('营运能力分析<%s>' % instruments(stock).symbol)
    stock_operating = df.minor_xs(stock)
    stock_operating = stock_operating.reindex(index = stock_operating.index[::-1])

    return stock_operating.T
    
def output_debt(df, stock):
    #print('偿债能力分析<%s>' % instruments(stock).symbol)
    stock_debt = df.minor_xs(stock)
    stock_debt = stock_debt.reindex(index = stock_debt.index[::-1])

    stock_debt['负债增长率'] = stock_debt['total_liabilities'].pct_change() * 100
    stock_debt['有息负债占比'] = stock_debt['interest_bearing_debt'] / stock_debt['total_liabilities'] * 100

    stock_debt['long_term_loans'] = stock_debt['long_term_loans'] / 10000
    stock_debt['short_term_loans'] = stock_debt['short_term_loans'] / 10000
    stock_debt['notes_payable'] = stock_debt['notes_payable'] / 10000
    stock_debt['accts_payable'] = stock_debt['accts_payable'] / 10000
    stock_debt['advance_from_customers'] = stock_debt['advance_from_customers'] / 10000
    stock_debt['other_payable'] = stock_debt['other_payable'] / 10000
    stock_debt['total_liabilities'] = stock_debt['total_liabilities'] / 10000
    stock_debt['interest_bearing_debt'] = stock_debt['interest_bearing_debt'] / 10000

    return stock_debt.T
    
def output_expense(df, stock):
    #print('营运能力——费用分析<%s>' % instruments(stock).symbol)
    stock_expense = df.minor_xs(stock)
    stock_expense = stock_expense.reindex(index = stock_expense.index[::-1])

    stock_expense['period_cost'] = stock_expense['selling_expense'] + stock_expense['ga_expense'] + stock_expense['financing_expense']
    stock_expense['三费占比'] = stock_expense['period_cost'] / stock_expense['operating_revenue'] * 100
    stock_expense['销售费比营收'] = stock_expense['selling_expense'] / stock_expense['operating_revenue'] * 100

    stock_expense['selling_expense'] = stock_expense['selling_expense'] / 10000
    stock_expense['ga_expense'] = stock_expense['ga_expense'] / 10000
    stock_expense['financing_expense'] = stock_expense['financing_expense'] / 10000
    stock_expense['period_cost'] = stock_expense['period_cost'] / 10000
    stock_expense['operating_revenue'] = stock_expense['operating_revenue'] / 10000

    return stock_expense.T
    
def output_cash_flow(df, stock):
    #print('真金白银——现金流分析<%s>' % instruments(stock).symbol)
    stock_cash_flow = df.minor_xs(stock)
    stock_cash_flow = stock_cash_flow.reindex(index = stock_cash_flow.index[::-1])

    stock_cash_flow['自由现金流'] = (stock_cash_flow['cash_flow_from_operating_activities'] - stock_cash_flow['cash_paid_for_asset']) / 10000

    stock_cash_flow['cash_flow_from_operating_activities'] = stock_cash_flow['cash_flow_from_operating_activities'] / 10000
    stock_cash_flow['cash_flow_from_investing_activities'] = stock_cash_flow['cash_flow_from_investing_activities'] / 10000
    stock_cash_flow['cash_paid_for_asset'] = stock_cash_flow['cash_paid_for_asset'] / 10000
    stock_cash_flow['cash_flow_from_financing_activities'] = stock_cash_flow['cash_flow_from_financing_activities'] / 10000

    return stock_cash_flow.T

# 外部资金占用状况比率=(应付款-应收款)/收入
# 应付款包括应付账款,其他应付款,应付票据,预收账款;应收账款包括应收票据,应收账款,其他应收款,预付账款。
# 该指标越大,表明企业占用上下游企业的资金相对越多,或者表明企业货币资金紧张,或者是过分精明甚至霸道;越小,表明企业占用上下游企业的资金相对少,可能是企业货币资金宽裕,也可能应收账款管理不力。
# 对该指标的进一步分析可以用以下两个指标。
# 一是应付款比率=平均应付款余额/收入
# 该比率可以作横向或纵向对比分析,看企业占用上游企业资金状况。
# 二是预收款比率=平均预收款余额/收入
# 该指标分析对下游企业资金的占用状况。
# 三、内部货币节约比率=非付现成本/收入
# 造成企业现金与利润差异的原因是非付现成本的存在,非付现成本越多,企业利润质量越高
def output_account(df, stock):
    #print('行业上下游地位分析<%s>' % instruments(stock).symbol)
    stock_account = df.minor_xs(stock)
    stock_account = stock_account.reindex(index = stock_account.index[::-1])

    stock_account.fillna(0, inplace = True)
    
    #上游资金的占用=应付款合计差额-（存货合计差额+预付款合计差额）
    stock_account['应付款合计'] = stock_account['notes_payable'] + stock_account['accts_payable']
    stock_account['上游占款'] = stock_account['应付款合计'] - (stock_account['inventory'] + stock_account['prepayment'])
    #下游资金的占用=应收款合计期末比期初减少差额+预收款期末比期初增加差额=预收款差额-应收款合计差额
    stock_account['应收款合计'] = stock_account['bill_receivable'] + stock_account['net_accts_receivable']
    stock_account['下游收款'] = stock_account['advance_from_customers'] - stock_account['应收款合计']

    stock_account = stock_account / 10000
    
    return stock_account.T

def info(stocks):
    names = []
    for i in instruments(stocks):
        names.append(i.symbol)
        print('%s[%s, %s] - [ %s - %s, 概念： %s]' % (i.symbol, i.order_book_id, i.listed_date, i.sector_code_name, i.industry_name, i.concept_names))
        
    valuation = query_valuation(stocks)
    valuation['market_cap'] = valuation['market_cap'] / 100000000
    valuation['name'] = pd.Series(data=names, index=stocks)
    
    return valuation
