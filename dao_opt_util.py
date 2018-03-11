import pandas as pd
import numpy as np

from scipy.stats import linregress

import datetime
import math

from rqdatac import * 


def get_fx(regress_setting, x_series, date_index):
    return pd.Series([(regress_setting.intercept + regress_setting.slope * x) for x in x_series], index = date_index)

def get_peak_valley(y, ref):
    # TODO:
    tmp_ratio = y / ref
#     min_index = tmp_ratio.argmin()
#     max_index = tmp_ratio.argmax()
    
#     return tuple(tmp_ratio[max_index], tmp_ratio[min_index])
    return tuple([tmp_ratio.min(), tmp_ratio.max()])

def calc_cagr(fx_series, regress_setting):
    # TODO: 
    return 1 if regress_setting.slope > 0 else -1
    
def get_opts(order_book_id, start_date = '2005-01-04', end_date = datetime.date.today(), show_lines = False):
    his = get_price(order_book_id, start_date = start_date, end_date = end_date, frequency = '1d', fields = 'close')
    x_s = range(1, len(his) + 1)
    
    log_his = np.log2(his)
    reg = linregress(x_s, log_his)
    
    # log_trend_line = get_fx(reg, x_s, log_his.index)
    # trend_line = 2 ** log_trend_line
    trend_line = 2 ** get_fx(reg, x_s, log_his.index)
    
    min_ratio, max_ratio = get_peak_valley(his, trend_line)
        
    diff = his[-1] / trend_line[-1]
    if diff >= 1:
        pct = (0.5 + math.log2(diff)/math.log2(max_ratio)/2) * 100
    else:
        pct = (0.5 - math.log2(diff)/math.log2(min_ratio)/2) * 100
    
    r2 = reg.rvalue ** 2
    cagr = calc_cagr(log_his, reg)
    
    if show_lines:
        t1_line = trend_line * max_ratio
        t2_line = trend_line * math.sqrt(max_ratio)

        b1_line = trend_line * min_ratio
        b2_line = trend_line * math.sqrt(min_ratio)

        df = pd.concat([his, t1_line, t2_line, trend_line, b2_line, b1_line], axis=1, keys=['Price','Top','T2', 'TL','B2','Bottom'])

        return tuple([df, r2, pct, cagr])
    else:
        return tuple([r2, pct, cagr])


def output_opt_result(order_book_id, start_date = '2005-01-04', end_date = datetime.date.today()):
    df, r2, pct, cagr = get_opts(order_book_id, start_date = start_date, end_date = end_date, show_lines = True)
    df.plot(logy=True)
    print('OPT = %.3f, R2 = %.3f, CAGR = %d' % (pct, r2, cagr))
    
    #return df