from rqdatac import * 


def get_stocks_in_my_index(index_code, date):
    MY_INDEX_DIC = {
        'BANK.DAO':['601988.XSHG', '601009.XSHG', '601398.XSHG', '601939.XSHG', '600036.XSHG', '000001.XSHE', '600016.XSHG', '601998.XSHG', '601288.XSHG', '002142.XSHE', '601818.XSHG', '600015.XSHG', '601169.XSHG', '600000.XSHG', '601328.XSHG', '601166.XSHG']
    }

    d = date.strftime('%Y-%m-%d')
    current_list = MY_INDEX_DIC[index_code]
    
    return [i.order_book_id for i in instruments(current_list) if i.listed_date <= d]

def get_symbol(code):
    if code.endswith('.DAO'):
        return code
    
    inst = instruments(code)
    if inst:
        return inst.symbol

    return code