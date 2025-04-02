import datetime

import map_table


def start(date):
    start_time = datetime.datetime.now()
    table = ['DM_FOREX', 'DS_Fx_Swap_Basic_Info']
    mapping = {
        'TRADE_REF': 'trade_Id',
        'VALUE_DATE': 'effective_Date',
        'MATURITY_DATE': 'pay_Date',
        'PURCHASE_CCY': 'purchase_CCY',
        'SALE_CCY': 'sale_CCY',
        'PARTITION_KEY': 'PARTITION_KEY',
        'PURCHASE_CCY_VALUATION': 'purchase_PV',
        'SALE_CCY_VALUATION': 'sale_PV',
        'PURCHASE_CCY_VALUATION_FAR_LEG': 'purchase_FAR_PV',
        'SALE_CCY_VALUATION_FAR_LEG': 'sale_FAR_PV',
        'TXN_DRC': 'buy_Sell',
        'FAR_END_BUY_CCY': 'far_Purchase_CCY',
        'FAR_END_SELL_CCY': 'far_Sale_CCY'
    }

    statue, info = map_table.map_t(table=table, maping=mapping, select_date=date,
                                   select_sql=f"FX_TYPE='S' AND BOOK_TYPE='T'", table_name='DS_Fx_Swap_Basic_Info')
    end_time = datetime.datetime.now()
    data = {
        'name': __name__,
        'partition_key': date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': statue
    }
    map_table.insert_log(data)
