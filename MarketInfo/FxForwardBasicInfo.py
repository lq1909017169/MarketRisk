import map_table
import datetime


def start(date):
    table = ['DM_FOREX', 'DS_FX_Forward_Basic_Info']
    start_time = datetime.datetime.now()
    mapping={
        'TRADE_REF': 'trade_Id',
        'PURCHASE_CCY_VALUATION': 'buy_PV',
        'PURCHASE_CCY': 'buy_Currency',
        'SALE_CCY_VALUATION': 'pay_PV',
        'SALE_CCY': 'pay_Currency',
        'MATURITY_DATE': 'maturity_Date',
        'VALUE_DATE': 'start_Date',
        'CPTY_ID': 'counter_Party',
        'PARTITION_KEY': 'PARTITION_KEY',
        'TXN_DRC': 'buy_Sell'
    }

    statue, info = map_table.map_t(table=table, maping=mapping,
                                   select_date=date, select_sql=f"FX_TYPE='N' AND BOOK_TYPE='T'",
                                   table_name='DS_FX_Forward_Basic_Info')

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


