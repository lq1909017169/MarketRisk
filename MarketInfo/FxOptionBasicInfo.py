import datetime

import map_table


def start(date):
    start_time = datetime.datetime.now()
    table = ['DM_EXCHANGE_OPTION', 'DS_Fx_Option_Basic_Info']
    mapping = {
        'TRADE_REF': 'trade_Id',
        'CALL_PUT': 'option_Type',
        'STRIKE': 'strike_Price',
        'CONTRACT_MARKET_VALUE': 'underlying_Amount',
        'PARTITION_KEY': 'PARTITION_KEY',
        'CONTRACT_NORMINAL_CCY': 'underlying_Currency',
        'POSITION': 'buy_Sell',
        'OPT_DELTA': 'delta',
        'OPT_GAMMA': 'gamma',
        'OPT_VEGA': 'vega',
        'VOL': 'vol',
        'MATURITY_DATE': 'maturity_Date',
    }
    statue, info = map_table.map_t(table=table, maping=mapping, select_date=date)

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


if __name__ == '__main__':
    start('20240731')
