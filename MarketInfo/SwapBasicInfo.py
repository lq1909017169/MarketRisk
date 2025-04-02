import map_table
import datetime


def start(date):
    start_time = datetime.datetime.now()
    statu, info = map_table.map_t(table=['DM_SWAP', 'DS_Swap_Basic_Info'], maping={
        'TRADE_REF': 'trade_Id',
        # swap_Type
        'VALUE_DATE': 'effective_Date',
        'MATURITY_DATE': 'maturity_Date',
        'PAY_RATE_VALUE': 'pay_Rate',
        'PAY_CURRENCY': 'pay_Currency',
        'PAY_NXT_RESET_DATE': 'pay_Frequency',
        'REC_NXT_RESET_DATE': 'receive_Frequency',
        'PAY_RATE_TYPE': 'pay_Leg_Type',
        'REC_RATE_VALUE': 'receive_Rate',
        'REC_CURRENCY': 'receive_Currency',
        'REC_RATE_TYPE': 'receive_Leg_Type',
        'PAYMENT_SIDE_VAL': 'pay_PV',
        'REVENUE_SIDE_VAL': 'receive_PV',
        'PARTITION_KEY': 'PARTITION_KEY',
    }, table_name='DS_Swap_Basic_Info', select_date=date)

    end_time = datetime.datetime.now()
    data = {
        'name': __name__,
        'partition_key': date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': statu
    }
    map_table.insert_log(data)

