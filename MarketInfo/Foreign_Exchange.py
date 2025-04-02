import map_table
import datetime


def start(date):
    start_time = datetime.datetime.now()
    table = ['DM_EX_RATE', 'DS_Foreign_Exchange']
    mapping = {
        'PARTITION_KEY': 'PARTITION_KEY',
        'CURRENCY_IN': 'SOURCE',
        'CURRENCY_OUT': 'TARGET',
        'EX_RATE': 'VALUE'
    }

    statue, info = map_table.map_t(table=table, maping=mapping, table_name=table[1], select_date=date)

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
