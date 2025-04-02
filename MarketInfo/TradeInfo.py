import map_table
import datetime


def start(date):
    start_time = datetime.datetime.now()
    table = ['DS_Swap_Basic_info', 'DS_Trade_Info']
    mapping = {
        'trade_Id': 'trade_Id',
        'PAY_CURRENCY': 'trade_Currency',
        'PARTITION_KEY': 'PARTITION_KEY'
    }
    status, info = map_table.map_t(table, mapping, append_name='DS_Swap_Basic_Info', select_date=date)

    end_time = datetime.datetime.now()
    data = {
        'name': 'DS_Swap_Basic_Info',
        'partition_key': date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': status
    }
    map_table.insert_log(data)

    table = ['DS_FX_Swap_Basic_Info', 'DS_Trade_Info']
    start_time = datetime.datetime.now()
    mapping = {
        'trade_Id': 'trade_Id',
        'PARTITION_KEY': 'PARTITION_KEY',
    }

    status, info = map_table.map_t(table, mapping, append_name='DS_FX_Swap_Basic_Info', select_date=date)

    end_time = datetime.datetime.now()
    data = {
        'name': 'DS_FX_Swap_Basic_Info',
        'partition_key': date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': status
    }
    map_table.insert_log(data)

    table = ['DS_FX_Forward_Basic_Info', 'DS_Trade_Info']
    start_time = datetime.datetime.now()
    mapping = {
        'trade_Id': 'trade_Id',
        'PARTITION_KEY': 'PARTITION_KEY',
    }

    status, info = map_table.map_t(table, mapping, append_name='DS_FX_Forward_Basic_Info', select_date=date)

    end_time = datetime.datetime.now()
    data = {
        'name': 'DS_FX_Forward_Basic_Info',
        'partition_key': date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': status
    }
    map_table.insert_log(data)

    # option
    table = ['DS_Fx_Option_Basic_Info', 'DS_Trade_Info']
    start_time = datetime.datetime.now()
    mapping = {
        'trade_Id': 'trade_Id',
        'PARTITION_KEY': 'PARTITION_KEY'
    }
    status, info = map_table.map_t(table, mapping, append_name='DS_FX_Option_Basic_Info', select_date=date)

    end_time = datetime.datetime.now()
    data = {
        'name': 'DS_FX_Forward_Basic_Info',
        'partition_key': date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': status
    }
    map_table.insert_log(data)
