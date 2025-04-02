import datetime

import map_table


def start(date):
    start_time = datetime.datetime.now()
    for table_name in ['DS_COMMODITY_REPORT_DETAIL', 'DS_COMMODITY_REPORT_SUMMARY', 'DS_FX_REPORT_DETAIL',
                       'DS_FX_REPORT_SUMMARY', 'DS_IR_REPORT_DETAIL', 'DS_IR_REPORT_SPECIFIC_SUMMARY',
                       'DS_IR_REPORT_SUMMARY', 'DS_OPTION_DETAIL', 'DS_OPTION_SUMMARY', 'RES_REPORT_DATA']:
        statue, info = map_table.delete_tables(table_name=table_name, partition_key=date)
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
