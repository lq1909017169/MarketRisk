from Simple.utils.configuration_table import Config
from Simple.utils.models import FxReportSummary
from Simple.utils.report_data import insert_report, con, insert_log
from Simple.utils.utils import apply_map_callback
from Simple.utils.log import log
import pandas as pd
import datetime


def foreign(config: Config):
    """
    :param config: columns=[CURRENCY, LONG_SHORT_DOMESTIC, DELTA_DOMESTIC, OPEN_POSITION_DOMESTIC，
                             LONG_SHORT_NOT_DOMESTIC, DELTA_NOT_DOMESTIC, OPEN_POSITION_NOT_DOMESTIC]
    :return:
    """
    start_time = datetime.datetime.now()
    config.connect.clean_table(table=FxReportSummary.__table_name__, partition_key=config.partition_key)

    data = config.connect.to_df(
        f"SELECT * FROM DS_FX_REPORT_DETAIL WHERE PARTITION_KEY='{config.partition_key}'").applymap(
        apply_map_callback)
    log.info(f'DS_FX_REPORT_DETAIL PARTITION_KEY-{config.partition_key}'
             f' get <{data.shape[0]}> pieces of data')

    log.info(f'fx summary PARTITION_KEY-{config.partition_key}  count start')
    start = datetime.datetime.now()

    def load_collect(series):
        long_short = series['LONG_SHORT_DOMESTIC'] + series['DELTA_DOMESTIC'] + series['LONG_SHORT_NOT_DOMESTIC'] + series[
            'DELTA_NOT_DOMESTIC']
        open_position = series['OPEN_POSITION_DOMESTIC'] + series['OPEN_POSITION_NOT_DOMESTIC']
        return pd.Series([series['CURRENCY'], long_short, open_position, series['PARTITION_KEY']],
                         index=['CURRENCY', 'LONG_SHORT', 'OPEN_POSITION', 'PARTITION_KEY'])

    collect_data = data.apply(load_collect, axis=1)
    if config.write_to_database:
        log.info(f'fx detail PARTITION_KEY-{config.partition_key} of data insert to table'
                 f' <{FxReportSummary.__table_name__}>, start')
        config.connect.save(FxReportSummary.__table_name__, collect_data, config.partition_key)
        log.info(f'fx detail insert over')

    def load_calculate(series):
        if series['CURRENCY'] != 'AUY':
            add_net = (abs(series['LONG_SHORT']) + series['LONG_SHORT']) / 2
            sub_net = (abs(series['LONG_SHORT']) - series['LONG_SHORT']) / 2
            return pd.Series([series['CURRENCY'], add_net, sub_net], index=['CURRENCY', 'ADD_DOMESTIC', 'SUB_DOMESTIC'])
        else:
            return pd.Series([None, None, None], index=['CURRENCY', 'ADD_DOMESTIC', 'SUB_DOMESTIC'])

    new_calculate_data = collect_data.apply(load_calculate, axis=1).dropna()

    if new_calculate_data.shape[0]:
        add_net_number = abs(new_calculate_data['ADD_DOMESTIC'].sum())
        sub_net_number = abs(new_calculate_data['SUB_DOMESTIC'].sum())
        if add_net_number > sub_net_number:
            net = add_net_number
        else:
            net = sub_net_number

        if 'AUY' in collect_data['CURRENCY'].to_list():
            gold_net_position_abs = abs(collect_data[collect_data['CURRENCY'] == 'AUY']['LONG_SHORT'].to_list()[0])
        else:
            gold_net_position_abs = 0
        net_position_abs = sum([gold_net_position_abs, net])
        foreign_exchange_exposure = net_position_abs * 0.08

        end = datetime.datetime.now()
        log.info(f'fx summary PARTITION_KEY-{config.partition_key} count over, runtime {end - start}, '
                 f'number_{foreign_exchange_exposure}')

        value_dict = {
            'C_16': foreign_exchange_exposure
        }

        insert_sql = insert_report(config.partition_key, 'G4C-1', value_dict)
        conn = con()
        cursor = conn.cursor()
        cursor.execute(insert_sql)
        status = 'Y'
        info = 'FX--success'
        return foreign_exchange_exposure
    else:
        log.debug('Fx not data')
        status = 'N'
        info = 'FX--debug:Fx is NULL'

    end_time = datetime.datetime.now()
    data = {
        'name': 'DS_FX_Forward_Basic_Info',
        'partition_key': config.partition_key,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': status
    }
    insert_sql = insert_log(data)
    conn = con()
    cursor = conn.cursor()
    cursor.execute(insert_sql)
