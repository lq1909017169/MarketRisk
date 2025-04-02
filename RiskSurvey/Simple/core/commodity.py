from Simple.handle.commodity import CommodityBaseHandler
from Simple.utils.configuration_table import Config
from Simple.utils.models import CommodityReportDetail, CommodityReportSummary
from Simple.utils.log import log
import datetime
import pandas as pd

from Simple.utils.report_data import insert_report, con, insert_log


def commodity(config: Config):
    start_time = datetime.datetime.now()
    """
    :param config: columns=['MerchandiseType', 'LongShort', 'Notional']
    :return:
    """
    config.connect.clean_table(CommodityReportDetail.__table_name__, config.partition_key)
    config.connect.clean_table(CommodityReportSummary.__table_name__, config.partition_key)

    data = CommodityBaseHandler(config=config).data
    if data.empty:
        log.debug('commodity not data')
        return 0
    else:
        if config.write_to_database:
            log.info(f'commodity detail PARTITION_KEY-{config.partition_key} of data insert to table'
                     f' <{CommodityReportDetail.__table_name__}>, start')
            config.connect.save(CommodityReportDetail.__table_name__, data, config.partition_key)
            log.info(f'commodity detail insert over')

        start = datetime.datetime.now()

        def call(s):
            long_value = s[s['LONG_SHORT_TYPE'] == 'LONG']['MARKET_VALUE'].sum()
            short_value = s[s['LONG_SHORT_TYPE'] == 'SHORT']['MARKET_VALUE'].sum()
            commodity_type = s['COMMODITY_TYPE'].values.tolist()[0]
            return pd.Series([long_value, short_value, config.partition_key, commodity_type],
                             index=['LONG_VALUE', 'SHORT_VALUE', 'PARTITION_KEY', 'COMMODITY_TYPE'])

        data = data.groupby('COMMODITY_TYPE').apply(call)

        def load_position(series):
            if series['COMMODITY_TYPE'] in ['XAG']:
                long_sub_short = abs(series['LONG_VALUE'] - series['SHORT_VALUE'])
                long_add_short = abs(series['LONG_VALUE'] + series['SHORT_VALUE'])
                return pd.Series(
                    [series['LONG_VALUE'], series['SHORT_VALUE'], series['COMMODITY_TYPE'], long_sub_short,
                     long_add_short, 0.15, 0.03, series['PARTITION_KEY']],
                    index=['LONG_VALUE', 'SHORT_VALUE', 'COMMODITY_TYPE', 'NET_VALUE', 'SUM_VALUE', 'NET_VALUE_FACTOR',
                           'SUM_VALUE_FACTOR', 'PARTITION_KEY'])
            else:
                return pd.Series(
                    [None, None, None, None, None, None, None, None],
                    index=['LONG_VALUE', 'SHORT_VALUE', 'COMMODITY_TYPE', 'NET_VALUE', 'SUM_VALUE', 'NET_VALUE_FACTOR',
                           'SUM_VALUE_FACTOR', 'PARTITION_KEY'])

        position_data = data.apply(load_position, axis=1).dropna(how='all').reset_index(drop=True)

        def load_total_amount(series):
            return abs(series['NET_VALUE'] * series['NET_VALUE_FACTOR']) + series['SUM_VALUE'] * series[
                'SUM_VALUE_FACTOR']

        position_data['CAPITAL'] = position_data.apply(load_total_amount, axis=1)
        if config.write_to_database:
            log.info(f'commodity summary PARTITION_KEY-{config.partition_key} of data insert to table'
                     f' <{CommodityReportSummary.__table_name__}>, start')

            config.connect.save(CommodityReportSummary.__table_name__, position_data, config.partition_key)
            log.info(f'commodity summary insert over')

        total_amount_number = position_data['CAPITAL'].sum()
        log.info(f"commodity count over, runtime  {datetime.datetime.now() - start},"
                 f" number_{total_amount_number}")

        value_dict = {
            'C_20': total_amount_number
        }

        insert_sql = insert_report(config.partition_key, 'G4C-1', value_dict)
        conn = con()
        cursor = conn.cursor()
        cursor.execute(insert_sql)

        end_time = datetime.datetime.now()
        data = {
            'name': __name__,
            'partition_key': config.partition_key,
            'info': 'Commodity--success',
            'start_time': start_time,
            'end_time': end_time,
            'status': 'Y'
        }
        insert_sql = insert_log(data)
        conn = con()
        cursor = conn.cursor()
        cursor.execute(insert_sql)

        return total_amount_number
