from Simple.handle.option import OptionHandler
from Simple.utils.configuration_table import Config
from Simple.utils.log import log
from Simple.utils.models import OptionDetail, OptionSummary
from Simple.utils.report_data import insert_log, con
from Simple.utils.utils import empty_df
import pandas as pd
import datetime


def option(config: Config):
    start_time = datetime.datetime.now()
    # config.connect.clean_table(table=OptionDetail.__table_name__, partition_key=config.partition_key)
    # config.connect.clean_table(table=OptionSummary.__table_name__, partition_key=config.partition_key)

    data = OptionHandler(config=config).data
    print(data)
    if config.write_to_database:
        log.info(f'option detail PARTITION_KEY-{config.partition_key} of data insert to table'
                 f' <{OptionDetail.__table_name__}>, start')
        config.connect.save(OptionDetail.__table_name__, data, config.partition_key)
        log.info(f'option detail insert over')

    log.info(f'option summary PARTITION_KEY-{config.partition_key}  count start')
    start = datetime.datetime.now()

    def load_option(series):

        def row_option(s):
            if s['GAMMA']:
                vu = s['VU']
                gamma = 0.5 * s['GAMMA'] * vu * vu
            else:
                gamma = 0
            return pd.Series([gamma], index=['G'])

        # gamma_sum = series['GAMMA'].sum()
        # delta_sum = series['DELTA'].sum()
        # if gamma_sum < 0:
        #     gamma = 0
        # else:
        #     gamma = gamma_sum
        # if delta_sum < 0:
        #     delta = 0
        # else:
        #     delta = delta_sum
        partition_key = series['PARTITION_KEY'].values[0]
        gamma_list = []
        for g in series.apply(row_option, axis=1).to_dict('records'):
            gamma_list.append(g['G'])
        gamma = sum(gamma_list)
        vol_list = []
        vega_list = []
        for v in series['VOL'].to_list():
            if v:
                vol_list.append(v)
        for v in series['VEGA'].to_list():
            if v:
                vega_list.append(v)
        if vol_list:
            vega = 0.25 * vol_list[0] * abs(sum(vega_list))
        else:
            vega = 0
        total_capital_requirement = gamma + vega
        return pd.Series([gamma, vega, total_capital_requirement, partition_key],
                         index=['GAMMA_CAPITAL', 'VEGA_CAPITAL', 'TOTAL', 'PARTITION_KEY'])

    result = data.groupby('CURRENCY').apply(load_option)
    end = datetime.datetime.now()
    log.info(f'option summary PARTITION_KEY-{config.partition_key} count over, runtime {end - start}, number_{result}')

    if result.empty:
        return empty_df(['GAMMA_CAPITAL', 'VEGA_CAPITAL', 'TOTAL', 'PARTITION_KEY']).dropna()
    result = result.reset_index(drop=False)
    if config.write_to_database:
        log.info(f'option summary PARTITION_KEY-{config.partition_key} of data insert to table '
                 f'{OptionSummary.__table_name__}')
        config.connect.save(OptionSummary.__table_name__, result, config.partition_key)
        log.info(f'option summary PARTITION_KEY-{config.partition_key} insert over')

    end_time = datetime.datetime.now()
    data = {
        'name': __name__,
        'partition_key': config.partition_key,
        'info': 'Option--success',
        'start_time': start_time,
        'end_time': end_time,
        'status': 'Y'
    }
    insert_sql = insert_log(data)
    conn = con()
    cursor = conn.cursor()
    cursor.execute(insert_sql)

    return result
