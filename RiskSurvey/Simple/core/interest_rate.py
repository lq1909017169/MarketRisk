from Simple.utils.exchange import Exchange, ExchangeManage
from Simple.utils.log import log
from Simple.utils.models import IrReportDetail, IrReportSummary, IrReportSpecificSummary, BondBasicInfo
from Simple.handle.interest_rate import InterestRateHandler
from Simple.utils.configuration_table import ms_risk_allowance_ratio
from Simple.utils.configuration_table import Config
from Simple.utils.report_data import insert_report, con, insert_log
from Simple.utils.utils import sign, apply_map_callback, empty_df, empty_series
import datetime
import pandas as pd



def rental(data: pd.DataFrame, partition_key: str, currency: str):
    """
    :param data: columns=[ResidualMaturity, Weight, Section, Notional, ContractType, LongShortType]
    :param partition_key: evaluation_date
    :return:
    """
    start = datetime.datetime.now()

    if data.shape[0] == 0:
        columns = ['WEIGHT', 'SECTION', 'LONG', 'SHORT']
        data = {c: [] for c in columns}
        rental_unchecked = pd.DataFrame(data)
    else:
        def load_rental(df):
            def load_weight(series):
                if 'BOND' in series['CONTRACT_TYPE']:
                    if series['LONG_SHORT_TYPE'] == 'LONG':
                        bond_long = series['MARKET_VALUE']
                        bond_short = 0
                        ir_long = 0
                        ir_short = 0
                    else:
                        bond_long = 0
                        bond_short = series['MARKET_VALUE']
                        ir_long = 0
                        ir_short = 0
                else:
                    if series['LONG_SHORT_TYPE'] == 'LONG':
                        bond_long = 0
                        bond_short = 0
                        ir_long = series['MARKET_VALUE']
                        ir_short = 0
                    else:
                        bond_long = 0
                        bond_short = 0
                        ir_long = 0
                        ir_short = series['MARKET_VALUE']
                return pd.Series(
                    [series['WEIGHT'], series['SECTION'], bond_long, bond_short, ir_long, ir_short,
                     series['TIME_FRAME'], series['PARTITION_KEY']],
                    index=['WEIGHT', 'SECTION', 'BOND_LONG', 'BOND_SHORT', 'IR_LONG', 'IR_SHORT', 'TIME_FRAME',
                           'PARTITION_KEY'])

            sort_df = df.apply(load_weight, axis=1)
            return pd.Series([sort_df['WEIGHT'].to_list()[0],
                              sort_df['SECTION'].to_list()[0],
                              sort_df['TIME_FRAME'].to_list()[0],
                              sort_df['PARTITION_KEY'].to_list()[0],
                              sort_df['BOND_LONG'].sum(),
                              sort_df['BOND_SHORT'].sum(),
                              sort_df['IR_LONG'].sum(),
                              sort_df['IR_SHORT'].sum()],
                             index=['WEIGHT', 'SECTION', 'TIME_FRAME', 'PARTITION_KEY', 'BOND_LONG',
                                    'BOND_SHORT', 'IR_LONG', 'IR_SHORT'])

        rental_data = data.groupby('WEIGHT').apply(load_rental)

        def load_collect(series):
            return pd.Series([series['WEIGHT'], series['SECTION'], series['BOND_LONG'] + series['IR_LONG'],
                              series['BOND_SHORT'] + series['IR_SHORT'], series['TIME_FRAME'], series['PARTITION_KEY'],
                              series['BOND_LONG'], series['IR_LONG'], series['BOND_SHORT'], series['IR_SHORT']],
                             index=['WEIGHT', 'SECTION', 'LONG_VALUE', 'SHORT_VALUE', 'TIME_FRAME', 'PARTITION_KEY',
                                    'BOND_LONG_VALUE', 'IR_LONG_VALUE', 'BOND_SHORT_VALUE', 'IR_SHORT_VALUE'])

        rental_unchecked = rental_data.apply(load_collect, axis=1).reset_index(drop=True)
        if rental_unchecked.empty:
            rental_unchecked = empty_df(
                ['WEIGHT', 'SECTION', 'LONG_VALUE', 'SHORT_VALUE', 'TIME_FRAME', 'PARTITION_KEY',
                 'BOND_LONG_VALUE', 'IR_LONG_VALUE', 'BOND_SHORT_VALUE', 'IR_SHORT_VALUE'])
        else:
            sections = set(rental_unchecked['SECTION'].to_list())
            if len(sections) == 3:
                rental_unchecked = rental_unchecked
            else:
                deficiency = {1: [0, 1, 0, 0, 1, partition_key, 0, 0, 0, 0],
                              2: [0.0125, 2, 0, 0, 5, partition_key, 0, 0, 0, 0],
                              3: [0.0275, 3, 0, 0, 8, partition_key, 0, 0, 0, 0]}
                for d in list(deficiency.keys()):
                    if d not in sections:
                        rental_unchecked.loc[len(rental_unchecked.index)] = deficiency[d]
                rental_unchecked = rental_unchecked
    log.info(f'ir general detail to summary runtime,  currency is {currency},  {datetime.datetime.now() - start}')
    rental_unchecked['CURRENCY'] = rental_unchecked.apply(lambda _: currency, axis=1)
    return rental_unchecked


def general(data):
    """
    :param data: columns=[Long, Short, Weight, Section]
    :return:
    """
    start = datetime.datetime.now()
    if data.shape[0] == 0:
        general_value = 0
    else:
        def risk_weighted_position_in_time_frame(series):
            """
            生成风险加权头寸和时段内数据
            """
            long = series['LONG_VALUE'] * series['WEIGHT']
            short = series['SHORT_VALUE'] * series['WEIGHT']

            return pd.Series([long, short, series['SECTION'], min(long, short), long - short],
                             index=['LONG_VALUE', 'SHORT_VALUE', 'SECTION', 'IN_TIME_FRAME_MATCHED',
                                    'IN_TIME_FRAME_UNMATCHED'])

        risk_weighted_position_in_time_frame_data = data.apply(risk_weighted_position_in_time_frame, axis=1)

        def in_time_zone(series):
            """
            时区内
            """
            unmatched = series['IN_TIME_FRAME_UNMATCHED'].to_list()
            small_unmatched = []
            big_unmatched = []
            for u in unmatched:
                if u < 0:
                    small_unmatched.append(abs(u))
                elif u > 0:
                    big_unmatched.append(u)
                else:
                    pass
            return pd.Series(
                [min(sum(big_unmatched), sum(small_unmatched)), sum(unmatched), series['SECTION'].to_list()[0]],
                index=['IN_TIME_ZONE_MATCHED', 'IN_TIME_ZONE_UNMATCHED', 'SECTION'])

        in_time_zone_data = risk_weighted_position_in_time_frame_data.groupby('SECTION').apply(in_time_zone)

        in_time_zone_map = in_time_zone_data.to_dict('records')
        in_time_zone_unmatched = {}
        for in_time_zone in in_time_zone_map:
            in_time_zone_unmatched[in_time_zone['SECTION']] = in_time_zone['IN_TIME_ZONE_UNMATCHED']
        if (in_time_zone_unmatched[1] > 0 > in_time_zone_unmatched[2]) or (
                in_time_zone_unmatched[1] < 0 < in_time_zone_unmatched[2]):
            zone_1_2 = min(abs(in_time_zone_unmatched[1]), abs(in_time_zone_unmatched[2]))
        else:
            zone_1_2 = 0

        if (in_time_zone_unmatched[2] > 0 > in_time_zone_unmatched[3]) or (
                in_time_zone_unmatched[2] < 0 < in_time_zone_unmatched[3]):
            if zone_1_2 == 0:
                zone_2_3 = min(abs(in_time_zone_unmatched[2]), abs(in_time_zone_unmatched[3]))
            else:
                if abs(in_time_zone_unmatched[2]) > abs(in_time_zone_unmatched[1]):
                    zone_2_3 = min(abs(abs(in_time_zone_unmatched[1]) + abs(in_time_zone_unmatched[2])),
                                   abs(in_time_zone_unmatched[3]))
                else:
                    zone_2_3 = 0
        else:
            zone_2_3 = 0

        sign_time_zone_value = {k: sign(v) for k, v in in_time_zone_unmatched.items()}
        if (sign_time_zone_value[1] == sign_time_zone_value[2]) and (
                sign_time_zone_value[2] == sign_time_zone_value[3]):
            time_zone_quota1 = 0
        else:
            if sign_time_zone_value[1] == sign_time_zone_value[2]:
                time_zone_quota1 = 0
            else:
                time_zone_quota1 = min(abs(in_time_zone_unmatched[1]), abs(in_time_zone_unmatched[2]))

        if (sign_time_zone_value[1] == sign_time_zone_value[2]) and (
                sign_time_zone_value[2] == sign_time_zone_value[3]):
            time_zone_quota2 = 0
        else:
            if sign_time_zone_value[1] == sign_time_zone_value[2]:
                time_zone_quota2 = min(abs(in_time_zone_unmatched[3]), abs(in_time_zone_unmatched[2]))
            else:
                if sign(in_time_zone_unmatched[1] + in_time_zone_unmatched[2]) == sign_time_zone_value[3]:
                    time_zone_quota2 = 0
                else:
                    if sign_time_zone_value[2] == sign_time_zone_value[3]:
                        time_zone_quota2 = 0
                    else:
                        time_zone_quota2 = min(abs(in_time_zone_unmatched[1] + in_time_zone_unmatched[2]),
                                               abs(in_time_zone_unmatched[3]))
        time_zone_quota3 = 0 - (abs(in_time_zone_unmatched[1] + in_time_zone_unmatched[2] + in_time_zone_unmatched[3]
                                    ) - abs(in_time_zone_unmatched[1]) - abs(in_time_zone_unmatched[2]) - abs(
            in_time_zone_unmatched[3])) / 2 - time_zone_quota1 - time_zone_quota2

        def vertical_capital_sum(series):
            return min(series['LONG_VALUE'], series['SHORT_VALUE'])

        vertical_capital_sum_number = risk_weighted_position_in_time_frame_data.apply(vertical_capital_sum,
                                                                                      axis=1).sum() * 0.1
        in_time_zone_matched = {}
        for in_time_zone in in_time_zone_map:
            in_time_zone_matched[in_time_zone['SECTION']] = in_time_zone['IN_TIME_ZONE_MATCHED']
        time_zone_one = in_time_zone_matched[1] * 0.4
        time_zone_two = in_time_zone_matched[2] * 0.3
        time_zone_three = in_time_zone_matched[3] * 0.3
        time_zone_1_2 = zone_1_2 * 0.4
        time_zone_2_3 = zone_2_3 * 0.4
        time_zone_1_3 = time_zone_quota3
        net_asset_value = abs(risk_weighted_position_in_time_frame_data['LONG_VALUE'].sum() -
                              risk_weighted_position_in_time_frame_data['SHORT_VALUE'].sum())
        general_value = sum([vertical_capital_sum_number, time_zone_one, time_zone_two, time_zone_three, time_zone_1_2,
                             time_zone_2_3, time_zone_1_3, net_asset_value])
    log.info(f'ir general count over, runtime__{datetime.datetime.now() - start}, number_{general_value}')

    return general_value


def specific(data: pd.DataFrame, config: Config) -> [int, float]:
    data = data[(data['RISK_TYPE'] != 'IR')]
    start = datetime.datetime.now()


    def load_specific(series):

        # if series['MARKET_VALUE']:
        if series['RISK_TYPE'] == '合格证券' and series['RATING'] in \
                ['BB+', 'BB', 'BB-', 'B+', 'B', 'B-', '未评级', '其他']:
            risk = '其他证券'
            rating = series['RATING']
        else:
            risk = series['RISK_TYPE']
            rating = series['RATING']
        if risk == '其他证券':
            rwa_rating = config.connect.verify(config.connect.query_one(
                BondBasicInfo, BOND_NAME=series[BondBasicInfo.BOND_NAME],
                PARTITION_KEY=config.partition_key))[0]['RWA_RATING']
            try:
                ratio = ms_risk_allowance_ratio(
                    risk, rating, series['RESIDUAL_MATURITY'], rwa_rating=rwa_rating)
            except BaseException as e:
                log.error(f'{series["BOND_NAME"]}, bond is not rwa_weight : {e}')
                return empty_series(['RISK_TYPE', 'RATING', 'TRADE_ID', 'PARTITION_KEY', 'SPECIFIC_VALUE', 'RATIO',
                                     'BOND_NAME', 'LONG_SHORT_TYPE', 'MARKET_VALUE'])
        else:
            ratio = ms_risk_allowance_ratio(
                risk, rating, series['RESIDUAL_MATURITY'], rwa_rating=0)
        if series['MARKET_VALUE']:
            specific_number = ratio * series['MARKET_VALUE']
        else:
            specific_number = 0
        return pd.Series([risk, rating, series['TRADE_ID'], series['PARTITION_KEY'], specific_number, ratio,
                          series['BOND_NAME'], series['LONG_SHORT_TYPE'], series['MARKET_VALUE']],
                         index=['RISK_TYPE', 'RATING', 'TRADE_ID', 'PARTITION_KEY', 'SPECIFIC_VALUE', 'RATIO',
                                'BOND_NAME', 'LONG_SHORT_TYPE', 'MARKET_VALUE'])

    if data.shape[0] == 0:
        log.info(f"ir specific count over, runtime  {datetime.datetime.now() - start},"
                 f" number_0")
        return 0
    else:
        specifics_data = data.apply(load_specific, axis=1).dropna(how='all')
        log.info(f"ir specific count over, runtime  {datetime.datetime.now() - start},"
                 f" number_{sum(specifics_data['SPECIFIC_VALUE'])}")

        if config.write_to_database:
            log.info(f'ir specific summary PARTITION_KEY-{config.partition_key} of data insert to table'
                     f' <{IrReportSpecificSummary.__table_name__}>, start')

            config.connect.save(IrReportSpecificSummary.__table_name__, specifics_data, config.partition_key)
            log.info(f'ir specific summary insert over')
        return sum(specifics_data['SPECIFIC_VALUE'])


def ir(config: Config):
    start_time = datetime.datetime.now()
    config.connect.clean_table(table=IrReportSummary.__table_name__, partition_key=config.partition_key)
    config.connect.clean_table(table=IrReportDetail.__table_name__, partition_key=config.partition_key)

    ir_data = InterestRateHandler(config=config).data.applymap(apply_map_callback)
    data = ir_data[['TRADE_ID', 'RESIDUAL_MATURITY', 'RISK_TYPE', 'RATING', 'COUPON_RATE', 'LONG_SHORT_TYPE',
                    'CONTRACT_TYPE', 'WEIGHT', 'SECTION', 'TIME_FRAME', 'CURRENCY', 'IS_DOMESTIC',
                    'PARTITION_KEY', 'SPECIFIC_RISK', 'START_DATE', 'END_DATE', 'MARKET_VALUE', 'BOND_NAME',
                    'BOND_TYPE', 'BUSINESS_CLASS']]

    exchange_manage = ExchangeManage(config=config)

    def exchange(s):
        currency_map = {
            'HKD': 'HKD',
            'XAU': 'XAU',
            'JPY': 'JPY',
            'AUY': 'AU99.99',
            'AU99.99': 'AU99.99',
            'XAG': 'XAG',
            'CAD': 'CAD',
            'USD': 'USD',
            'GBP': 'GBP',
            'CNY': 'CNY',
            'AUD': 'AUD',
            'EUR': 'EUR',
        }
        if s['CURRENCY']:
            if s['CURRENCY'] in ['HKD', 'JPY', 'AUY', 'AU99.99', 'CAD', 'USD', 'GBP', 'AUD', 'EUR']:
                # if s['CURRENCY'] != config.currency and s['CURRENCY'] != 'XAG' and s['CURRENCY'] == 'XAU':
                s['MARKET_VALUE'] = Exchange(exchange_manage=exchange_manage,
                                             target_currency=currency_map[s['CURRENCY']],
                                             source_currency=config.currency,
                                             foreign=s['MARKET_VALUE']).rate()
            elif s['CURRENCY'] == 'XAG':
                rate = Exchange(exchange_manage=exchange_manage,
                                target_currency=currency_map[s['CURRENCY']],
                                source_currency='USD',
                                foreign=s['MARKET_VALUE']).rate()
                s['MARKET_VALUE'] = Exchange(exchange_manage=exchange_manage,
                                             target_currency='USD',
                                             source_currency=config.currency,
                                             foreign=rate).rate()
            elif s['CURRENCY'] == 'XAU':
                rate = Exchange(exchange_manage=exchange_manage,
                                target_currency=currency_map[s['CURRENCY']],
                                source_currency='USD',
                                foreign=s['MARKET_VALUE']).rate()
                s['MARKET_VALUE'] = Exchange(exchange_manage=exchange_manage,
                                             target_currency='USD',
                                             source_currency=config.currency,
                                             foreign=rate).rate()
            else:
                s = s
        else:
            log.error(f"data in Currency_data is None, data_trade_id: {s['TRADE_ID']}")
            s = empty_series(['TRADE_ID', 'RESIDUAL_MATURITY', 'RISK_TYPE', 'RATING', 'COUPON_RATE', 'LONG_SHORT_TYPE',
                              'CONTRACT_TYPE', 'WEIGHT', 'SECTION', 'TIME_FRAME', 'CURRENCY', 'IS_DOMESTIC',
                              'PARTITION_KEY', 'SPECIFIC_RISK', 'START_DATE', 'END_DATE', 'MARKET_VALUE', 'BOND_NAME',
                              'BOND_TYPE', 'BUSINESS_CLASS'])
        return s

    data = data.apply(exchange, axis=1)
    data = data.dropna(how='all')
    data = data.applymap(apply_map_callback)
    log.info(f'exchange rate to data, rows__{data.shape[0]}')
    data['PARTITION_KEY'] = data['PARTITION_KEY'].apply(lambda x: str(x).split('.')[0])

    if config.write_to_database:
        log.info(f'ir detail PARTITION_KEY-{config.partition_key} of data insert to table'
                 f' <{IrReportDetail.__table_name__}>, start')

        config.connect.save(IrReportDetail.__table_name__, data, config.partition_key)
        log.info(f'ir detail insert over')


    s = specific(data=data, config=config)

    value_dict = {
        'C_7': s
    }

    insert_sql = insert_report(config.partition_key, 'G4C-1', value_dict)
    conn = con()
    cursor = conn.cursor()
    cursor.execute(insert_sql)

    rental_list = []
    currency_list = set(data['CURRENCY'].values.tolist())
    for currency in currency_list:
        df = data[data['CURRENCY'] == currency]
        ren = rental(df, config.partition_key, currency)
        rental_list.append(ren)

    if rental_list:
        rental_data = pd.concat(rental_list, axis=0).reset_index(drop=True)
        if config.write_to_database:
            log.info(f'ir general detail PARTITION_KEY-{config.partition_key} of data insert to table'
                     f' <{IrReportSummary.__table_name__}>, start')

            config.connect.save(IrReportSummary.__table_name__, rental_data, config.partition_key)

            log.info(f'ir general detail insert over')
            status = "Y"
            info = 'IR--success'
    else:
        log.debug('Ir not data')
        status = "Y"
        info = 'IR--debug:Ir is NULL'

    end_time = datetime.datetime.now()
    data = {
        'name': __name__,
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



def rental_cls(data: pd.DataFrame, partition_key: str):
    """
    :param data: columns=[ResidualMaturity, Weight, Section, Notional, ContractType, LongShortType]
    :param partition_key: evaluation_date
    :return:
    """
    if data.shape[0] == 0:
        columns = ['WEIGHT', 'SECTION', 'LONG', 'SHORT']
        data = {c: [] for c in columns}
        rental_unchecked = pd.DataFrame(data)
    else:
        def load_rental(df):
            def load_weight(series):
                if 'BOND' in series['CONTRACT_TYPE']:
                    if series['LONG_SHORT_TYPE'] == 'LONG':
                        bond_long = series['MARKET_VALUE']
                        bond_short = 0
                        ir_long = 0
                        ir_short = 0
                    else:
                        bond_long = 0
                        bond_short = series['MARKET_VALUE']
                        ir_long = 0
                        ir_short = 0
                else:
                    if series['LONG_SHORT_TYPE'] == 'LONG':
                        bond_long = 0
                        bond_short = 0
                        ir_long = series['MARKET_VALUE']
                        ir_short = 0
                    else:
                        bond_long = 0
                        bond_short = 0
                        ir_long = 0
                        ir_short = series['MARKET_VALUE']
                return pd.Series(
                    [series['WEIGHT'], series['SECTION'], bond_long, bond_short, ir_long, ir_short,
                     series['TIME_FRAME'], series['PARTITION_KEY']],
                    index=['WEIGHT', 'SECTION', 'BOND_LONG', 'BOND_SHORT', 'IR_LONG', 'IR_SHORT', 'TIME_FRAME',
                           'PARTITION_KEY'])

            sort_df = df.apply(load_weight, axis=1)
            return pd.Series([sort_df['WEIGHT'].to_list()[0],
                              sort_df['SECTION'].to_list()[0],
                              sort_df['TIME_FRAME'].to_list()[0],
                              sort_df['PARTITION_KEY'].to_list()[0],
                              sort_df['BOND_LONG'].sum(),
                              sort_df['BOND_SHORT'].sum(),
                              sort_df['IR_LONG'].sum(),
                              sort_df['IR_SHORT'].sum()],
                             index=['WEIGHT', 'SECTION', 'TIME_FRAME', 'PARTITION_KEY', 'BOND_LONG',
                                    'BOND_SHORT', 'IR_LONG', 'IR_SHORT'])

        rental_data = data.groupby('WEIGHT').apply(load_rental)

        def load_collect(series):
            return pd.Series([series['WEIGHT'], series['SECTION'], series['BOND_LONG'] + series['IR_LONG'],
                              series['BOND_SHORT'] + series['IR_SHORT'], series['TIME_FRAME'], series['PARTITION_KEY'],
                              series['BOND_LONG'], series['IR_LONG'], series['BOND_SHORT'], series['IR_SHORT']],
                             index=['WEIGHT', 'SECTION', 'LONG_VALUE', 'SHORT_VALUE', 'TIME_FRAME', 'PARTITION_KEY',
                                    'BOND_LONG_VALUE', 'IR_LONG_VALUE', 'BOND_SHORT_VALUE', 'IR_SHORT_VALUE'])

        rental_unchecked = rental_data.apply(load_collect, axis=1).reset_index(drop=True)
        if rental_unchecked.empty:
            rental_unchecked = empty_df(
                ['WEIGHT', 'SECTION', 'LONG_VALUE', 'SHORT_VALUE', 'TIME_FRAME', 'PARTITION_KEY',
                 'BOND_LONG_VALUE', 'IR_LONG_VALUE', 'BOND_SHORT_VALUE', 'IR_SHORT_VALUE'])
        else:
            sections = set(rental_unchecked['SECTION'].to_list())
            if len(sections) == 3:
                rental_unchecked = rental_unchecked
            else:
                deficiency = {1: [0, 1, 0, 0, 1, partition_key, 0, 0, 0, 0],
                              2: [0.0125, 2, 0, 0, 5, partition_key, 0, 0, 0, 0],
                              3: [0.0275, 3, 0, 0, 8, partition_key, 0, 0, 0, 0]}
                for d in list(deficiency.keys()):
                    if d not in sections:
                        rental_unchecked.loc[len(rental_unchecked.index)] = deficiency[d]
                rental_unchecked = rental_unchecked
    return rental_unchecked


def ir_cls(config: Config):
    ir_data = InterestRateHandler(config=config).data.applymap(apply_map_callback)
    data = ir_data[['TRADE_ID', 'RESIDUAL_MATURITY', 'RISK_TYPE', 'RATING', 'COUPON_RATE', 'LONG_SHORT_TYPE',
                    'CONTRACT_TYPE', 'WEIGHT', 'SECTION', 'TIME_FRAME', 'CURRENCY', 'IS_DOMESTIC',
                    'PARTITION_KEY', 'SPECIFIC_RISK', 'START_DATE', 'END_DATE', 'MARKET_VALUE', 'BOND_NAME',
                    'BOND_TYPE', 'BUSINESS_CLASS']]

    exchange_manage = ExchangeManage(config=config)

    def exchange(s):
        currency_map = {
            'HKD': 'HKD',
            'XAU': 'XAU',
            'JPY': 'JPY',
            'AUY': 'AU99.99',
            'AU99.99': 'AU99.99',
            'XAG': 'XAG',
            'CAD': 'CAD',
            'USD': 'USD',
            'GBP': 'GBP',
            'CNY': 'CNY',
            'AUD': 'AUD',
            'EUR': 'EUR'
        }
        log.info(s)
        if s['CURRENCY']:
            if s['CURRENCY'] in ['HKD', 'JPY', 'AUY', 'AU99.99', 'CAD', 'USD', 'GBP', 'AUD', 'EUR']:
                # if s['CURRENCY'] != config.currency and s['CURRENCY'] != 'XAG' and s['CURRENCY'] == 'XAU':
                s['MARKET_VALUE'] = Exchange(exchange_manage=exchange_manage,
                                             target_currency=currency_map[s['CURRENCY']],
                                             source_currency=config.currency,
                                             foreign=s['MARKET_VALUE']).rate()
            elif s['CURRENCY'] == 'XAG':
                rate = Exchange(exchange_manage=exchange_manage,
                                target_currency=currency_map[s['CURRENCY']],
                                source_currency='USD',
                                foreign=s['MARKET_VALUE']).rate()
                s['MARKET_VALUE'] = Exchange(exchange_manage=exchange_manage,
                                             target_currency='USD',
                                             source_currency=config.currency,
                                             foreign=rate).rate()
            elif s['CURRENCY'] == 'XAU':
                rate = Exchange(exchange_manage=exchange_manage,
                                target_currency=currency_map[s['CURRENCY']],
                                source_currency='USD',
                                foreign=s['MARKET_VALUE']).rate()
                s['MARKET_VALUE'] = Exchange(exchange_manage=exchange_manage,
                                             target_currency='USD',
                                             source_currency=config.currency,
                                             foreign=rate).rate()
            else:
                s = s
            print(s)
        else:
            log.error(f"data in Currency_data is None, data_trade_id: {s['TRADE_ID']}")
            s = empty_series(['TRADE_ID', 'RESIDUAL_MATURITY', 'RISK_TYPE', 'RATING', 'COUPON_RATE', 'LONG_SHORT_TYPE',
                              'CONTRACT_TYPE', 'WEIGHT', 'SECTION', 'TIME_FRAME', 'CURRENCY', 'IS_DOMESTIC',
                              'PARTITION_KEY', 'SPECIFIC_RISK', 'START_DATE', 'END_DATE', 'MARKET_VALUE', 'BOND_NAME',
                              'BOND_TYPE', 'BUSINESS_CLASS'])
        return s

    data = data.apply(exchange, axis=1)
    data = data.dropna(how='all')
    data = data.applymap(apply_map_callback)
    data['PARTITION_KEY'] = data['PARTITION_KEY'].apply(lambda x: str(x).split('.')[0])
    re_data = rental_cls(data, config.partition_key)
    g = general(data=re_data)

    value_dict = {
        'C_6': g,
    }

    insert_sql = insert_report(config.partition_key, 'G4C-1', value_dict)
    conn = con()
    cursor = conn.cursor()
    cursor.execute(insert_sql)

    # return g, s
