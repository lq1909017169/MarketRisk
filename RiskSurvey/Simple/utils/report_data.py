from configparser import ConfigParser
import jaydebeapi
import pandas as pd
import datetime
import warnings
import traceback
import uuid
import os
from Simple.utils.log import log

warnings.filterwarnings('ignore')

def con():
    config = ConfigParser()
    config.read(os.path.split(os.path.abspath(__file__))[0] + '/setting.ini', encoding='utf-8')
    ob_connect = {k: v for k, v in config.items('OB_CONNECT')}
    url, user, password, driver, jarFile = ob_connect['url'], ob_connect['user'], ob_connect['password'], ob_connect[
        'driver'], ob_connect['jar']
    conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
    # cursor = conn.cursor()
    return conn


def insert_report(partition_key, SHEET_NAME, data: dict):
    insert_sql = f"INSERT INTO RES_REPORT_DATA(PARTITION_KEY, ID, FAMILY, REP_NAME, SHEET_NAME, ROW_DES, COL_DES, VALUE) VALUES "

    values_list = []
    for key, value in data.items():
        COL_DES, ROW_DES = key.split('_')
        values = f"('{partition_key}', '{str(uuid.uuid4()).replace('-', '')}', 'SOLO', '{SHEET_NAME}', '{SHEET_NAME}', '{ROW_DES}', '{COL_DES}', '{value}')"
        values_list.append(values)
    insert_sql = insert_sql + ','.join(values_list)
    return insert_sql


def insert_log(data):
    data['start_time'] = data['start_time'].strftime("%Y-%m-%d %H:%M:%S")
    data['end_time'] = data['end_time'].strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
    insert into RWA_PYTHON_LOG(OBJECT_OWN, OBJECT_CTG, OBJECT_NAME, RUN_DATE, PRO_DESC, START_TIME, END_TIME, FLAG) 
    VALUES ('RWA', 'NRWA_MARKET_TASK', '{data['name']}', '{data['partition_key']}', '{data['info']}', 
    '{data['start_time']}', '{data['end_time']}', '{data['status']}')
    """
    return sql


def IR(run_date):
    log.info('IR Update-start')
    conn = con()
    cursor = conn.cursor()
    currency_sql = f"select DISTINCT CURRENCY from DS_IR_REPORT_SUMMARY WHERE PARTITION_KEY={run_date}"
    df_cuy = pd.read_sql(sql=currency_sql, con=conn)
    result_data = {}
    # for cuy in ['AUY']:
    for cuy in df_cuy['CURRENCY']:
        IR_select_sql = f"select * from DS_IR_REPORT_SUMMARY WHERE PARTITION_KEY={run_date} AND CURRENCY='{cuy}'"
        df = pd.read_sql(sql=IR_select_sql, con=conn)
        # IR_rate_sql = f"select * from DS_IR_REPORT_SUMMARY WHERE PARTITION_KEY={run_date} AND CURRENCY='{cuy}'"
        value_dict = {}
        for time_value in range(7, 22):
            if 7 <= time_value < 11:
                SECTION = 1
            elif 11 <= time_value < 14:
                SECTION = 2
            elif 14 <= time_value < 22:
                SECTION = 3

            TIME_FRAME = time_value - 6
            for row, row_value in {'F': 'BOND_LONG_VALUE', 'G': 'BOND_SHORT_VALUE',
                                   'H': 'IR_LONG_VALUE', 'I': 'IR_SHORT_VALUE'}.items():
                try:
                    value = df[(df['SECTION'] == SECTION) & (df['TIME_FRAME'] == TIME_FRAME)][row_value].values[0]
                except:
                    value = 0
                value_dict[row + '_' + str(time_value)] = value
        insert_sql = insert_report(run_date, f'G4C-1(b)-{cuy}', value_dict)
        cursor.execute(insert_sql)


def IR_SPECIFIC(run_date):
    log.info(f'IR_SPECIFIC Update-start')
    conn = con()
    cursor = conn.cursor()
    log.info('IR_SPECIFIC Update-start')
    IR_SPECIFIC_select_sql = f"select * from DS_IR_REPORT_SPECIFIC_SUMMARY WHERE PARTITION_KEY={run_date}"
    df = pd.read_sql(sql=IR_SPECIFIC_select_sql, con=conn)
    df['MARKET_VALUE'] = df['MARKET_VALUE'].astype('float')
    df['SPECIFIC_VALUE'] = df['SPECIFIC_VALUE'].astype('float')
    df['RATIO'] = df['RATIO'].astype('float')
    df_zf = df[df['RISK_TYPE'] == '政府证券']
    df_zf_long = df_zf[df_zf['LONG_SHORT_TYPE'] == 'LONG']

    data = {'C_8': df_zf_long[
        (df_zf_long['RATING'].isin(['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-'])) & (df_zf_long['RATIO'] == 0)][
        'MARKET_VALUE'].sum(),
            'D_9': df_zf_long[
                (df_zf_long['RATING'].isin(['A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'])) & (
                        df_zf_long['RATIO'] == 0.0025)][
                'MARKET_VALUE'].sum(),
            'E_9': df_zf_long[
                (df_zf_long['RATING'].isin(['A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'])) & (df_zf_long['RATIO'] == 0.01)][
                'MARKET_VALUE'].sum(),
            'F_9': df_zf_long[
                (df_zf_long['RATING'].isin(['A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'])) & (df_zf_long['RATIO'] == 0.016)][
                'MARKET_VALUE'].sum(),
            'G_10': df_zf_long[
                (df_zf_long['RATING'].isin(['BB+', 'BB', 'BB-', 'B+', 'B', 'B-'])) & (df_zf_long['RATIO'] == 0.08)][
                'MARKET_VALUE'].sum(),
            'G_12': df_zf_long[(df_zf_long['RATING'].isin(['未评级'])) & (df_zf_long['RATIO'] == 0.08)][
                'MARKET_VALUE'].sum(),
            'H_11': df_zf_long[(df_zf_long['RATING'].isin(['CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C+', 'C', 'C-',
                                                           'DDD+', 'DDD', 'DDD-', 'DD+', 'DD', 'DD-', 'D+', 'D',
                                                           'D-'])) & (df_zf_long['RATIO'] == 0.12)][
                'MARKET_VALUE'].sum()}
    df_zf_short = df_zf[df_zf['LONG_SHORT_TYPE'] == 'SHORT']
    data.update({
        'C_14': df_zf_short[
            (df_zf_short['RATING'].isin(['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-'])) & (df_zf_short['RATIO'] == 0)][
            'MARKET_VALUE'].sum(),
        'D_15': df_zf_short[
            (df_zf_short['RATING'].isin(['A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'])) & (df_zf_short['RATIO'] == 0.0025)][
            'MARKET_VALUE'].sum(),
        'E_15': df_zf_short[
            (df_zf_short['RATING'].isin(['A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'])) & (df_zf_short['RATIO'] == 0.01)][
            'MARKET_VALUE'].sum(),
        'F_15': df_zf_short[
            (df_zf_short['RATING'].isin(['A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'])) & (df_zf_short['RATIO'] == 0.016)][
            'MARKET_VALUE'].sum(),
        'G_16': df_zf_short[
            (df_zf_short['RATING'].isin(['BB+', 'BB', 'BB-', 'B+', 'B', 'B-'])) & (df_zf_short['RATIO'] == 0.08)][
            'MARKET_VALUE'].sum(),
        'G_18': df_zf_short[(df_zf_short['RATING'].isin(['未评级'])) & (df_zf_short['RATIO'] == 0.08)][
            'MARKET_VALUE'].sum(),
        'H_17': df_zf_short[(df_zf_short['RATING'].isin(['CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C+', 'C', 'C-',
                                                         'DDD+', 'DDD', 'DDD-', 'DD+', 'DD', 'DD-', 'D+', 'D',
                                                         'D-'])) & (df_zf_short['RATIO'] == 0.12)][
            'MARKET_VALUE'].sum()
    })
    # 合格证券
    df_true = df[df['RISK_TYPE'] == '合格证券']
    df_true_long = df_true[df_true['LONG_SHORT_TYPE'] == 'LONG']

    data.update({
        'D_20': df_true_long[df_true_long['RATIO'] == 0.0025]['MARKET_VALUE'].sum(),
        'E_20': df_true_long[df_true_long['RATIO'] == 0.01]['MARKET_VALUE'].sum(),
        'F_20': df_true_long[df_true_long['RATIO'] == 0.016]['MARKET_VALUE'].sum(),
    })

    df_true_short = df_true[df_true['LONG_SHORT_TYPE'] == 'SHORT']

    data.update({
        'D_21': df_true_short[df_true_short['RATIO'] == 0.0025]['MARKET_VALUE'].sum(),
        'E_21': df_true_short[df_true_short['RATIO'] == 0.01]['MARKET_VALUE'].sum(),
        'F_21': df_true_short[df_true_short['RATIO'] == 0.016]['MARKET_VALUE'].sum(),
    })
    # 其他证券
    df_other = df[df['RISK_TYPE'] == '其他证券']

    df_other_long = df_other[df_other['LONG_SHORT_TYPE'] == 'LONG']
    data.update({
        'G_23': df_other_long[df_other_long['RATIO'] == 0.08]['MARKET_VALUE'].sum(),
        'H_23': df_other_long[df_other_long['RATIO'] == 0.12]['MARKET_VALUE'].sum(),
        'I_23': df_other_long[~df_other_long['RATIO'].isin([0.08, 0.12])][
            'MARKET_VALUE'].sum(),
    })

    df_other_short = df_other[df_other['LONG_SHORT_TYPE'] == 'SHORT']

    data.update({
        'G_24': df_other_short[df_other_short['RATIO'] == 0.08]['MARKET_VALUE'].sum(),
        'H_24': df_other_short[df_other_short['RATIO'] == 0.12]['MARKET_VALUE'].sum(),
        'I_24': df_other_short[~df_other_short['RATIO'].isin([0.08, 0.12])][
            'MARKET_VALUE'].sum(),
    })
    # 资本总额
    # sum_value = (data['I_23'] + data['I_24']) * 0.02
    # SPECIFIC_VALUE
    I_29 = (df_other_long[~df_other_long['RATIO'].isin([0.08, 0.12])][
            'SPECIFIC_VALUE'].sum()) + (df_other_short[~df_other_short['RATIO'].isin(
            [0.08, 0.12])]['SPECIFIC_VALUE'].sum())
    data.update({
        'I_29': I_29
    })

    # data.update({
    #     'I_29': sum_value
    # })
    insert_sql = insert_report(run_date, 'G4C-1(a)', data)
    cursor.execute(insert_sql)
    log.info(f'IR_SPECIFIC Update-end')


def FX_REPORT(run_date):
    conn = con()
    cursor = conn.cursor()
    log.info('FX_REPORT Update-start')
    data = {}
    for ccy, cow in {'USD': '6', 'EUR': '7', 'JPY': '8', 'GBP': '9', 'HKD': '10', 'CHF': '11',
                     'AUD': '12', 'CAD': '13', 'SGD': '14', 'AUY': '17', 'other_long': '15',
                     'other_short': '16'}.items():
        if ccy in ['other_long', 'other_short']:
            data.update({
                'C' + '_' + cow: 0,
                'D' + '_' + cow: 0,
                'E' + '_' + cow: 0,
                'F' + '_' + cow: 0,
                'G' + '_' + cow: 0,
                'H' + '_' + cow: 0,
            })
            continue
        FX_REPORT_sql = f"select * from DS_FX_REPORT_DETAIL WHERE PARTITION_KEY={run_date} AND　CURRENCY='{ccy}'"
        df = pd.read_sql(sql=FX_REPORT_sql, con=conn)
        columns = ['LONG_SHORT_DOMESTIC', 'DELTA_DOMESTIC', 'OPEN_POSITION_DOMESTIC',
                   'LONG_SHORT_NOT_DOMESTIC', 'DELTA_NOT_DOMESTIC', 'OPEN_POSITION_NOT_DOMESTIC']
        df[columns] = df[columns].astype(float)

        data.update({
            'C' + '_' + cow: df['LONG_SHORT_DOMESTIC'].sum(),
            'D' + '_' + cow: df['DELTA_DOMESTIC'].sum(),
            'E' + '_' + cow: df['OPEN_POSITION_DOMESTIC'].sum(),
            'F' + '_' + cow: df['LONG_SHORT_NOT_DOMESTIC'].sum(),
            'G' + '_' + cow: df['DELTA_NOT_DOMESTIC'].sum(),
            'H' + '_' + cow: df['OPEN_POSITION_NOT_DOMESTIC'].sum(),
        })
    insert_sql = insert_report(run_date, 'G4C-1(e)', data)
    cursor.execute(insert_sql)
    log.info(f'FX_REPORT Update-end')


def OPTION_SUMMARY(run_date):
    conn = con()
    cursor = conn.cursor()
    log.info('OPTION_SUMMARY Update-start')
    COMMODITY_OPTION_select_sql = f"select * from DS_OPTION_SUMMARY WHERE PARTITION_KEY={run_date}"
    df = pd.read_sql(sql=COMMODITY_OPTION_select_sql, con=conn)
    data = {}
    if len(df):
        for index in range(len(df)):
            cow = str(index + 6)
            data['B' + '_' + cow] = df.loc[index]['CURRENCY']
            data['C' + '_' + cow] = df.loc[index]['GAMMA_CAPITAL'] / 10000
            data['D' + '_' + cow] = df.loc[index]['VEGA_CAPITAL'] / 10000
            data['E' + '_' + cow] = df.loc[index]['TOTAL'] / 10000
    else:
        data['B_6'] = 0
        data['C_6'] = 0
        data['D_6'] = 0
        data['E_6'] = 0
    insert_sql = insert_report(run_date, 'G4C-1(k)', data)
    cursor.execute(insert_sql)
    log.info(f'OPTION_SUMMARY Update-end')


def COMMODITY_REPORT_SUMMARY(run_date):
    conn = con()
    cursor = conn.cursor()
    log.info('COMMODITY_REPORT_SUMMARY Update-start')
    COMMODITY_OPTION_select_sql = f"select * from DS_COMMODITY_REPORT_SUMMARY WHERE PARTITION_KEY={run_date}"
    df = pd.read_sql(sql=COMMODITY_OPTION_select_sql, con=conn)
    data = {}
    if len(df):
        for index in range(len(df)):
            cow = str(index + 7)
            data['B' + '_' + cow] = df.loc[index]['COMMODITY_TYPE']
            data['C' + '_' + cow] = df.loc[index]['LONG_VALUE']
            data['D' + '_' + cow] = df.loc[index]['SHORT_VALUE']
    else:
        data['B_7'] = 0
        data['C_7'] = 0
        data['D_7'] = 0
    insert_sql = insert_report(run_date, 'G4C-1(f)', data)
    cursor.execute(insert_sql)
    log.info(f'COMMODITY_REPORT_SUMMARY Update-end')


# def G4C1(run_date):


def start(run_date):
    start_t = datetime.datetime.now()
    try:
        log.info(f'Report_Data-update start')
        start_time = datetime.datetime.now()
        # 删除报表中关于估值日的数据

        # conn = con()
        # cursor = conn.cursor()
        log.info(f'Report_Data delete-PARTITION_KEY={run_date}files')
        # delete_sql = f'DELETE FROM DS_MR_REPORT_DATA WHERE PARTITION_KEY={run_date}'
        # cursor.execute(delete_sql)
        # 一般利率风险
        IR(run_date)
        # 特定利率风险 DS_IR_REPORT_SPECIFIC_SUMMARY
        IR_SPECIFIC(run_date)
        # 外汇风险
        FX_REPORT(run_date)
        # 商品期权 DS_COMMODITY_REPORT_SUMMARY
        OPTION_SUMMARY(run_date)
        # 商品风险
        COMMODITY_REPORT_SUMMARY(run_date)
        end_time = datetime.datetime.now()
        log.info(f'Report_Data-update end,run time {end_time - start_time}')
        status = 'Y'
        info = 'Report_Data--sucess'
    except:
        e = str(traceback.format_exc())
        log.error('Report_Data-error，error：' + e)
        status = 'Y'
        info = f'Report_Data--error:{e}'

    end_time = datetime.datetime.now()
    data = {
        'name': __name__,
        'partition_key': run_date,
        'info': info,
        'start_time': start_t,
        'end_time': end_time,
        'status': status
    }
    insert_sql = insert_log(data)
    conn = con()
    cursor = conn.cursor()
    cursor.execute(insert_sql)


# if __name__ == '__main__':
#     start('20240331')
