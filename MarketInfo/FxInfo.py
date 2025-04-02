import jaydebeapi
import pandas as pd
import uuid
import datetime
import traceback

import map_table
from log import log
from config import OceanBase

url = OceanBase['url']
user = OceanBase['user']
password = OceanBase['password']
driver = OceanBase['driver']
jarFile = OceanBase['jar']


def insert_sql(data, currency, date):
    conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
    cur = conn.cursor()
    delete_sql = f"DELETE FROM DS_FX_Report_Detail WHERE PARTITION_KEY='{date}' AND currency='{currency}'"
    cur.execute(delete_sql)
    log.info(f"Delete DS_FX_Report_Detail PARTITION_KEY={date} AND currency='{currency}'")

    sql = f"INSERT INTO DS_FX_Report_Detail(id, currency, long_Short_Domestic, delta_Domestic, " \
          f"open_Position_Domestic, long_Short_Not_Domestic, delta_Not_Domestic, open_Position_Not_Domestic, " \
          f"PARTITION_KEY, entry_Date) VALUES ('{str(uuid.uuid4()).replace('-', '')}', '{currency}', %f, %f, " \
          f"%f, %f, %f, %f, '{date}', '{datetime.date.today()}')" % tuple(data)
    cur.execute(sql)
    log.info(f"Insert DS_FX_Report_Detail PARTITION_KEY={date} AND currency='{currency}'")


def G32(date, currency):
    conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
    sql = f"select * from DM_GL_BALANCE where PARTITION_KEY='{date}' AND GL_ITEM_ID in ('1304','3002'," \
          "'11011701','11011801','1106','100301','30040701','3002','2106','21010301','21010401','30040702'," \
          f"'20030901','142','144','146','156','158','143','145','147','157','159', '1001', '1002', '1003', '1004', " \
          f"'1011', '1013', '1031', '1101', '1106', '1111', '1123', '1131', '1132', '1221', '1231', '1301', '1302', " \
          f"'1303', '1304', '1305', '1307', '1308', '1309', '1311', '1312', '1324', '1431', '1432', '1441', '1442', " \
          f"'1501', '1502', '1503', '1504', '1505', '1506', '1511', '1512', '1521', '1522', '1523', '1531', '1601', " \
          f"'1602', '1603', '1605', '1606', '1607', '1701', '1702', '1703', '1711', '1801', '1802', '1811', '1821', " \
          f"'1822', '1823', '1901', '2001', '2002', '2003', '2004', '2005', '2011', '2012', '2013', '2014', '2015', " \
          f"'2016', '2017', '2019', '2020', '2021', '2101', '2106', '2111', '2203', '2211', '2221', '2231', '2232', " \
          f"'2241', '2309', '2313', '2314', '2401', '2502', '2601', '2801', '2901', '3001', '3002', '3004', '4001', " \
          f"'4002', '4003', '4004', '4101', '4102', '4103', '4104', '4201') " \
          f"AND VALUE_CUR='{currency}' AND GL_ORG_ID = 706660888"

    df = pd.read_sql(sql=sql, con=conn)

    sql = f"select EX_RATE from DM_EX_RATE where PARTITION_KEY='{date}' AND CURRENCY_IN='{currency}'"
    rate = pd.read_sql(sql=sql, con=conn)
    if len(rate):
        rate = rate.iloc[0, 0]
        if rate is None:
            rate = 0
    else:
        rate = 0

    # 借方为0，贷方为1
    def get_value(key, types):
        if key in df['GL_ITEM_ID'].tolist():
            if types:
                get_df = df.query(f"GL_ITEM_ID == '{key}' and BALANCE < 0")
                value = abs(get_df['BALANCE'].sum()) / 10000
            else:
                get_df = df.query(f"GL_ITEM_ID == '{key}' and BALANCE > 0")
                value = abs(get_df['BALANCE'].sum()) / 10000
        else:
            value = 0
        return value

    balance_10_0 = sum([get_value(x, 0) for x in
                        ['1001', '1002', '1003', '1004', '1011', '1013', '1031', '1101', '1106',
                         '1111', '1123', '1131', '1132', '1221', '1231', '1301', '1302', '1303',
                         '1304', '1305', '1307', '1308', '1309', '1311', '1312', '1324', '1431',
                         '1432', '1441', '1442', '1501', '1502', '1503', '1504', '1505', '1506',
                         '1511', '1512', '1521', '1522', '1523', '1531', '1601', '1602', '1603',
                         '1605', '1606', '1607', '1701', '1702', '1703', '1711', '1801', '1802',
                         '1811', '1821', '1822', '1823', '1901']])
    balance_10_1 = sum([get_value(x, 1) for x in
                        ['1001', '1002', '1003', '1004', '1011', '1013', '1031', '1101', '1106',
                         '1111', '1123', '1131', '1132', '1221', '1231', '1301', '1302', '1303',
                         '1304', '1305', '1307', '1308', '1309', '1311', '1312', '1324', '1431',
                         '1432', '1441', '1442', '1501', '1502', '1503', '1504', '1505', '1506',
                         '1511', '1512', '1521', '1522', '1523', '1531', '1601', '1602', '1603',
                         '1605', '1606', '1607', '1701', '1702', '1703', '1711', '1801', '1802',
                         '1811', '1821', '1822', '1823', '1901']])

    balance_30_0 = sum([get_value(x, 0) for x in ['3001', '3002', '3004']])

    balance_30_1 = sum([get_value(x, 1) for x in ['3001', '3002', '3004']])

    balance_20_0 = sum([get_value(x, 0) for x in
                        ['2001', '2002', '2003', '2004', '2005', '2011', '2012', '2013', '2014',
                         '2015', '2016', '2017', '2019', '2020', '2021', '2101', '2106', '2111',
                         '2203', '2211', '2221', '2231', '2232', '2241', '2309', '2313', '2314',
                         '2401', '2502', '2601', '2801', '2901']])

    balance_20_1 = sum([get_value(x, 1) for x in
                        ['2001', '2002', '2003', '2004', '2005', '2011', '2012', '2013', '2014',
                         '2015', '2016', '2017', '2019', '2020', '2021', '2101', '2106', '2111',
                         '2203', '2211', '2221', '2231', '2232', '2241', '2309', '2313', '2314',
                         '2401', '2502', '2601', '2801', '2901']])



    spot_balance = balance_10_0 - balance_10_1 + get_value('1304', 1) + balance_30_0 - get_value('3002', 0) - get_value(
        '11011701', 0) - get_value('11011801', 0) - get_value('1106', 0)

    # 借方为0，贷方为1
    spot_debt = balance_20_1 - balance_20_0 + balance_30_1 - get_value('3002', 1) - get_value('2016', 1) - get_value(
        '21010301', 1) - get_value('21010401', 1) - get_value('20030901', 1)
    # 借方为0，贷方为1
    forward_buy = get_value('142', 0) + get_value('144', 0) + get_value('14602', 0) + get_value('156', 0) + get_value(
        '158', 0)
    # 借方为0，贷方为1
    forward_sell = get_value('143', 0) + get_value('145', 0) + get_value('14702', 0) + get_value('157', 0) + get_value(
        '159', 0)

    change_option = 0

    open_position = spot_balance * rate - spot_debt * rate + forward_buy * rate - forward_sell * rate + change_option

    # balance_40_0 = sum([get_value(x, 0) for x in ['4001', '4002', '4003', '4004', '4101',
    #                                               '4102', '4103', '4104', '4201']])
    #
    # balance_40_1 = sum([get_value(x, 1) for x in ['4001', '4002', '4003', '4004', '4101',
    #                                               '4102', '4103', '4104', '4201']])

    # open_structural = get_value('100301', 0) + (get_value('30040702', 0) - get_value('30040702', 1)) + (
    #         balance_40_0 - balance_40_1) - get_value('4003', 0) + get_value('4003', 1)

    open_structural = get_value('1511', 0) + get_value('611108', 0) + get_value('400323', 0) - get_value(
        '670106', 1)

    # print(open_structural * rate)
    data = [open_position, 0, open_structural * rate, 0, 0, 0]
    insert_sql(data, currency, date)


def AU(date):
    conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
    sql = f"select * from DM_GL_BALANCE where PARTITION_KEY='{date}' AND GL_ITEM_ID in ('61010117', '20030901', " \
          f"'1570201', '1570301', '1570401', '1560201', '1560301', '1560401') AND VALUE_CUR='CNY' " \
          f"AND GL_ORG_ID = 706660888"

    df = pd.read_sql(sql=sql, con=conn)

    AUY_sql = f"select * from DM_FOREX where PARTITION_KEY={date} AND TRADE_REF='FSS_7066605002681'"
    AUY_df = pd.read_sql(sql=AUY_sql, con=conn)
    if len(AUY_df):
        G22_AUY = AUY_df['PURCHASE_CCY_NOMINAL'].values[0] / 10000
    else:
        G22_AUY = 0

    # 借方为0，贷方为1
    def get_value(key, types):
        if key in df['GL_ITEM_ID'].tolist():
            if types:
                get_df = df.query(f"GL_ITEM_ID == '{key}' and BALANCE < 0")
                value = abs(get_df['BALANCE'].sum()) / 10000
            else:
                get_df = df.query(f"GL_ITEM_ID == '{key}' and BALANCE > 0")
                value = abs(get_df['BALANCE'].sum()) / 10000
        else:
            value = 0
        return value

    balance_spot = G22_AUY + get_value('61010117', 0)

    liability_spot = get_value('20030901', 1)

    forward_buy = get_value('1570201', 0) + get_value('1570301', 0) + get_value('1570401', 0)

    forward_sell = get_value('1560201', 1) + get_value('1560301', 1) + get_value('1560401', 1)

    # open_position = balance_spot - liability_spot + forward_buy - forward_sell

    open_position = 0

    data = [open_position, 0, 0, 0, 0, 0]

    currency = 'AUY'

    insert_sql(data, currency, date)


def start(date):
    log.info(f'FxInfo-start')
    start_time = datetime.datetime.now()
    try:
        for ccy in ['USD', 'EUR', 'JPY', 'GBP', 'HKD', 'CHF', 'AUD', 'CAD', 'SGD']:
            G32(date=date, currency=ccy)
        AU(date)
        log.info(f'FxInfo-end')
        info = 'FxInfo -- success'
        statue = 'Y'
    except:
        e = str(traceback.format_exc())
        log.error('FxInfo-error，error：' + e)
        info = f'FxInfo--error:{e}'
        statue = 'N'

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
