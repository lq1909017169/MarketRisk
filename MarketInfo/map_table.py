import traceback

import jaydebeapi
import pandas as pd
import uuid
import datetime
import warnings

from config import OceanBase
from log import log

warnings.filterwarnings('ignore')

url = OceanBase['url']
user = OceanBase['user']
password = OceanBase['password']
driver = OceanBase['driver']
jarFile = OceanBase['jar']


def callback(series):
    values = "'" + "','".join(tuple(map(str, series.values.tolist()))) + "'"
    return f"({values})"


def create_sql(table, data):
    into = "insert into {table} ({keys}) values {values}"
    keys = ','.join(data.columns)
    values = ','.join(data.apply(callback, axis=1).values)
    return into.format_map({
        'table': table[1],
        'keys': keys,
        'values': values
    })


def map_t(table: list, maping: dict, select_date=None, table_name=None, append_name=None, select_sql=None):
    try:
        conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
        cursor = conn.cursor()
        log.info(f'{table[1]}-start')
        start_time = datetime.datetime.now()
        if append_name:
            log.info(f'Delete {table[1]} about PARTITION_KEY={select_date}，about product={append_name}')
            delete_sql = f"DELETE FROM {table[1]} WHERE PARTITION_KEY='{select_date}' AND product='{append_name}'"
            cursor.execute(delete_sql)
            if select_sql:
                select_query = f"select {', '.join(list(maping.keys()))} from {table[0]} where PARTITION_KEY=" \
                               f"'{select_date}' AND {select_sql}"
            else:
                select_query = f"select {', '.join(list(maping.keys()))} from {table[0]} where PARTITION_KEY=" \
                               f"'{select_date}'"

        else:
            log.info(f'Delete {table[1]} PARTITION_KEY={select_date}')
            delete_sql = f"DELETE FROM {table[1]} WHERE PARTITION_KEY='{select_date}'"
            cursor.execute(delete_sql)
            if select_sql:
                select_query = f"select {', '.join(list(maping.keys()))} from {table[0]} where PARTITION_KEY=" \
                               f"'{select_date}' AND {select_sql}"
            else:
                select_query = f"select {', '.join(list(maping.keys()))} from {table[0]} where PARTITION_KEY=" \
                               f"'{select_date}'"

        res = pd.read_sql(select_query, conn, chunksize=1000000)
        for data in res:
            # 针对每张表添加条件
            if table_name == 'DS_Foreign_Exchange':
                data = data.dropna()
                delete_foreign_exchange_sql = 'DELETE FROM DS_FOREIGN_EXCHANGE WHERE PARTITION_KEY=20230930'
                cursor.execute(delete_foreign_exchange_sql)
                a_sql = "INSERT INTO DS_FOREIGN_EXCHANGE VALUES (1, '20230930','AU99.99', 'CNY', 462.68)"
                cursor.execute(a_sql)
            elif table_name == 'DS_BOND_BASIC_INFO':
                data = data.dropna(subset=['INVEST_REF', 'PRODUCT_CODE', 'MATURITY_DATE'])
            elif table_name == 'DS_Swap_Basic_Info':
                data = data.dropna(subset=['TRADE_REF'])
            data = data.rename(columns=maping)
            if append_name:
                data['product'] = append_name

            data['id'] = data.apply(lambda _: str(uuid.uuid4()).replace('-', ''), axis=1)
            if len(data):
                sql = create_sql(table, data.fillna(''))
                print(sql)
                cursor.execute(sql)
            if table_name == 'DS_Fx_Swap_Basic_Info':
                delete_swap_sql1 = "DELETE FROM DS_FX_SWAP_BASIC_INFO WHERE TRADE_ID like 'COMX_20220328300050004%' " \
                                   "AND PARTITION_KEY='20230930'"
                delete_swap_sql2 = "DELETE FROM DS_TRADE_INFO WHERE TRADE_ID like 'COMX_20220328300050004%' " \
                                   "AND PARTITION_KEY='20230930'"
                cursor.execute(delete_swap_sql1)
                cursor.execute(delete_swap_sql2)
            end_time = datetime.datetime.now()
            log.info(f'{table[1]} - end  , run time {end_time - start_time}, insert {table[1]} {len(data)} files')
        cursor.close()
        return 'Y', f'map_table--{table[1]} success'
    except:
        e = str(traceback.format_exc())
        log.error(f'{table[1]}-error，error：' + e)
        return 'N', 'map_table-error，error：' + e


def delete_tables(table_name, partition_key):
    conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
    cursor = conn.cursor()
    log.info(f'{table_name}-start')
    start_time = datetime.datetime.now()
    log.info(f'Delete {table_name} about PARTITION_KEY={partition_key}')
    try:
        if table_name == 'RES_REPORT_DATA':
            delete_sql = f"DELETE FROM {table_name} WHERE PARTITION_KEY='{partition_key}' AND SHEET_NAME like 'G4C-1%'"
        else:
            delete_sql = f"DELETE FROM {table_name} WHERE PARTITION_KEY='{partition_key}'"
        cursor.execute(delete_sql)
        end_time = datetime.datetime.now()
        log.info(f'Delete{table_name} - end  , run time {end_time - start_time}')
        return 'Y', f'delete_tables--{table_name} success'
    except:
        e = str(traceback.format_exc())
        log.error('BondInfo-error，error：' + e)
        return 'N', 'delete_tables-error，error：' + e


def create_t(table='', rule=''):
    conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
    cor = conn.cursor()
    # 创建表
    sql = f'CREATE TABLE {table} ({rule}); '
    cor.execute(sql)
    print(sql)
    cor.close()


def insert_log(data):
    conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
    cursor = conn.cursor()
    data['start_time'] = data['start_time'].strftime("%Y-%m-%d %H:%M:%S")
    data['end_time'] = data['end_time'].strftime("%Y-%m-%d %H:%M:%S")

    sql = f"""
    insert into RWA_PYTHON_LOG(OBJECT_OWN, OBJECT_CTG, OBJECT_NAME, RUN_DATE, PRO_DESC, START_TIME, END_TIME, FLAG) 
    VALUES ('RWA', 'NRWA_MARKET_TASK', '{data['name']}', '{data['partition_key']}', '{data['info']}', 
    '{data['start_time']}', '{data['end_time']}', '{data['status']}')
    """
    cursor.execute(sql)

