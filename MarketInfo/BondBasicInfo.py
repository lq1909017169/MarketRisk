import datetime
import uuid
import jaydebeapi
import pandas as pd
import warnings
import traceback

from map_table import insert_log
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
        'table': table,
        'keys': keys,
        'values': values
    })


def map_t(mapping: dict, date):
    try:
        start_time = datetime.datetime.now()
        log.info('BondInfo-start')
        conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
        cursor = conn.cursor()

        log.info(f'delete DS_BOND_BASIC_INFO about {date} files')
        delete_sql = f"DELETE FROM DS_BOND_BASIC_INFO WHERE PARTITION_KEY='{date}'"
        cursor.execute(delete_sql)

        select_query1 = f"select {', '.join(list(mapping.keys()))} from DM_INVEST where PARTITION_KEY={date} " \
                        f"AND INVEST_TYPE='B'"
        select_query2 = f"select TRADE_REF, INVEST_REF, PARTITION_KEY, POSITION, UNT_CLEAN_PRICE, LONG_SHORT, " \
                        f"MATURITY_DATE from DM_INVEST_POS where PARTITION_KEY={date} AND BOOK_TYPE='T' AND " \
                        f"UNT_CLEAN_PRICE IS NOT NULL"
        select_query3 = f"select ISSUER_CODE, RATING, RATING_ECAI_CODE, RATING_DATE  from DM_RATING_ISSUER where " \
                        f"PARTITION_KEY={date} AND RATING_ECAI_CODE != 'SCRA'"
        # select_query3 = f"select ISSUER_CODE, EXT_LT_RAT_CD_USED from SR_RATING_ISSUER WHERE PARTITION_KEY={date}"

        res1 = pd.read_sql(select_query1, conn)
        res2 = pd.read_sql(select_query2, conn)
        res3 = pd.read_sql(select_query3, conn)

        if len(res2) == 0:
            log.info("BondInfo-update end")
            return

        res3['RATING_DATE'] = pd.to_datetime(res3['RATING_DATE'])
        data3 = res3.loc[res3.groupby(['ISSUER_CODE', 'RATING_ECAI_CODE'])['RATING_DATE'].idxmax()]

        replace_rating = {
            'AAApi': 'AAA',
            'AA+pi': 'AA+',
            'AApi': 'AA',
            'A-1': 'AA-',
            'A-1+': 'AA-',
            'A-2': 'BBB-',
            'A-3': 'BBB-',
            'SD': 'D',
            'NR': '未评级'
        }

        data3['RATING'] = data3['RATING'].replace(replace_rating)

        rating_list = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+',
                       'BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C+', 'C', 'C-',
                       'DDD+', 'DDD', 'DDD-', 'DD+', 'DD', 'DD-', 'D+', 'D', 'D-', '其他', '未评级']
        data3['RATING'] = data3['RATING'].where(data3['RATING'].isin(rating_list), '未评级')

        trade_Ref_List = res2['TRADE_REF'].tolist()
        select_sql = f"""select RW_ORI, TRADE_REF from RES_CR where TRADE_REF in ('{"', '".join(trade_Ref_List)}') 
        AND PARTITION_KEY='{date}'"""
        data = pd.read_sql(select_sql, conn)
        data = data.rename(columns={'RW_ORI': 'rate'})
        res2 = pd.merge(res2, data, on=['TRADE_REF'], how='left')

        data1 = pd.merge(res1, res2[['INVEST_REF', 'rate']], on=['INVEST_REF'], how='left')
        data1 = data1.drop_duplicates(subset=['INVEST_REF', 'PARTITION_KEY'])
        mapping.update({'rate': 'RWA_RATING'})
        df = data1.rename(columns=mapping)
        df['id'] = df.apply(lambda _: str(uuid.uuid4()).replace('-', ''), axis=1)
        if len(df):
            sql = create_sql('DS_BOND_BASIC_INFO', df.fillna(''))
            cursor.execute(sql)

        def check(x):
            # x = x.drop_duplicates(subset=['RATING']).fillna('未评级')
            rating_list = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+',
                           'BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C+', 'C', 'C-',
                           'DDD+', 'DDD', 'DDD-', 'DD+', 'DD', 'DD-', 'D+', 'D', 'D-', '其他', '未评级']
            T_rating = ['AAA+', 'AAA', 'AAA-', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+']
            F_rating = ['BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C+', 'C', 'C-',
                        'DDD+', 'DDD', 'DDD-', 'DD+', 'DD', 'DD-', 'D+', 'D', 'D-', '其他', '未评级']
            list1 = x['RATING'].tolist()

            sorted_list = sorted(list1, key=lambda x: rating_list.index(x))

            N_T_list = [x for x in list1 if x in T_rating]

            T_list = [x for x in T_rating if x in sorted_list]
            F_list = [x for x in F_rating if x in sorted_list]

            if len(N_T_list) >= 2:
                if len(T_list) == 1:
                    ele = T_list[0]
                else:
                    ele = T_list[1]
            else:
                if len(F_list):
                    ele = F_list[0]
                else:
                    ele = '未评级'
            issuer_code = x['ISSUER_CODE'].tolist()[0]
            return pd.Series([issuer_code, ele], index=['ISSUER_ID', 'RATING'])

        data3 = data3[['ISSUER_CODE', 'RATING']].fillna('未评级')

        data3 = data3.groupby('ISSUER_CODE').apply(check).reset_index(drop=True)
        # data3 = res3.rename(columns={'ISSUER_CODE': 'ISSUER_ID', 'EXT_LT_RAT_CD_USED': 'RATING'})
        # print(data3)
        res2['sim_PV'] = res2['UNT_CLEAN_PRICE']

        data2 = res2[['INVEST_REF', 'PARTITION_KEY', 'sim_PV', 'LONG_SHORT', 'MATURITY_DATE']]

        data = pd.merge(data2, data1[['INVEST_REF', 'ISSUER_ID', 'PRODUCT_CODE']], on=['INVEST_REF'], how='left')
        data = pd.merge(data, data3, on=['ISSUER_ID'], how='left')

        # data.loc[(data['ISSUER_ID'] == '财政部'), 'RATING'] = 'AAA'
        data.loc[(data['PRODUCT_CODE'].isin(['1', '8'])), 'RATING'] = 'AAA'
        data['RATING'] = data['RATING'].fillna('未评级')

        data['PARTITION_KEY'] = date
        data['MATURITY_DATE'] = data['MATURITY_DATE'].astype(str)
        ele_date = date[:4] + '-' + date[4:6] + '-' + date[6:] + ' 00:00:00'
        data = data[data['MATURITY_DATE'] > ele_date]

        data['id'] = data.apply(lambda _: str(uuid.uuid4()).replace('-', ''), axis=1)
        data['trade_Id'] = data.apply(lambda _: str(uuid.uuid4()).replace('-', ''), axis=1)
        data['PRODUCT'] = 'DS_Bond_Basic_Info'
        mapping.update({'RATING': 'rating', 'sim_PV': 'sim_PV', 'LONG_SHORT': 'long_Short'})
        data = data.rename(columns=mapping)
        data = data[['symbol', 'rating', 'id', 'sim_PV', 'PARTITION_KEY', 'long_Short', 'trade_Id', 'PRODUCT']]
        delete_sql = f"DELETE FROM DS_TRADE_INFO WHERE PARTITION_KEY='{date}' AND PRODUCT='DS_Bond_Basic_Info'"
        cursor.execute(delete_sql)

        sql = create_sql('DS_TRADE_INFO', data)
        cursor.execute(sql)
        end_time = datetime.datetime.now()
        log.info(
            f'BondInfo-update end,run time {end_time - start_time}, insert DS_Bond_Basic_Info '
            f'{len(df)} files，Trade_Info {len(data)} files')
        return 'Y', f'{__name__} success'
    except:
        e = str(traceback.format_exc())
        log.error('BondInfo-error，error：' + e)
        return 'N', 'BondInfo-error，error：' + e


def start(date):
    start_time = datetime.datetime.now()
    data_invest = {
        'INVEST_REF': 'symbol',
        'INVEST_DESCRIPTION': 'bond_Name',
        'ISSUER_ID': 'issuer_Name',
        'PRODUCT_CODE': 'bond_Type',
        'CURRENCY_CODE': 'currency',
        'UNIT_NOMINAL': 'par_Value',
        'BID_PRICE': 'issuer_Price',
        'COUPON_RATE': 'coupon_Rate',
        'INVEST_DATE': 'start_Date',
        'MATURITY_DATE': 'end_Date',
        'COUPON_RATE_TYPE': 'calculation_Mode',
        'PARTITION_KEY': 'PARTITION_KEY'
    }
    status, info = map_t(data_invest, date)
    """
        MATURITY_YEAR = 'MATURITY_YEAR'
        PAYMENT_MODE = 'PAYMENT_MODE'
        IS_OPTION = 'IS_OPTION'
        ISSUER_RATING = 'ISSUER_RATING'
        IS_GUARANTEE = 'IS_GUARANTEE'
    """
    end_time = datetime.datetime.now()
    data = {
        'name': __name__,
        'partition_key': date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': status
    }
    insert_log(data)

