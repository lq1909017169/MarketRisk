import jaydebeapi
import pandas as pd
import sys
from config import OceanBase
from log import log
import datetime
import warnings
import traceback

warnings.filterwarnings('ignore')

url = OceanBase['url']
user = OceanBase['user']
password = OceanBase['password']
driver = OceanBase['driver']
jarFile = OceanBase['jar']


def start(run_date):
    try:
        start_time = datetime.datetime.now()
        log.info(f'Net_AGREEMENT_MANUAL Update-start')
        conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
        cursor = conn.cursor()
        sele_sql = f"SELECT * FROM Net_AGREEMENT_MANUAL where PARTITION_KEY='{run_date}'"
        df = pd.read_sql(con=conn, sql=sele_sql)
        df = df[['CUST_NAME', 'CUST_CODE', 'NET_FLAG', 'PORTFOLIO_NAME', 'PRODUCT_CODE', 'PARTITION_KEY']]
        df['PRODUCT_CODE'] = df['PRODUCT_CODE'].str.lstrip('COMX_')
        df['NET_FLAG'] = df['NET_FLAG'].replace({'T': '汇率衍生品', 'F': ''})
        for index in range(len(df)):
            CUST_CODE = df.iloc[index]['CUST_CODE']
            PRODUCT_CODE = df.iloc[index]['PRODUCT_CODE']
            NET_FLAG = df.iloc[index]['NET_FLAG']
            PARTITION_KEY = df.iloc[index]['PARTITION_KEY']
            update_sql1 = f"UPDATE DM_SWAP SET NETTING_AGREEMENT_REF='{NET_FLAG}' WHERE CPTY_ID='{CUST_CODE}' " \
                          f"AND PRODUCT_CODE='{PRODUCT_CODE}' AND PARTITION_KEY='{PARTITION_KEY}'"
            update_sql2 = f"UPDATE DM_FOREX SET NETTING_AGREEMENT_REF='{NET_FLAG}' WHERE CPTY_ID='{CUST_CODE}' " \
                          f"AND PRODUCT_CODE='{PRODUCT_CODE}' AND PARTITION_KEY='{PARTITION_KEY}'"
            update_sql3 = f"UPDATE DM_EXCHANGE_OPTION SET NETTING_AGREEMENT_REF='{NET_FLAG}' WHERE CPTY_ID='{CUST_CODE}' " \
                          f"AND PRODUCT_CODE='{PRODUCT_CODE}' AND PARTITION_KEY='{PARTITION_KEY}'"
            update_sql4 = f"UPDATE DM_COMMODITY_SWAP SET NETTING_AGREEMENT_REF='{NET_FLAG}' WHERE CPTY_ID='{CUST_CODE}' " \
                          f"AND PRODUCT_CODE='{PRODUCT_CODE}' AND PARTITION_KEY='{PARTITION_KEY}'"
            cursor.execute(update_sql1)
            cursor.execute(update_sql2)
            cursor.execute(update_sql3)
            cursor.execute(update_sql4)
        end_time = datetime.datetime.now()
        log.info(f'Net_AGREEMENT_MANUAL Update-end run time {end_time - start_time}, insert DM_SWAP,DM_FOREX, '
                 f'DM_EXCHANGE_OPTION, DM_COMMODITY_SWAP {len(df)} files')
        cursor.close()
    except:
        e = traceback.format_exc()
        log.error(f'Net_AGREEMENT_MANUAL Update-error: {str(e)}')


if __name__ == '__main__':
    run_date = sys.argv[1]
    start(run_date=run_date)
