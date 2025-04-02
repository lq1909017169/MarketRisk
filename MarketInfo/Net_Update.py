import jaydebeapi
import pandas as pd
import sys
from config import OceanBase
from log import log
import datetime
import warnings
import traceback
import map_table

warnings.filterwarnings('ignore')

url = OceanBase['url']
user = OceanBase['user']
password = OceanBase['password']
driver = OceanBase['driver']
jarFile = OceanBase['jar']


def start(run_date):
    try:
        start_time = datetime.datetime.now()
        log.info(f'Net_CPTY_ID Update-start')
        conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
        cursor = conn.cursor()
        sele_sql = f"SELECT * FROM Net_AGREEMENT_MANUAL where PARTITION_KEY='{run_date}' and PRODUCT_CODE='IRS_STRD0' " \
                   f"and NET_FLAG='T'"
        df = pd.read_sql(con=conn, sql=sele_sql)

        select_swap_sql = f"SELECT CPTY_ID FROM DM_SWAP where PARTITION_KEY='{run_date}' and PRODUCT_CODE='IRS_STRD0'"
        df_swap = pd.read_sql(con=conn, sql=select_swap_sql)

        select_forex_sql = f"SELECT CPTY_ID FROM DM_FOREX where PARTITION_KEY='{run_date}' and PRODUCT_CODE='IRS_STRD0'"
        df_forex = pd.read_sql(con=conn, sql=select_forex_sql)

        num1 = 0
        if len(df_swap):
            for code in list(set(df_swap['CPTY_ID'])):
                if code in list(set(df['CUST_CODE'].tolist())):
                    num1 += 1
                    update_sql = f"UPDATE DM_SWAP SET NETTING_AGREEMENT_REF='NET_{code}' WHERE CPTY_ID='{code}' " \
                                 f"AND PARTITION_KEY='{run_date}'"
                    # print(update_sql)
                    cursor.execute(update_sql)
        num2 = 0
        if len(df_forex):
            for code in list(set(df_forex['CPTY_ID'])):
                if code in list(set(df['CUST_CODE'].tolist())):
                    num2 += 1
                    update_sql = f"UPDATE DM_FOREX SET NETTING_AGREEMENT_REF='NET_{code}' WHERE CPTY_ID='{code}' " \
                                 f"AND PARTITION_KEY='{run_date}'"
                    # print(update_sql)
                    cursor.execute(update_sql)
        end_time = datetime.datetime.now()
        log.info(f"Net_CPTY_ID Update-end run time {end_time - start_time},"
                 f"DM_SWAP UPDATE {num1} files, "
                 f"DM_FOREX UPDATE {num2} files")

        return 'Y', 'Net_Update--success'
    except:
        e = traceback.format_exc()
        log.error(f'Net_CPTY_ID Update-error: {str(e)}')
        return 'N', f'Net_Update--error:{e}'


if __name__ == '__main__':
    run_date = sys.argv[1]
    start_time = datetime.datetime.now()
    # run_date = '20230930'
    status, info = start(run_date=run_date)
    end_time = datetime.datetime.now()
    data = {
        'name': 'DS_FX_Forward_Basic_Info',
        'partition_key': run_date,
        'info': info,
        'start_time': start_time,
        'end_time': end_time,
        'status': status
    }
    map_table.insert_log(data)
