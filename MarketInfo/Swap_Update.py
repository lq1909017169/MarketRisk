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
        log.info(f'Swap Update-start')
        conn = jaydebeapi.connect(driver, url, [user, password], jarFile)
        cursor = conn.cursor()
        sele_sql = f"SELECT * FROM DERIVATIVE_MANUAL where PARTITION_KEY='{run_date}'"
        df = pd.read_sql(con=conn, sql=sele_sql)
        df = df[['LOAN_REFERENCE_NO', 'PAY_LASTRESETTING_RATE', 'PAY_PV', 'RECEIVE_LASTRESETTING_RATE', 'RECEIVE_PV',
                 'MARKET_VALUE', 'PAY_NEXTRESET_DATE', 'RECEIVE_NEXTRESE_DATE', 'PARTITION_KEY', 'FLOAT_PROFIT_LOSS']]
        df = df.fillna('')
        for index in range(len(df)):
            data = df.iloc[index]
            # print(data['RECEIVE_NEXTRESE_DATE'])
            sql = f"UPDATE DM_SWAP SET PAY_RATE_VALUE='{data['PAY_LASTRESETTING_RATE']}', " \
                  f"REC_RATE_VALUE='{data['RECEIVE_LASTRESETTING_RATE']}', PAYMENT_SIDE_VAL='{data['PAY_PV']}', " \
                  f"REVENUE_SIDE_VAL='{data['RECEIVE_PV']}' , REC_NXT_RESET_DATE='{data['RECEIVE_NEXTRESE_DATE']}',  " \
                  f"PAY_NXT_RESET_DATE='{data['PAY_NEXTRESET_DATE']}',  CONTRACT_MARKET_VALUE='{data['MARKET_VALUE']}', " \
                  f"FLOAT_PROFIT_LOSS='{data['FLOAT_PROFIT_LOSS']}'" \
                  f"WHERE TRADE_REF='{data['LOAN_REFERENCE_NO']}' AND PARTITION_KEY='{data['PARTITION_KEY']}'"
            # print(sql)
            cursor.execute(sql)
        end_time = datetime.datetime.now()
        log.info(f'Swap Update-end run time {end_time - start_time}, insert DM_SWAP {len(df)} files')
        return 'Y', 'Swap_Update--success'
    except:
        e = traceback.format_exc()
        log.error(f'Swap Update-error: {str(e)}')
        return 'N', f'Swap_Update--error:{str(e)}'


if __name__ == '__main__':
    run_date = sys.argv[1]
    start_time = datetime.datetime.now()
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
