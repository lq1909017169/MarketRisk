# 外汇即期
import map_table

def start(date):

    table = ['DM_FOREX', 'DS_Fx_Spot_Basic_Info']
    maping = {
        'TRADE_REF': 'trade_Id',
        'PURCHASE_CCY': 'currency',
        'PURCHASE_CCY_NOMINAL': 'balance',
        'PARTITION_KEY': 'PARTITION_KEY',
        # 'SALE_CCY': 'balance_Name',
        # 'class_Type': ''
        'TXN_DRC': 'buy_Sell'
    }

    map_table.map_t(table, maping, select_sql=f"FX_TYPE='T' AND BOOK_TYPE='T' AND　PARTITION_KEY={date}")
