import BondBasicInfo
import Foreign_Exchange
import SwapBasicInfo
import DS_Fx_Swap_Basic_Info
import FxForwardBasicInfo
import FxInfo
import TradeInfo
import sys
import delete_table
import FxOptionBasicInfo


def start(run_date):
    # 删除明细表和汇总表
    delete_table.start(run_date)

    # 债券信息
    BondBasicInfo.start(run_date)

    # 外汇互换
    DS_Fx_Swap_Basic_Info.start(run_date)

    # 货币汇率表
    Foreign_Exchange.start(run_date)

    # 外汇远期
    FxForwardBasicInfo.start(run_date)

    # 外汇明细
    FxInfo.start(run_date)

    # 利率互换
    SwapBasicInfo.start(run_date)

    # 外汇期权
    FxOptionBasicInfo.start(run_date)

    # 交易明细
    TradeInfo.start(run_date)

    sys.exit(0)


if __name__ == '__main__':
    run_date = sys.argv[1]
    # run_date = '20230930'
    start(run_date=run_date)
