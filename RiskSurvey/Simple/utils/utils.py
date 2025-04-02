import uuid
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd


def get_id() -> str:
    """
    获取一个随机的uuid
    """
    return str(uuid.uuid4()).replace('-', '')


def get_current_time() -> str:
    """
    获取当前的时间，以%Y-%m-%d %H:%M:%S格式输出
    """
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d")


def create_dates(start_date: str, frequency: str) -> str:
    """
    :param start_date: 开始的日期
    :param frequency: 相隔的时间
    :return: 相加之后的日期
    """
    dt = datetime.datetime.strptime(start_date[:10], "%Y-%m-%d")
    frequency_number = float(frequency[:-1])
    if 1 > frequency_number > 0:
        fre = int(float(frequency[:-1]))
        new_dt = dt + relativedelta(days=int((frequency_number - fre) * 30))
    else:
        fre = int(float(frequency[:-1]))
        month_date = dt + relativedelta(months=fre)
        if frequency_number - fre >= 0:
            new_dt = month_date
        else:
            new_dt = dt + relativedelta(days=int((frequency_number - fre) * 30))

    return new_dt.date().strftime("%Y-%m-%d")


def get_end(start_date: str, evaluation_date: str, frequency: str):
    """
    :param start_date: 开始的日期
    :param evaluation_date: 估值日期
    :param frequency: 相隔的日期
    :return: 返回估值日相近的交易日
    """
    end_date = ''
    # if

    while start_date <= evaluation_date:
        start_date = create_dates(start_date, frequency)
        end_date = start_date
    return end_date


def get_month_diff(start_date: str,
                   end_date: str
                   ) -> [float, int]:
    """
    两个时间点中间相差的月数
    :param start_date: 开始的日期
    :param end_date: 结束的日期
    :return: 相差的的月数
    """
    start_date = datetime.datetime.strptime(start_date[:10], "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date[:10], "%Y-%m-%d").date()
    day_difference = end_date - start_date
    month_difference = day_difference.days / 365 * 12
    return month_difference


# def get_month_diff(start_date: str,
#                    end_date: str
#                    ) -> [float, int]:
#     """
#     两个时间点中间相差的月数
#     :param start_date: 开始的日期
#     :param end_date: 结束的日期
#     :return: 相差的的月数
#     """
#     start_date = start_date[:10]
#     end_date = end_date[:10]
#     run_year_map = {'01': 31, '03': 31, '05': 31, '07': 31, '08': 31, '10': 31, '12': 31, '02': 29,
#                     '04': 30, '06': 30, '09': 30, '11': 30}
#     year_map = {'01': 31, '03': 31, '05': 31, '07': 31, '08': 31, '10': 31, '12': 31, '02': 29,
#                 '04': 30, '06': 30, '09': 30, '11': 30}
#     month = 0
#
#     if start_date == end_date:
#         month = 1
#     if start_date > end_date:
#         month = -1
#     else:
#         while start_date < end_date:
#             if int(start_date[:4]) % 4 == 0:
#                 month_number = start_date[5:7]
#                 start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
#                 new_date = start_date + relativedelta(days=run_year_map[month_number])
#             else:
#                 month_number = start_date[5:7]
#                 start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
#                 new_date = start_date + relativedelta(days=year_map[month_number])
#             month += 1
#             start_date = new_date.strftime('%Y-%m-%d')
#     if month <= 1:
#         month = 1
#     else:
#         month = month
#     return month


def correct_sensitivity(tenors: list,
                        sensitivity: list
                        ) -> list:
    """
    将tenors和sensitivity的长度对齐没有的用0补齐
    :param tenors: 曲线的时间点
    :param sensitivity: 敏感度数据
    :return: list
    """
    number = len(tenors) - len(sensitivity)
    if number > 0:
        [sensitivity.append(0) for _ in range(number)]
    elif number < 0:
        sensitivity = sensitivity[:number]
    return sensitivity


def analysis_data(series, residual_maturity: [int, float], columns: list):
    """
    :param series: df数据的每一行数据
    :param residual_maturity: 剩余期限
    :param columns 列名列表
    :return:
    """
    if not pd.isna(series['RESIDUAL_MATURITY_RIGHT']):
        if series['RESIDUAL_MATURITY_LEFT'] < residual_maturity <= series['RESIDUAL_MATURITY_RIGHT']:
            return series
        else:
            return pd.Series([None for _ in columns], index=columns)
    else:
        if series['RESIDUAL_MATURITY_LEFT'] < residual_maturity:
            return series
        else:
            return pd.Series([None for _ in columns], index=columns)


def sign(number):
    if number > 0:
        return 1
    elif number < 0:
        return -1
    else:
        return 0


# def commodity_to_db(config, commodity):
#     if commodity.shape[0] != 0:
#         def to_db(series):
#             data = {
#                 'tradeId': series['TradeId'],
#                 'commodityType': series['MerchandiseType'],
#                 'longShortType': series['LongShort'],
#                 'notional': series['Notional'],
#                 'reportDate': config.evaluationDate,
#                 'entryDate': get_current_time()
#             }
#             return pd.Series(list(data.values()), list(data.keys()))
#
#         commodity_db = commodity.apply(to_db, axis=1)
#         config.connect.saves(CommodityReportDetail, commodity_db.to_dict('records'))


def verify(data: pd.DataFrame, columns, func, args, axis) -> pd.DataFrame:
    if data.shape[0] == 0:
        return pd.DataFrame({k: [] for k in columns})
    else:
        return data.apply(func=func, args=args, axis=axis)


def verify_residual_maturity(residual_maturity):
    if residual_maturity <= 0:
        return -1
    else:
        return residual_maturity


def empty_df(columns):
    return pd.DataFrame({column: [None] for column in columns})


def empty_series(columns):
    return pd.Series([None for _ in columns], index=columns)


def apply_map_callback(s):
    try:
        if '-' in s and s[0] != '-':
            return s
        elif '-' in s and s[0] == '-':
            return eval(s)
        else:
            return eval(s)
    except:
        return s
