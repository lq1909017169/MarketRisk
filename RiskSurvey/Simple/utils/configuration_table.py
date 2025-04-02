from Simple.utils.connect import Connect
from Simple.utils.utils import analysis_data
import pandas as pd

PERIOD_WEIGHT_BIG_DATA = [
    {'RESIDUAL_MATURITY_LEFT': 0, 'RESIDUAL_MATURITY_RIGHT': 1, 'WEIGHT': 0, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 1},
    {'RESIDUAL_MATURITY_LEFT': 1, 'RESIDUAL_MATURITY_RIGHT': 3, 'WEIGHT': 0.002, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 2},
    {'RESIDUAL_MATURITY_LEFT': 3, 'RESIDUAL_MATURITY_RIGHT': 6, 'WEIGHT': 0.004, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 3},
    {'RESIDUAL_MATURITY_LEFT': 6, 'RESIDUAL_MATURITY_RIGHT': 12, 'WEIGHT': 0.007, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 4},
    {'RESIDUAL_MATURITY_LEFT': 12, 'RESIDUAL_MATURITY_RIGHT': 24, 'WEIGHT': 0.0125, 'ASSUMED_RATE': 0.9, 'SECTION': '2',
     'TIME_FRAME': 5},
    {'RESIDUAL_MATURITY_LEFT': 24, 'RESIDUAL_MATURITY_RIGHT': 36, 'WEIGHT': 0.0175, 'ASSUMED_RATE': 0.8, 'SECTION': '2',
     'TIME_FRAME': 6},
    {'RESIDUAL_MATURITY_LEFT': 36, 'RESIDUAL_MATURITY_RIGHT': 48, 'WEIGHT': 0.0225, 'ASSUMED_RATE': 0.75,
     'SECTION': '2',
     'TIME_FRAME': 7},
    {'RESIDUAL_MATURITY_LEFT': 48, 'RESIDUAL_MATURITY_RIGHT': 60, 'WEIGHT': 0.0275, 'ASSUMED_RATE': 0.75,
     'SECTION': '2',
     'TIME_FRAME': 8},
    {'RESIDUAL_MATURITY_LEFT': 60, 'RESIDUAL_MATURITY_RIGHT': 84, 'WEIGHT': 0.0325, 'ASSUMED_RATE': 0.7, 'SECTION': '3',
     'TIME_FRAME': 9},
    {'RESIDUAL_MATURITY_LEFT': 84, 'RESIDUAL_MATURITY_RIGHT': 120, 'WEIGHT': 0.0375, 'ASSUMED_RATE': 0.65,
     'SECTION': '3',
     'TIME_FRAME': 10},
    {'RESIDUAL_MATURITY_LEFT': 120, 'RESIDUAL_MATURITY_RIGHT': 180, 'WEIGHT': 0.045, 'ASSUMED_RATE': 0.6,
     'SECTION': '3',
     'TIME_FRAME': 11},
    {'RESIDUAL_MATURITY_LEFT': 180, 'RESIDUAL_MATURITY_RIGHT': 240, 'WEIGHT': 0.0525, 'ASSUMED_RATE': 0.6,
     'SECTION': '3',
     'TIME_FRAME': 12},
    {'RESIDUAL_MATURITY_LEFT': 240, 'RESIDUAL_MATURITY_RIGHT': None, 'WEIGHT': 0.06, 'ASSUMED_RATE': 0.6,
     'SECTION': '3',
     'TIME_FRAME': 13},
]
PERIOD_WEIGHT_BIG_TABLE = pd.DataFrame.from_records(PERIOD_WEIGHT_BIG_DATA)

PERIOD_WEIGHT_SMALL_DATA = [
    {'RESIDUAL_MATURITY_LEFT': 0, 'RESIDUAL_MATURITY_RIGHT': 1, 'WEIGHT': 0, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 1},
    {'RESIDUAL_MATURITY_LEFT': 1, 'RESIDUAL_MATURITY_RIGHT': 3, 'WEIGHT': 0.002, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 2},
    {'RESIDUAL_MATURITY_LEFT': 3, 'RESIDUAL_MATURITY_RIGHT': 6, 'WEIGHT': 0.004, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 3},
    {'RESIDUAL_MATURITY_LEFT': 6, 'RESIDUAL_MATURITY_RIGHT': 12, 'WEIGHT': 0.007, 'ASSUMED_RATE': 1, 'SECTION': '1',
     'TIME_FRAME': 4},
    {'RESIDUAL_MATURITY_LEFT': 12 * 1, 'RESIDUAL_MATURITY_RIGHT': 12 * 1.9, 'WEIGHT': 0.0125, 'ASSUMED_RATE': 0.9,
     'SECTION': '2', 'TIME_FRAME': 5},
    {'RESIDUAL_MATURITY_LEFT': 12 * 1.9, 'RESIDUAL_MATURITY_RIGHT': 12 * 2.8, 'WEIGHT': 0.0175, 'ASSUMED_RATE': 0.8,
     'SECTION': '2', 'TIME_FRAME': 6},
    {'RESIDUAL_MATURITY_LEFT': 12 * 2.8, 'RESIDUAL_MATURITY_RIGHT': 12 * 3.6, 'WEIGHT': 0.0225, 'ASSUMED_RATE': 0.75,
     'SECTION': '2', 'TIME_FRAME': 7},
    {'RESIDUAL_MATURITY_LEFT': 12 * 3.6, 'RESIDUAL_MATURITY_RIGHT': 12 * 4.3, 'WEIGHT': 0.0275, 'ASSUMED_RATE': 0.75,
     'SECTION': '3', 'TIME_FRAME': 8},
    {'RESIDUAL_MATURITY_LEFT': 12 * 4.3, 'RESIDUAL_MATURITY_RIGHT': 12 * 5.7, 'WEIGHT': 0.0325, 'ASSUMED_RATE': 0.7,
     'SECTION': '3', 'TIME_FRAME': 9},
    {'RESIDUAL_MATURITY_LEFT': 12 * 5.7, 'RESIDUAL_MATURITY_RIGHT': 12 * 7.3, 'WEIGHT': 0.0375, 'ASSUMED_RATE': 0.65,
     'SECTION': '3', 'TIME_FRAME': 10},
    {'RESIDUAL_MATURITY_LEFT': 12 * 7.3, 'RESIDUAL_MATURITY_RIGHT': 12 * 9.3, 'WEIGHT': 0.045, 'ASSUMED_RATE': 0.6,
     'SECTION': '3', 'TIME_FRAME': 11},
    {'RESIDUAL_MATURITY_LEFT': 12 * 9.3, 'RESIDUAL_MATURITY_RIGHT': 12 * 10.6, 'WEIGHT': 0.0525, 'ASSUMED_RATE': 0.6,
     'SECTION': '3', 'TIME_FRAME': 12},
    {'RESIDUAL_MATURITY_LEFT': 12 * 10.6, 'RESIDUAL_MATURITY_RIGHT': 12 * 12, 'WEIGHT': 0.06, 'ASSUMED_RATE': 0.6,
     'SECTION': '3', 'TIME_FRAME': 13},
    {'RESIDUAL_MATURITY_LEFT': 12 * 12, 'RESIDUAL_MATURITY_RIGHT': 12 * 20, 'WEIGHT': 0.08, 'ASSUMED_RATE': 0.6,
     'SECTION': '3', 'TIME_FRAME': 14},
    {'RESIDUAL_MATURITY_LEFT': 12 * 20, 'RESIDUAL_MATURITY_RIGHT': None, 'WEIGHT': 0.125, 'ASSUMED_RATE': 0.6,
     'SECTION': '3', 'TIME_FRAME': 15},
]
PERIOD_WEIGHT_SMALL_TABLE = pd.DataFrame.from_records(PERIOD_WEIGHT_SMALL_DATA)

TIME_FRAME_DATA = [
    {'RESIDUAL_MATURITY_LEFT': 0, 'RESIDUAL_MATURITY_RIGHT': 12, 'SECTION': '1'},
    {'RESIDUAL_MATURITY_LEFT': 12, 'RESIDUAL_MATURITY_RIGHT': 48, 'SECTION': '2'},
    {'RESIDUAL_MATURITY_LEFT': 48, 'RESIDUAL_MATURITY_RIGHT': None, 'SECTION': '3'}
]
TIME_FRAME_TABLE = pd.DataFrame.from_records(TIME_FRAME_DATA)

SAME_TIMEZONE_WEIGHT = {
    1: 0.4,
    2: 0.3,
    3: 0.3
}

MS_RISK_RESIDUAL_MATURITY_DATA = [
    {'RESIDUAL_MATURITY_LEFT': 0, 'RESIDUAL_MATURITY_RIGHT': 6, 'RATIO': 0.0025},
    {'RESIDUAL_MATURITY_LEFT': 6, 'RESIDUAL_MATURITY_RIGHT': 24, 'RATIO': 0.01},
    {'RESIDUAL_MATURITY_LEFT': 24, 'RESIDUAL_MATURITY_RIGHT': None, 'RATIO': 0.016},
]
MS_RISK_RESIDUAL_MATURITY_TABLE = pd.DataFrame.from_records(MS_RISK_RESIDUAL_MATURITY_DATA)


def ms_risk_residual_maturity(
        residual_maturity: [int, float]
) -> float:
    """
    根据剩余期限获取特定市场风险资本计提比率
    :param residual_maturity: 剩余期限
    :return: 市场风险资本计提比率
    """
    for data in MS_RISK_RESIDUAL_MATURITY_TABLE.apply(
            analysis_data, axis=1,
            args=(residual_maturity, MS_RISK_RESIDUAL_MATURITY_TABLE.columns.to_list())).to_dict('records'):
        if not pd.isna(data['RATIO']):
            return data['RATIO']
        else:
            pass


def ms_risk_allowance(ratio: [int, float], rwa_rating: [int, float]) -> pd.DataFrame:
    MS_RISK_ALLOWANCE_RATIO_DATA = [
        {'RISK': '政府证券', 'RATING': 'AAA+', 'RATIO': 0},
        {'RISK': '政府证券', 'RATING': 'AAA', 'RATIO': 0},
        {'RISK': '政府证券', 'RATING': 'AAA-', 'RATIO': 0},
        {'RISK': '政府证券', 'RATING': 'AA+', 'RATIO': 0},
        {'RISK': '政府证券', 'RATING': 'AA', 'RATIO': 0},
        {'RISK': '政府证券', 'RATING': 'AA-', 'RATIO': 0},
        {'RISK': '政府证券', 'RATING': 'A+', 'RATIO': ratio},
        {'RISK': '政府证券', 'RATING': 'A', 'RATIO': ratio},
        {'RISK': '政府证券', 'RATING': 'A-', 'RATIO': ratio},
        {'RISK': '政府证券', 'RATING': 'BBB+', 'RATIO': ratio},
        {'RISK': '政府证券', 'RATING': 'BBB', 'RATIO': ratio},
        {'RISK': '政府证券', 'RATING': 'BBB-', 'RATIO': ratio},
        {'RISK': '政府证券', 'RATING': 'BB+', 'RATIO': 0.08},
        {'RISK': '政府证券', 'RATING': 'BB', 'RATIO': 0.08},
        {'RISK': '政府证券', 'RATING': 'BB-', 'RATIO': 0.08},
        {'RISK': '政府证券', 'RATING': 'B+', 'RATIO': 0.08},
        {'RISK': '政府证券', 'RATING': 'B', 'RATIO': 0.08},
        {'RISK': '政府证券', 'RATING': 'B-', 'RATIO': 0.08},
        {'RISK': '政府证券', 'RATING': '未评级', 'RATIO': 0.08},
        {'RISK': '政府证券', 'RATING': '其他', 'RATIO': 0.12},
        {'RISK': '合格证券', 'RATING': 'AAA+', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'AAA', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'AAA-', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'AA+', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'AA', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'AA-', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'A+', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'A', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'A-', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'BBB+', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'BBB', 'RATIO': ratio},
        {'RISK': '合格证券', 'RATING': 'BBB-', 'RATIO': ratio},
        {'RISK': '其他证券', 'RATING': 'BB+', 'RATIO': rwa_rating / 12.5},
        {'RISK': '其他证券', 'RATING': 'BB', 'RATIO': rwa_rating / 12.5},
        {'RISK': '其他证券', 'RATING': 'BB-', 'RATIO': rwa_rating / 12.5},
        {'RISK': '其他证券', 'RATING': 'B+', 'RATIO': rwa_rating / 12.5},
        {'RISK': '其他证券', 'RATING': 'B', 'RATIO': rwa_rating / 12.5},
        {'RISK': '其他证券', 'RATING': 'B-', 'RATIO': rwa_rating / 12.5},
        {'RISK': '其他证券', 'RATING': '未评级', 'RATIO': rwa_rating / 12.5},
        {'RISK': '其他证券', 'RATING': '其他', 'RATIO': rwa_rating / 12.5}
    ]
    return pd.DataFrame.from_records(MS_RISK_ALLOWANCE_RATIO_DATA)


def ms_risk_allowance_ratio(
        risk_type: str,
        rating: str,
        residual_maturity: [int, float],
        rwa_rating: [int, float]
) -> [float, int, None]:
    """
    特定市场风险计提比率获取
    :param risk_type: 证券类别
    :param rating: 发行主体外部评级
    :param residual_maturity: 剩余期限
    :param rwa_rating: 信用风险权重
    :return: 市场风险计提比率
    """
    ratio = ms_risk_residual_maturity(residual_maturity)
    MS_RISK_ALLOWANCE_RATIO_TABLE = ms_risk_allowance(ratio=ratio, rwa_rating=rwa_rating)
    return MS_RISK_ALLOWANCE_RATIO_TABLE[
        (MS_RISK_ALLOWANCE_RATIO_TABLE['RISK'] == risk_type) & (MS_RISK_ALLOWANCE_RATIO_TABLE['RATING'] == rating)][
        'RATIO'].to_list()[0]


# def get_ms_risk_allowance_ratio(
#         risk_type: str,
#         rating: str,
#         residual_maturity: [int, float]
# ) -> [float, int, None]:
#     """
#     特定市场风险计提比率获取
#     :param risk_type: 证券类别
#     :param rating: 发行主体外部评级
#     :param residual_maturity: 剩余期限
#     :return: 市场风险计提比率
#     """
#     return ms_risk_allowance_ratio(risk_type=risk_type,
#                                    rating=rating,
#                                    residual_maturity=residual_maturity)


class Config:

    def __init__(self, currency: str, partition_key: str, connect: Connect, write_to_database: bool = True):
        self.currency = currency
        self.partition_key = partition_key
        self.partition_date = f"{partition_key[:4]}-{partition_key[4:6]}-{partition_key[6:]}"
        self.connect = connect
        self.cursor = connect.cursor
        self.write_to_database = write_to_database
