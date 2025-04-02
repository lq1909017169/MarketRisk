from Simple.utils.configuration_table import PERIOD_WEIGHT_BIG_TABLE, PERIOD_WEIGHT_SMALL_TABLE, TIME_FRAME_TABLE
from Simple.utils.utils import analysis_data


def period_weight(
        coupon_rate: [int, float],
        residual_maturity: [int, float]
):
    """
    根据票面利率和剩余期限获取对应的风险权重和假定收益率变化
    :param coupon_rate: 票面利率
    :param residual_maturity: 剩余期限
    :return: (风险权重, 假定收益率变化, 区间)
    """
    if coupon_rate >= 0.03:
        weight = PERIOD_WEIGHT_BIG_TABLE.apply(analysis_data, axis=1, args=(
            residual_maturity, PERIOD_WEIGHT_BIG_TABLE.columns.to_list())).dropna(how='all').to_dict('records')[0]

    else:
        weight = PERIOD_WEIGHT_SMALL_TABLE.apply(analysis_data, axis=1, args=(
            residual_maturity, PERIOD_WEIGHT_SMALL_TABLE.columns.to_list())).dropna(how='all').to_dict('records')[0]
    return {'WEIGHT': weight['WEIGHT'], 'ASSUMED_RATE': weight['ASSUMED_RATE'], 'SECTION': weight['SECTION'],
            'TIME_FRAME': weight['TIME_FRAME']}


def time_frame(residual_maturity: [int, float]):
    """
    :param residual_maturity: 剩余期限
    :return:
    """
    return TIME_FRAME_TABLE.apply(analysis_data, axis=1,
                                  args=(residual_maturity, TIME_FRAME_TABLE.columns.to_list())).dropna().to_dict(
        'records')[0]['TIME_ZONE']


def timezone_weight(
        timezone1: int,
        timezone2: int
) -> [float, int]:
    """
    根据两个证劵的剩余期限获取，两个证券之间的权重
    :param timezone1: 证卷1的剩余期限
    :param timezone2: 证卷2的剩余期限
    :return: 权重
    """
    num = abs(timezone1 - timezone2)
    if num == 1:
        weight = 0.4
    else:
        weight = 1
    return weight
