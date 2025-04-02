import pandas as pd


def stock(data: pd.DataFrame):
    """
    :param data: columns=[CommonStockLong, CommonStockShort, ConvertibleBondLong, ConvertibleBondShort,
                          StockPurAgrLong, StockPurAgrShort, StockFutureContrLong, StockFutureContrShort,
                          StockSwapLong, StockSwapShort, FutureTstockindxLong, FutureTstockindxShort,
                          FutureTindstockLong, FutureTindstockShort, OptionTstockindxLong, OptionTstockindxShort,
                          OptionTindstockLong, OptionTindstockShort, OthersLong, OthersShort, Exchange]
    """
    total_amount = {}
    for column in data.columns:
        if column != 'Exchange':
            total_amount[column] = data[column].sum()

    last_total_amount_long = 0
    last_total_amount_short = 0

    for k, v in total_amount.items():
        if 'Long' in k:
            last_total_amount_long = last_total_amount_long + v
        else:
            last_total_amount_short = last_total_amount_short + v

    last_total_amount_long_add_short = last_total_amount_long + last_total_amount_short

    last_total_amount_long_sub_short = last_total_amount_long - last_total_amount_short

    def load_type_collect(series):
        longs = []
        shorts = []
        for c in series.index.to_list():
            if 'Long' in c:
                longs.append(c)
            elif 'Short' in c:
                shorts.append(c)
            else:
                pass
        long_collect = sum([series[l] for l in longs])
        short_collect = sum([series[s] for s in shorts])
        long_add_short = long_collect + short_collect
        specific_risks = long_add_short * 0.08
        long_sub_short = long_collect - short_collect
        general_risks = abs(long_sub_short * 0.08)
        specific_general_amount = specific_risks + general_risks

        return pd.Series(
            [series['Exchange'], long_collect, short_collect, long_add_short, specific_risks, long_sub_short,
             general_risks, specific_general_amount],
            index=['Exchange', 'Long', 'Short', 'LongAddShort', 'SpecificRisks', 'LongSub_short', 'GeneralRisks',
                   'SpecificGeneralAmount'])

    type_collect_data = data.apply(load_type_collect, axis=1)
    specific_risks_amount = type_collect_data['SpecificRisks'].sum()
    general_risks_amount = type_collect_data['GeneralRisks'].sum()
    specific_general_amount = type_collect_data['SpecificGeneralAmount'].sum()
    return specific_general_amount
