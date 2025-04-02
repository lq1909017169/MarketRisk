from Simple.utils.configuration_table import Config
from Simple.utils.exchange import ExchangeManage, Exchange
from Simple.utils.log import log
from Simple.utils.models import TradeInfo, CommoditySwapBasicInfo
from Simple.utils.utils import verify, empty_df
import pandas as pd


class CommodityBaseHandler:

    def __init__(self, config: Config):
        self.config = config
        self.columns = ['TRADE_ID', 'COMMODITY_TYPE', 'LONG_SHORT_TYPE', 'MARKET_VALUE', 'PARTITION_KEY',
                        'MATURITY_DATE']


# class CommodityForwardHandler(CommodityBaseHandler):
#
#     def __init__(self, config: Config):
#         super().__init__(config)
#         self.data = self.config.connect.to_df(f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
#                                               f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' and "
#                                               f"{TradeInfo.PRODUCT}='%COMMODITYFORWARD%'")
#
#     @staticmethod
#     def callback(series, self):
#         commodity_forward = self.config.connect.verify(
#             self.config.connect.query_one(
#                 CommodityForwardBasicInfo, TRADE_ID=series[TradeInfo.TRADE_ID]
#             )
#         )[0]
#
#         commodity_price = self.config.connect.verify(
#             self.config.connect.query_one(
#                 CommodityPrice, SYMBOL=commodity_forward[CommodityForwardBasicInfo.COMMODITY],
#                 TAG=commodity_forward[CommodityForwardBasicInfo.CURRENCY],
#                 TYPE='close'
#             ))[0]
#         if commodity_forward[CommodityForwardBasicInfo] == self.config.currency:
#             pv = series[TradeInfo.NOTIONAL] * commodity_price[CommodityPrice.VALUE]
#         else:
#             exchange_manage = ExchangeManage(config=self.config)
#             pv = Exchange(
#                 exchange_manage, commodity_forward[CommodityForwardBasicInfo.CURRENCY],
#                 series[TradeInfo.NOTIONAL] * commodity_price[CommodityPrice.VALUE]).rate()
#         if commodity_forward[CommodityForwardBasicInfo.BUY_SELL] == 'buy':
#             result = [commodity_forward[CommodityForwardBasicInfo.TRADE_ID],
#                       commodity_forward[CommodityForwardBasicInfo.COMMODITY],
#                       'LONG', pv]
#         else:
#             result = [commodity_forward[CommodityForwardBasicInfo.TRADE_ID],
#                       commodity_forward[CommodityForwardBasicInfo.COMMODITY],
#                       'SHORT', pv]
#         return pd.Series(result, index=self.columns)
#
#     def run(self):
#         return verify(data=self.data,
#                       columns=self.columns,
#                       func=self.callback,
#                       args=(self,),
#                       axis=1).dropna()


def exchange_rate(config, exchange_manage, currency, market_value):
    currency_map = {
        'HKD': 'HKD',
        'XAU': 'XAU',
        'JPY': 'JPY',
        'AUY': 'AU99.99',
        'XAG': 'XAG',
        'CAD': 'CAD',
        'USD': 'USD',
        'GBP': 'GBP',
        'CNY': 'CNY',
        'AUD': 'AUD',
        'EUR': 'EUR'
    }
    if currency != config.currency and currency != 'XAG':
        value = Exchange(exchange_manage=exchange_manage,
                         target_currency=currency_map[currency],
                         source_currency=config.currency,
                         foreign=market_value).rate()
    elif currency == 'XAG':
        rate = Exchange(exchange_manage=exchange_manage,
                        target_currency=currency_map[currency],
                        source_currency='USD',
                        foreign=market_value).rate()
        value = Exchange(exchange_manage=exchange_manage,
                         target_currency='USD',
                         source_currency=config.currency,
                         foreign=rate).rate()
    else:
        value = market_value
    return value


class CommoditySwapHandler(CommodityBaseHandler):

    def __init__(self, config: Config):
        super().__init__(config)
        log.info(f'{self.__class__.__name__} start get data')

        self.data = self.config.connect.to_df(
            f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
            f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' and "
            f"{TradeInfo.PRODUCT}='DS_Commodity_Swap_Basic_Info'")

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key}'
                 f' get <{self.data.shape[0]}> pieces of data')
        self.exchange_manage = ExchangeManage(config=config)

    @staticmethod
    def callback(series, self):
        commodity_swap = self.config.connect.verify(
            self.config.connect.query_one(
                CommoditySwapBasicInfo,
                TRADE_ID=series[TradeInfo.TRADE_ID]
            ))[0]
        buy_sell = commodity_swap[CommoditySwapBasicInfo.BUY_SELL]

        if buy_sell == 'B':
            market_value = commodity_swap[CommoditySwapBasicInfo.PURCHASE_FAR_PV]
            currency = commodity_swap[CommoditySwapBasicInfo.FAR_PURCHASE_COMMODITY]
            market_value = exchange_rate(self.config, self.exchange_manage, currency, market_value)
            end_date = commodity_swap[CommoditySwapBasicInfo.PAY_DATE]
            result = [commodity_swap[TradeInfo.TRADE_ID], currency, 'LONG', market_value / 10000,
                      self.config.partition_key, end_date]
        else:
            market_value = commodity_swap[CommoditySwapBasicInfo.SALE_FAR_PV]
            currency = commodity_swap[CommoditySwapBasicInfo.FAR_SALE_COMMODITY]
            market_value = exchange_rate(self.config, self.exchange_manage, currency, market_value)
            end_date = commodity_swap[CommoditySwapBasicInfo.PAY_DATE]
            result = [commodity_swap[TradeInfo.TRADE_ID], currency, 'SHORT', market_value / 10000,
                      self.config.partition_key, end_date]

        return pd.Series(result, index=self.columns)

    def run(self):
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} start split data')

        if self.data.empty:
            result = empty_df(self.columns).dropna(how='all')
        else:
            result = verify(self.data,
                            self.columns,
                            func=self.callback,
                            args=(self,),
                            axis=1).dropna(how='all')

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} split over，'
                 f'split <{result.shape[0]}> positions')
        return result


class CommodityBaseHandler:

    def __init__(self, config: Config):
        swap = CommoditySwapHandler(config=config).run()
        self.data = pd.concat([swap], axis=0).reset_index(drop=True)
