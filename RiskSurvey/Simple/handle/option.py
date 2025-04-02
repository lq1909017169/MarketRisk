from Simple.utils.configuration_table import Config
from Simple.utils.exchange import Exchange, ExchangeManage
from Simple.utils.log import log
from Simple.utils.models import TradeInfo, FxOptionBasicInfo
from Simple.utils.utils import empty_series, verify
import pandas as pd
import datetime


class OptionBaseHandler:

    def __init__(self, config: Config):
        self.config = config
        self.columns = ['TRADE_ID', 'CURRENCY', 'DELTA', 'GAMMA', 'VEGA', 'VOL', 'VU',
                        'PARTITION_KEY']
        # self.columns = ['TRADE_ID', 'CURRENCY', 'DELTA', 'GAMMA', 'VEGA', 'VOL', 'VU', 'UNDERLYING_REF',
        #                 'PARTITION_KEY']


class FxOptionHandler(OptionBaseHandler):

    def __init__(self, config: Config):
        super().__init__(config)
        log.info(f'{self.__class__.__name__} start get data')

        self.data = self.config.connect.to_df(
            f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
            f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' AND "
            f"{TradeInfo.PRODUCT}='DS_FX_Option_Basic_Info'"
        )

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key}'
                 f' get <{self.data.shape[0]}> pieces of data')
        self.exchange_manage = ExchangeManage(config=config)

    @staticmethod
    def callback(series, self):
        try:
            fx_option = self.config.connect.verify(
                self.config.connect.query_one(FxOptionBasicInfo, TRADE_ID=series[TradeInfo.TRADE_ID],
                                              PARTITION_KEY=self.config.partition_key)
            )[0]
            # if fx_option[FxOptionBasicInfo.UNDERLYING_CURRENCY] != self.config.currency:
            #     vu = fx_option[FxOptionBasicInfo.STRIKE_PRICE] * 0.08
            #     result = [fx_option[FxOptionBasicInfo.TRADE_ID], fx_option[FxOptionBasicInfo.UNDERLYING_CURRENCY],
            #               fx_option[FxOptionBasicInfo.DELTA], fx_option[FxOptionBasicInfo.GAMMA],
            #               fx_option[FxOptionBasicInfo.VEGA], fx_option[FxOptionBasicInfo.VOL], vu,
            #               self.config.partition_key]
            # else:
            #     vu = 1 * 0.08
            #     result = [fx_option[FxOptionBasicInfo.TRADE_ID], fx_option[FxOptionBasicInfo.UNDERLYING_CURRENCY],
            #               fx_option[FxOptionBasicInfo.DELTA], fx_option[FxOptionBasicInfo.GAMMA],
            #               fx_option[FxOptionBasicInfo.VEGA], fx_option[FxOptionBasicInfo.VOL], vu,
            #               self.config.partition_key]
            vu = fx_option[FxOptionBasicInfo.STRIKE_PRICE] * 0.08
            result = [fx_option[FxOptionBasicInfo.TRADE_ID], fx_option[FxOptionBasicInfo.UNDERLYING_CURRENCY],
                      fx_option[FxOptionBasicInfo.DELTA], fx_option[FxOptionBasicInfo.GAMMA],
                      fx_option[FxOptionBasicInfo.VEGA], fx_option[FxOptionBasicInfo.VOL], vu,
                      self.config.partition_key]
            # result = [fx_option[FxOptionBasicInfo.TRADE_ID], fx_option[FxOptionBasicInfo.UNDERLYING_CURRENCY],
            #           fx_option[FxOptionBasicInfo.DELTA], fx_option[FxOptionBasicInfo.GAMMA],
            #           fx_option[FxOptionBasicInfo.VEGA], fx_option[FxOptionBasicInfo.VOL], self.config.partition_key]
            return pd.Series(result, index=self.columns)
        except BaseException as e:
            log.error(f"{self.__class__.__name__}: {e}")
            return empty_series(self.columns)

    def run(self):
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} start split data')

        result = verify(self.data,
                        self.columns,
                        func=self.callback,
                        args=(self,),
                        axis=1).dropna(how='all')

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} split over，'
                 f'split <{result.shape[0]}> positions')
        return result


class OptionHandler:

    def __init__(self, config: Config):
        start = datetime.datetime.now()
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key} start')

        option = FxOptionHandler(config=config).run()
        self.data = pd.concat([option], axis=0).reset_index(drop=True)

        end = datetime.datetime.now()
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key} over，'
                 f'split {self.__class__.__name__} <{self.data.shape[0]}> positions, runtime__{end - start}')
