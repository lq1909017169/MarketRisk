import pandas as pd
from Simple.utils.configuration_table import Config
from Simple.utils.log import log
from Simple.utils.models import ForeignExchange


class ExchangeManage:

    def __init__(self, config: Config):
        self.config = config
        self.exchange = self.config.connect.to_df(
            f"SELECT * FROM {ForeignExchange.__table_name__} WHERE "
            f"{ForeignExchange.PARTITION_KEY}='{config.partition_key}'")
        self.columns = ['BASE', 'TAG', 'CHAR', 'RATE']

    def manage(self) -> pd.DataFrame:
        def load_manage(series):
            just = pd.Series(
                [series[ForeignExchange.SOURCE], series[ForeignExchange.TARGET], '*', series[ForeignExchange.VALUE]],
                index=self.columns)
            against = pd.Series(
                [series[ForeignExchange.TARGET], series[ForeignExchange.SOURCE], '/', series[ForeignExchange.VALUE]],
                index=self.columns)
            return pd.concat([just, against], axis=1).T

        if self.exchange.shape[0] == 0:
            log.error(f'{self.config.partition_key}, 当前日期的汇率数据不存在')
            return pd.DataFrame({c: [] for c in self.columns})
        return pd.concat(self.exchange.apply(load_manage, axis=1).to_list(), ignore_index=True)

    def manage_map(self):
        manage_map = []
        manage_list = self.manage().to_dict('records')
        for m in manage_list:
            manage_map.append({'BASE': m['BASE'], 'TAG': m['TAG']})
        return manage_map


class Exchange:

    def __init__(self, exchange_manage: ExchangeManage, source_currency: str, target_currency: str,
                 foreign: [int, float]):
        """
        :param exchange_manage: 汇率数据信息， ExchangeManage.currency是目标币种
        :param source_currency: 原币种
        :param target_currency: 目标币种
        :param foreign: 原币种的数值
        """
        self.exchange_manage = exchange_manage
        self.manage = exchange_manage.manage()
        self.source_currency = source_currency
        self.target_currency = target_currency
        self.foreign = foreign

    def rate(self):
        currency_map = {'BASE': self.target_currency, 'TAG': self.source_currency}
        try:
            row = self.manage[(self.manage['BASE'] == currency_map['BASE']) &
                              (self.manage['TAG'] == currency_map['TAG'])].to_dict('records')[0]
            if row['CHAR'] == '/':
                return self.foreign / eval(row['RATE'])
            else:
                return self.foreign * eval(row['RATE'])
        except BaseException as e:
            log.error(
                f"数据库中不存在{self.exchange_manage.config.currency}和{self.source_currency}之间的汇率数据, 错误: {e}")
