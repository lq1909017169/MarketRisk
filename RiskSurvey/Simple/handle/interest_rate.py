from Simple.utils.configuration_table import Config
from Simple.utils.log import log
from Simple.utils.models import BondBasicInfo, TradeInfo, BondMapRisk, SwapBasicInfo, CommoditySwapBasicInfo, \
    FxSwapBasicInfo, FxForwardBasicInfo
from Simple.utils import period_weight
from Simple.utils.utils import get_month_diff, verify, empty_series, empty_df, \
    verify_residual_maturity
import pandas as pd
import datetime


class IrBaseHandler:

    def __init__(self, config: Config):
        self.config = config
        self.columns = ['TRADE_ID', 'RESIDUAL_MATURITY', 'RISK_TYPE', 'RATING', 'COUPON_RATE',
                        'LONG_SHORT_TYPE', 'CONTRACT_TYPE', 'WEIGHT', 'SECTION', 'TIME_FRAME', 'CURRENCY',
                        'IS_DOMESTIC', 'PARTITION_KEY', 'SPECIFIC_RISK', 'START_DATE', 'END_DATE', 'MARKET_VALUE',
                        'BOND_NAME', 'BOND_TYPE', 'BUSINESS_CLASS']

    def run(self):
        pass


class IrBondHandler(IrBaseHandler):

    def __init__(self, config: Config):
        log.info(f'{self.__class__.__name__} start get data')

        super().__init__(config)
        self.data = self.config.connect.to_df(
            f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
            f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' AND "
            f"{TradeInfo.PRODUCT}='DS_Bond_Basic_Info'"
        )

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key}'
                 f' get <{self.data.shape[0]}> pieces of data')

    @staticmethod
    def callback(series, self):
        try:
            bond = self.config.connect.verify(
                self.config.connect.query_one(BondBasicInfo, SYMBOL=series[TradeInfo.SYMBOL],
                                              PARTITION_KEY=self.config.partition_key))[0]
            risk = self.config.connect.verify(
                self.config.connect.query_one(BondMapRisk, BOND_TYPE=bond[BondBasicInfo.BOND_TYPE]))[0]
            rating = series[TradeInfo.RATING]
            coupon_rate = float(bond[BondBasicInfo.COUPON_RATE]) / 100
            start_date = bond[BondBasicInfo.START_DATE]
            end_date = bond[BondBasicInfo.END_DATE]
            bond_name = bond[BondBasicInfo.BOND_NAME]
            bond_type = bond[BondBasicInfo.BOND_TYPE]
            pv = series[TradeInfo.SIM_PV]
            residual_maturity = verify_residual_maturity(
                get_month_diff(self.config.partition_date, end_date))
            long_short = "LONG" if series[TradeInfo.LONG_SHORT] == 'Long' else "SHORT"
            weight = period_weight(coupon_rate=coupon_rate, residual_maturity=residual_maturity)
            return pd.Series(
                [series[TradeInfo.TRADE_ID], residual_maturity, risk[BondMapRisk.RISK_TYPE],
                 rating, coupon_rate, long_short, "BOND", weight['WEIGHT'],
                 weight['SECTION'], weight['TIME_FRAME'], bond[BondBasicInfo.CURRENCY], 'Y',
                 self.config.partition_key, "Y", start_date, end_date, float(pv) / 10000, bond_name, bond_type, 'BOND'],
                index=self.columns)
        except BaseException as e:
            log.error(f"{self.__class__.__name__} :{e}")
            return empty_series(self.columns)

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


class IrSwapHandler(IrBaseHandler):

    def __init__(self, config: Config):
        super().__init__(config)
        log.info(f'{self.__class__.__name__} start get data')

        self.data = self.config.connect.to_df(
            f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
            f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' and "
            f"{TradeInfo.PRODUCT}='DS_Swap_Basic_Info'"
        )

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key}'
                 f' get <{self.data.shape[0]}> pieces of data')

    @staticmethod
    def callback(series, self):
        try:
            swap = self.config.connect.verify(self.config.connect.query_one(
                SwapBasicInfo, TRADE_ID=series[TradeInfo.TRADE_ID],
                PARTITION_KEY=self.config.partition_key
            ))[0]
            risk_type = 'IR'
            rating = ''
            start_date = swap[SwapBasicInfo.EFFECTIVE_DATE]
            end_date = swap[SwapBasicInfo.MATURITY_DATE]
            pay_pv = swap[SwapBasicInfo.PAY_PV]
            receive_pv = swap[SwapBasicInfo.RECEIVE_PV]
            business_class = 'IR_SWAP'
            is_domestic = "Y"
            if swap[SwapBasicInfo.PAY_LEG_TYPE] == 'FIXED':
                pay_rate = float(swap[SwapBasicInfo.PAY_RATE]) / 100
                pay_residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, end_date=end_date))
                pay_weight = period_weight(coupon_rate=pay_rate, residual_maturity=pay_residual_maturity)
                pay_map = [series[TradeInfo.TRADE_ID], pay_residual_maturity,
                           risk_type, rating, pay_rate, 'SHORT', 'IR', pay_weight['WEIGHT'],
                           pay_weight['SECTION'], pay_weight['TIME_FRAME'], swap[SwapBasicInfo.PAY_CURRENCY],
                           is_domestic, self.config.partition_key, "N", start_date, end_date, float(pay_pv) / 10000,
                           '', '', business_class]
                pay_series = pd.Series(pay_map, index=self.columns)
            else:
                pay_rate = float(swap[SwapBasicInfo.PAY_RATE]) / 100
                new_end_date = swap[SwapBasicInfo.PAY_FREQUENCY]
                pay_residual_maturity = get_month_diff(
                    self.config.partition_date,
                    new_end_date,
                )
                pay_weight = period_weight(coupon_rate=pay_rate,
                                           residual_maturity=pay_residual_maturity)
                pay_map = [series[TradeInfo.TRADE_ID], pay_residual_maturity,
                           risk_type, rating, pay_rate, 'SHORT', 'IR',
                           pay_weight['WEIGHT'], pay_weight['SECTION'], pay_weight['TIME_FRAME'],
                           swap[SwapBasicInfo.PAY_CURRENCY], is_domestic, self.config.partition_key, "N",
                           start_date, new_end_date, float(pay_pv) / 10000, '', '', business_class]
                pay_series = pd.Series(pay_map, index=self.columns)
            if swap[SwapBasicInfo.RECEIVE_LEG_TYPE] == 'FIXED':
                receive_rate = float(swap[SwapBasicInfo.RECEIVE_RATE]) / 100
                receive_residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, end_date))
                receive_weight = period_weight(coupon_rate=receive_rate, residual_maturity=receive_residual_maturity)
                receive_map = [series[TradeInfo.TRADE_ID],
                               receive_residual_maturity, risk_type, rating,
                               receive_rate, 'LONG', 'IR',
                               receive_weight['WEIGHT'], receive_weight['SECTION'], receive_weight['TIME_FRAME'],
                               swap[SwapBasicInfo.RECEIVE_CURRENCY], is_domestic,
                               self.config.partition_key, "N", start_date, end_date, float(receive_pv) / 10000, '', '',
                               business_class]
                receive_series = pd.Series(receive_map, index=self.columns)
            else:
                receive_rate = float(swap[SwapBasicInfo.RECEIVE_RATE]) / 100
                new_end_date = swap[SwapBasicInfo.RECEIVE_FREQUENCY]
                receive_residual_maturity = get_month_diff(
                    self.config.partition_date,
                    new_end_date,
                )
                receive_weight = period_weight(coupon_rate=receive_rate,
                                               residual_maturity=receive_residual_maturity)
                receive_map = [series[TradeInfo.TRADE_ID],
                               receive_residual_maturity, risk_type, rating,
                               receive_rate, 'LONG', 'IR', receive_weight['WEIGHT'], receive_weight['SECTION'],
                               receive_weight['TIME_FRAME'], swap[SwapBasicInfo.RECEIVE_CURRENCY], is_domestic,
                               self.config.partition_key, "N", start_date, new_end_date,
                               float(receive_pv) / 10000, '', '', business_class]
                receive_series = pd.Series(receive_map, index=self.columns)
            return pd.concat([pay_series, receive_series], axis=1).T
        except BaseException as e:
            log.error(f"{self.__class__.__name__}: {series[TradeInfo.TRADE_ID]}:{e}")

            return empty_df(self.columns)

    def run(self):
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} start split data')

        if self.data.empty:
            result = empty_df(self.columns).dropna(how='all')
        else:
            result = pd.concat(verify(data=self.data,
                                      columns=self.columns,
                                      func=self.callback,
                                      args=(self,),
                                      axis=1).values).dropna(how='all')

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} split over，'
                 f'split <{result.shape[0]}> positions')
        return result


class IrFxForWardHandler(IrBaseHandler):

    def __init__(self, config: Config):
        super().__init__(config)
        log.info(f'{self.__class__.__name__} start get data')

        self.data = self.config.connect.to_df(
            f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
            f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' and "
            f"{TradeInfo.PRODUCT}='DS_FX_Forward_Basic_Info'"
        )

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key}'
                 f' get <{self.data.shape[0]}> pieces of data')

    @staticmethod
    def callback(series, self):
        try:
            is_domestic = "Y"
            business_class = 'FX_FORWARD'
            fx_forward = self.config.connect.verify(
                self.config.connect.query_one(
                    FxForwardBasicInfo, TRADE_ID=series[TradeInfo.TRADE_ID],
                    PARTITION_KEY=self.config.partition_key
                )
            )[0]
            start_date = fx_forward[FxForwardBasicInfo.START_DATE]
            end_date = fx_forward[FxForwardBasicInfo.MATURITY_DATE]
            risk_type = 'IR'
            rating = ''
            residual_maturity = verify_residual_maturity(get_month_diff(self.config.partition_date, end_date))
            buy_pv = fx_forward[FxForwardBasicInfo.BUY_PV]
            pay_pv = fx_forward[FxForwardBasicInfo.PAY_PV]
            buy_currency = fx_forward[FxForwardBasicInfo.BUY_CURRENCY]
            pay_currency = fx_forward[FxForwardBasicInfo.PAY_CURRENCY]
            weight = period_weight(coupon_rate=0, residual_maturity=residual_maturity)
            buy = [series[TradeInfo.TRADE_ID], residual_maturity, risk_type, rating, 0, 'LONG', 'IR',
                   weight['WEIGHT'], weight['SECTION'], weight['TIME_FRAME'], buy_currency, is_domestic,
                   self.config.partition_key, "N", start_date, end_date, float(buy_pv) / 10000, '', '', business_class]
            buy_series = pd.Series(buy, index=self.columns)
            pay = [series[TradeInfo.TRADE_ID], residual_maturity, risk_type, rating, 0, 'SHORT', 'IR',
                   weight['WEIGHT'], weight['SECTION'], weight['TIME_FRAME'], pay_currency, is_domestic,
                   self.config.partition_key, "N", start_date, end_date, float(pay_pv) / 10000, '', '', business_class]
            pay_series = pd.Series(pay, index=self.columns)
            return pd.concat([buy_series, pay_series], axis=1).T
        except BaseException as e:
            log.error(f"{self.__class__.__name__}: {series[TradeInfo.TRADE_ID]} :{e}")
            return empty_series(self.columns)

    def run(self):
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} start split data')

        if self.data.empty:
            result = empty_df(self.columns).dropna(how='all')
        else:
            result = pd.concat(verify(data=self.data,
                                      columns=self.columns,
                                      func=self.callback,
                                      args=(self,),
                                      axis=1).values).dropna(how='all')

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} split over，'
                 f'split <{result.shape[0]}> positions')
        return result


class IrFxSwapHandler(IrBaseHandler):

    def __init__(self, config: Config):
        super().__init__(config)
        log.info(f'{self.__class__.__name__} start get data')

        self.data = self.config.connect.to_df(
            f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
            f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' and "
            f"{TradeInfo.PRODUCT}='DS_FX_Swap_Basic_Info'"
        )

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key}'
                 f' get <{self.data.shape[0]}> pieces of data')

    @staticmethod
    def callback(series, self):
        try:
            is_domestic = "Y"
            fx_swap = self.config.connect.verify(
                self.config.connect.query_one(
                    FxSwapBasicInfo, TRADE_ID=series[TradeInfo.TRADE_ID],
                    PARTITION_KEY=self.config.partition_key
                )
            )[0]
            business_class = 'FX_SWAP'
            start_date = fx_swap[FxSwapBasicInfo.EFFECTIVE_DATE]
            start = start_date[:10]
            end_date = fx_swap[FxSwapBasicInfo.PAY_DATE]
            risk_type = 'IR'
            rating = ''

            purchase_pv = fx_swap[FxSwapBasicInfo.PURCHASE_PV]
            purchase_far_pv = fx_swap[FxSwapBasicInfo.PURCHASE_FAR_PV]
            sale_pv = fx_swap[FxSwapBasicInfo.SALE_PV]
            sale_far_pv = fx_swap[FxSwapBasicInfo.SALE_FAR_PV]
            purchase_currency = fx_swap[FxSwapBasicInfo.PURCHASE_CCY]
            sale_currency = fx_swap[FxSwapBasicInfo.SALE_CCY]
            sale_far_currency = fx_swap[FxSwapBasicInfo.FAR_SALE_CCY]
            purchase_far_currency = fx_swap[FxSwapBasicInfo.FAR_PURCHASE_CCY]

            if start > self.config.partition_date:
                residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, start_date))
                weight = period_weight(coupon_rate=0, residual_maturity=residual_maturity)

                far_residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, end_date))
                far_weight = period_weight(coupon_rate=0, residual_maturity=far_residual_maturity)
                purchase = [series[TradeInfo.TRADE_ID], residual_maturity, risk_type, rating, 0, 'LONG', 'IR',
                            weight['WEIGHT'], weight['SECTION'], weight['TIME_FRAME'], purchase_currency,
                            is_domestic, self.config.partition_key, "N", start_date, end_date,
                            float(purchase_pv) / 10000, '', '', business_class]
                sale_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'SHORT', 'IR',
                            far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                            sale_far_currency, is_domestic, self.config.partition_key, "N", start_date, end_date,
                            float(sale_far_pv) / 10000, '', '', business_class]
                sale = [series[TradeInfo.TRADE_ID], residual_maturity, risk_type, rating, 0, 'SHORT', 'IR',
                        weight['WEIGHT'], weight['SECTION'], weight['TIME_FRAME'], sale_currency,
                        is_domestic, self.config.partition_key, "N", start_date, end_date, float(sale_pv) / 10000,
                        '', '', business_class]
                purchase_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'LONG',
                                'IR', far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                                purchase_far_currency, is_domestic, self.config.partition_key, "N", start_date,
                                end_date, float(purchase_far_pv) / 10000, '', '', business_class]
                purchase_far_series = pd.Series(purchase_far, index=self.columns)
                sale_series = pd.Series(sale, index=self.columns)
                purchase_series = pd.Series(purchase, index=self.columns)
                sale_far_series = pd.Series(sale_far, index=self.columns)
                result = pd.concat([purchase_series, sale_far_series, purchase_far_series, sale_series], axis=1).T
            else:
                far_residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, end_date))
                far_weight = period_weight(coupon_rate=0, residual_maturity=far_residual_maturity)
                sale_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'SHORT', 'IR',
                            far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                            sale_far_currency, is_domestic, self.config.partition_key, "N", start_date, end_date,
                            float(sale_far_pv) / 10000, '', '', business_class]
                purchase_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'LONG',
                                'IR', far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                                purchase_far_currency, is_domestic, self.config.partition_key, "N", start_date,
                                end_date, float(purchase_far_pv) / 10000, '', '', business_class]
                purchase_far_series = pd.Series(purchase_far, index=self.columns)
                sale_far_series = pd.Series(sale_far, index=self.columns)
                result = pd.concat([sale_far_series, purchase_far_series], axis=1).T
            return result
        except BaseException as e:
            log.error(f"{self.__class__.__name__}: {series[TradeInfo.TRADE_ID]}: {e}")
            return empty_df(self.columns)

    def run(self):
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} start split data')

        if self.data.empty:
            result = empty_df(self.columns).dropna(how='all')
        else:
            result = pd.concat(verify(data=self.data,
                                      columns=self.columns,
                                      func=self.callback,
                                      args=(self,),
                                      axis=1).values).dropna(how='all')

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} split over，'
                 f'split <{result.shape[0]}> positions')
        return result


class IrCommoditySwapHandler(IrBaseHandler):

    def __init__(self, config: Config):
        super().__init__(config)
        log.info(f'{self.__class__.__name__} start get data')

        self.data = self.config.connect.to_df(
            f"SELECT * FROM {TradeInfo.__table_name__} WHERE "
            f"{TradeInfo.PARTITION_KEY}='{self.config.partition_key}' and "
            f"{TradeInfo.PRODUCT}='DS_Commodity_Swap_Basic_Info'")

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key}'
                 f' get <{self.data.shape[0]}> pieces of data')

    @staticmethod
    def callback(series, self):
        try:
            is_domestic = "Y"
            business_class = 'COMMODITY_SWAP'
            commodity = self.config.connect.verify(
                self.config.connect.query_one(
                    CommoditySwapBasicInfo, TRADE_ID=series[TradeInfo.TRADE_ID],
                    PARTITION_KEY=self.config.partition_key
                )
            )[0]
            start_date = commodity[CommoditySwapBasicInfo.EFFECTIVE_DATE]
            start = start_date[:10]
            end_date = commodity[CommoditySwapBasicInfo.PAY_DATE]
            risk_type = 'IR'
            rating = ''

            purchase_pv = commodity[CommoditySwapBasicInfo.PURCHASE_PV]
            purchase_far_pv = commodity[CommoditySwapBasicInfo.PURCHASE_FAR_PV]
            sale_pv = commodity[CommoditySwapBasicInfo.SALE_PV]
            sale_far_pv = commodity[CommoditySwapBasicInfo.SALE_FAR_PV]
            purchase_currency = commodity[CommoditySwapBasicInfo.PURCHASE_COMMODITY]
            sale_currency = commodity[CommoditySwapBasicInfo.SALE_COMMODITY]
            sale_far_currency = commodity[CommoditySwapBasicInfo.FAR_SALE_COMMODITY]
            purchase_far_currency = commodity[CommoditySwapBasicInfo.FAR_PURCHASE_COMMODITY]
            if start > self.config.partition_date:
                residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, start_date))
                weight = period_weight(coupon_rate=0, residual_maturity=residual_maturity)

                far_residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, end_date))
                far_weight = period_weight(coupon_rate=0, residual_maturity=far_residual_maturity)
                purchase = [series[TradeInfo.TRADE_ID], residual_maturity, risk_type, rating, 0, 'LONG', 'IR',
                            weight['WEIGHT'], weight['SECTION'], weight['TIME_FRAME'], purchase_currency,
                            is_domestic, self.config.partition_key, "N", start_date, end_date,
                            float(purchase_pv) / 10000, '', '', business_class]
                sale_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'SHORT', 'IR',
                            far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                            sale_far_currency, is_domestic, self.config.partition_key, "N", start_date, end_date,
                            float(sale_far_pv) / 10000, '', '', business_class]
                sale = [series[TradeInfo.TRADE_ID], residual_maturity, risk_type, rating, 0, 'SHORT', 'IR',
                        weight['WEIGHT'], weight['SECTION'], weight['TIME_FRAME'], sale_currency,
                        is_domestic, self.config.partition_key, "N", start_date, end_date, float(sale_pv) / 10000,
                        '', '', business_class]
                purchase_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'LONG',
                                'IR', far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                                purchase_far_currency, is_domestic, self.config.partition_key, "N", start_date,
                                end_date, float(purchase_far_pv) / 10000, '', '', business_class]
                purchase_far_series = pd.Series(purchase_far, index=self.columns)
                sale_series = pd.Series(sale, index=self.columns)
                purchase_series = pd.Series(purchase, index=self.columns)
                sale_far_series = pd.Series(sale_far, index=self.columns)
                return pd.concat([purchase_series, sale_far_series, purchase_far_series, sale_series], axis=1).T
            else:
                far_residual_maturity = verify_residual_maturity(
                    get_month_diff(self.config.partition_date, end_date))
                far_weight = period_weight(coupon_rate=0, residual_maturity=far_residual_maturity)
                sale_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'SHORT', 'IR',
                            far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                            sale_far_currency, is_domestic, self.config.partition_key, "N", start_date, end_date,
                            float(sale_far_pv) / 10000, '', '', business_class]
                purchase_far = [series[TradeInfo.TRADE_ID], far_residual_maturity, risk_type, rating, 0, 'LONG',
                                'IR', far_weight['WEIGHT'], far_weight['SECTION'], far_weight['TIME_FRAME'],
                                purchase_far_currency, is_domestic, self.config.partition_key, "N", start_date,
                                end_date, float(purchase_far_pv) / 10000, '', '', business_class]
                purchase_far_series = pd.Series(purchase_far, index=self.columns)
                sale_far_series = pd.Series(sale_far, index=self.columns)
                return pd.concat([sale_far_series, purchase_far_series], axis=1).T
        except BaseException as e:
            log.error(f"{self.__class__.__name__}: {series[TradeInfo.TRADE_ID]}: {e}")
            return empty_df(self.columns)

    def run(self):
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} start split data')

        if self.data.empty:
            result = empty_df(self.columns).dropna(how='all')
        else:
            result = pd.concat(verify(data=self.data,
                                      columns=self.columns,
                                      func=self.callback,
                                      args=(self,),
                                      axis=1).values).dropna(how='all')

        log.info(f'{self.__class__.__name__} PARTITION_KEY-{self.config.partition_key} split over，'
                 f'split <{result.shape[0]}> positions')
        return result


class InterestRateHandler:

    def __init__(self, config: Config):
        start = datetime.datetime.now()
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key} start')

        bond = IrBondHandler(config=config).run()
        swap = IrSwapHandler(config=config).run()
        fx_swap = IrFxSwapHandler(config=config).run()
        fx_forward = IrFxForWardHandler(config=config).run()
        commodity = IrCommoditySwapHandler(config=config).run()

        df = pd.concat([bond, swap, fx_forward, fx_swap, commodity], axis=0).reset_index(drop=True)
        ir_df = pd.concat([bond, swap], axis=0).reset_index(drop=True)
        fx_df = pd.concat([fx_forward, fx_swap], axis=0).reset_index(drop=True)
        self.data = df[df['RESIDUAL_MATURITY'] != -1]
        self.ir_data = ir_df[ir_df['RESIDUAL_MATURITY'] != -1]
        self.fx_data = fx_df[fx_df['RESIDUAL_MATURITY'] != -1]
        self.commodity_data = commodity[commodity['RESIDUAL_MATURITY'] != -1]
        end = datetime.datetime.now()
        log.info(f'{self.__class__.__name__} PARTITION_KEY-{config.partition_key} over，'
                 f'split {self.__class__.__name__} <{self.data.shape[0]}> positions, runtime__{end - start}')


