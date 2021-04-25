# -*- coding: utf-8 -*-
import datetime

import pandas as pd

from rqdatac.validators import (
    check_items_in_container,
    ensure_date_int,
    ensure_order_book_ids,
    ensure_string,
    ensure_string_in,
    ensure_date_range,
)

from rqdatac import get_trading_dates
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api
from rqdatac.services.basic import all_instruments, instruments
from rqdatac.services.get_price import get_price
from rqdatac.utils import is_panel_removed

VALID_GREEKS_FIELDS = ['iv', 'delta', 'gamma', 'vega', 'theta', 'rho']


@export_as_api(namespace='options')
def get_greeks(order_book_ids, start_date=None, end_date=None, fields=None, model='implied_forward', market="cn"):
    """获取指定股票期权的隐含波动率iv， 以及5个希腊字母数值(delta, gamma, bega, theta, rho)
    :param order_book_ids: 股票 order_book_id or order_book_id list
    :param start_date: 开始日期, 必要参数
    :param end_date: 结束日期；默认为今天
    :param fields: str或list类型. 默认为None, 返回所有fields.
    :param model: str类型， last: 代表用每日close价格， implied_forward 代表用隐含风险收益率计算
    :param market: 默认值为"cn"
    """

    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    check_items_in_container(model, ['implied_forward', 'last'], 'model')
    if start_date is not None:
        start_date = ensure_date_int(start_date)
    else:
        raise ValueError('start_date is expected')
    if end_date is not None:
        end_date = ensure_date_int(end_date)
    else:
        end_date = ensure_date_int(datetime.datetime.now().date())
    if end_date < start_date:
        raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))

    if fields is None:
        fields = VALID_GREEKS_FIELDS
    else:
        check_items_in_container(fields, VALID_GREEKS_FIELDS, 'Greeks')

    data = get_client().execute("options.get_greeks", order_book_ids, start_date, end_date, fields, model, market=market)
    if not data:
        return None

    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "trading_date"], inplace=True)
    df.sort_index(inplace=True)
    return df[fields]


SPECIAL_UNDERLYING_SYMBOL = ("510050.XSHG", "510300.XSHG", "159919.XSHE")


@export_as_api(namespace='options')
def get_contracts(
    underlying,
    option_type=None,
    maturity=None,
    strike=None,
    trading_date=None
):
    """返回符合条件的期权

    :param underlying: 标的合约, 可以填写'M'代表期货品种的字母；也可填写'M1901'这种具体 order_book_id
    :param option_type: 期权类型, 'C'代表认购期权, 'P'代表认沽期权合约, 默认返回全部
    :param maturity: 到期月份, 如'1811'代表期权18年11月到期, 默认返回全部到期月份
    :param strike: 行权价, 向左靠档, 默认返回全部行权价
    :param trading_date: 查询日期, 默认返回当前全部

    :returns
        返回order_book_id list；如果无符合条件期权则返回空list[]
    """
    underlying = ensure_string(underlying, "underlying").upper()
    instruments_df = all_instruments(type='Option')
    underlying_symbols = instruments_df.underlying_symbol.unique()
    underlying_order_book_ids = instruments_df.underlying_order_book_id.unique()
    instruments_df = all_instruments(type='Option', date=trading_date)
    if underlying in underlying_symbols:
        instruments_df = instruments_df[instruments_df.underlying_symbol == underlying]
    elif underlying in underlying_order_book_ids:
        instruments_df = instruments_df[instruments_df.underlying_order_book_id == underlying]
    else:
        raise ValueError("Unknown underlying")
    if instruments_df.empty:
        return []

    if option_type is not None:
        option_type = ensure_string(option_type, "option_type").upper()
        ensure_string_in(option_type, {'P', 'C'}, "option_type")
        instruments_df = instruments_df[instruments_df.option_type == option_type]

    if maturity is not None:
        maturity = int(maturity)
        month = maturity % 100
        if month not in range(1, 13):
            raise ValueError("Unknown month")
        year = maturity // 100 + 2000
        str_month = str(month)
        if len(str_month) == 1:
            str_month = '0' + str_month
        date_str = str(year) + '-' + str_month
        instruments_df = instruments_df[instruments_df.maturity_date.str.startswith(date_str)]
        if instruments_df.empty:
            return []

    if strike:
        if underlying in SPECIAL_UNDERLYING_SYMBOL and trading_date:
            order_book_ids = instruments_df.order_book_id.tolist()

            strikes = get_price(order_book_ids, start_date=trading_date, end_date=trading_date, fields='strike_price',
                                expect_df=is_panel_removed)
            if strikes is None:
                return []
            if is_panel_removed:
                strikes.reset_index(level=1, inplace=True, drop=True)
            else:
                strikes = strikes.T

            instruments_df.set_index(instruments_df.order_book_id, inplace=True)
            instruments_df['strike_price'] = strikes[strikes.columns[0]]
            instruments_df = instruments_df[instruments_df.strike_price.notnull()]
            if instruments_df.empty:
                return []

        l = []
        for date in instruments_df.maturity_date.unique():
            df = instruments_df[instruments_df.maturity_date == date]
            df = df[df.strike_price <= strike]
            if df.empty:
                continue
            df = df[df.strike_price.rank(method='min', ascending=False) == 1]
            l += df.order_book_id.tolist()
        return l

    return instruments_df.order_book_id.tolist()


VALID_CONTRACT_PROPERTY_FIELDS = ['product_name', 'symbol', 'contract_multiplier', 'strike_price']


def _get_multi_index(oids, end_date, listed, de_listed):
    mult = []
    tds = get_trading_dates('20150101', datetime.date.today())
    index = pd.to_datetime(tds)
    for oid in oids:
        if oid not in listed:
            continue
        s = str(listed[oid])
        e = str(min(end_date, de_listed[oid]))
        _start, _end = pd.Timestamp(s), pd.Timestamp(e)
        start_pos, end_pos = index.searchsorted(_start), index.searchsorted(_end)
        _index = index[start_pos:end_pos+1]
        mult.extend([(oid, i) for i in _index])
    return pd.MultiIndex.from_tuples(mult, names=['order_book_id', 'trading_date'])


@export_as_api(namespace='options')
def get_contract_property(
    order_book_ids,
    start_date=None,
    end_date=None,
    fields=None,
    market='cn'
):
    """获取期权合约属性
    :param order_book_ids: 股票 order_book_id or order_book_id list
    :param start_date: 开始日期, 必要参数
    :param end_date: 结束日期；默认为今天
    :param fields: str或list类型. 默认为None, 返回所有fields.
    :param market: 默认值为"cn", 可选：cn

    :returns
        返回DataFrame, 索引为 MultiIndex([order_book_id, trading_date])
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    listed_dates = {}
    de_listed_dates = {}
    for oid in order_book_ids:
        i = instruments(oid)
        listed_dates[oid] = int(i.listed_date.replace('-', ''))
        de_listed_dates[oid] = int(i.de_listed_date.replace('-', ''))

    end_date = datetime.date.today() if not end_date else end_date
    end_date = ensure_date_int(end_date)
    if start_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
        # 如果指定start_date, 将退市的order_book_id过滤掉
        order_book_ids = [oid for oid in order_book_ids if start_date <= de_listed_dates[oid]]

    if fields is None:
        fields = VALID_CONTRACT_PROPERTY_FIELDS[:]
    else:
        if not isinstance(fields, list):
            fields = [fields]
        check_items_in_container(fields, VALID_CONTRACT_PROPERTY_FIELDS, 'Contract Property')
    # get data from server
    data = get_client().execute('options.get_contract_property', order_book_ids, fields)
    if not data:
        return
    df = pd.DataFrame(data)

    index = _get_multi_index(order_book_ids, end_date, listed_dates, de_listed_dates)
    df = df.set_index(['order_book_id', 'trading_date'])
    df = df.reindex(index).ffill()
    if start_date:
        msk = df.index.get_level_values('trading_date') >= pd.Timestamp(str(start_date))
        df = df[msk]
    return df.sort_index()

