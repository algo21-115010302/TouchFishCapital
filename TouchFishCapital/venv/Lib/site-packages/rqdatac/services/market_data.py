# -*- coding: utf-8 -*-
import warnings
from collections import OrderedDict

import pandas as pd

from rqdatac.validators import (
    ensure_list_of_string,
    ensure_order,
    check_items_in_container,
    ensure_date_range,
    ensure_date_int,
    ensure_order_book_ids,
    raise_for_no_panel,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, compatible_with_parm
from rqdatac.utils import pf_fill_nan, is_panel_removed


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_split(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取拆分信息

    :param order_book_ids: 股票 order_book_id or order_book_id list
    :param start_date: 开始日期；默认为上市首日
    :param end_date: 结束日期；默认为今天
    :param market:  (Default value = "cn")

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date is not None:
        start_date = ensure_date_int(start_date)
    if end_date is not None:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("get_split", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.sort_values("ex_dividend_date", inplace=True)
    # cumprod [1, 2, 4] -> [1, 1*2, 1*2*4]
    df["cum_factor"] = df["split_coefficient_to"] / df["split_coefficient_from"]
    df["cum_factor"] = df.groupby("order_book_id")["cum_factor"].cumprod()
    if len(order_book_ids) == 1:
        df.set_index("ex_dividend_date", inplace=True)
    else:
        df.set_index(["order_book_id", "ex_dividend_date"], inplace=True)
    df.sort_index(inplace=True)
    return df


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_dividend(order_book_ids, start_date=None, end_date=None, adjusted=False, market="cn"):
    """获取分红信息

    :param order_book_ids: 股票 order_book_id or order_book_id list
    :param start_date: 开始日期，默认为股票上市日期
    :param end_date: 结束日期，默认为今天
    :param adjusted: deprecated
    :param market:  (Default value = "cn")

    """
    if adjusted:
        warnings.warn(
            "get_dividend adjusted = `True` is not supported yet. "
            "The default value is `False` now."
        )
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date is not None:
        start_date = ensure_date_int(start_date)
    if end_date is not None:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("get_dividend", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    if len(order_book_ids) == 1:
        df.set_index("declaration_announcement_date", inplace=True)
    else:
        df.set_index(["order_book_id", "declaration_announcement_date"], inplace=True)
    return df.sort_index()


@export_as_api
def get_dividend_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """对应时间段是否发生分红

    :param order_book_ids: 股票 order_book_id or order_book_id list
    :param start_date: 开始日期，默认为空
    :param end_date: 结束日期，默认为空
    :param market:  (Default value = "cn")

    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date is not None:
        start_date = ensure_date_int(start_date)
    if end_date is not None:
        end_date = ensure_date_int(end_date)
    if start_date and end_date:
        if start_date > end_date:
            raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))

    data = get_client().execute("get_dividend_info", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    if len(order_book_ids) == 1:
        df.set_index("effective_date", inplace=True)
    else:
        df.set_index(["order_book_id", "effective_date"], inplace=True)
    return df.sort_index()


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_ex_factor(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取复权因子

    :param order_book_ids: 如'000001.XSHE'
    :param market: 国家代码, 如 'cn' (Default value = "cn")
    :param start_date: 开始日期，默认为股票上市日期
    :param end_date: 结束日期，默认为今天
    :returns: 如果有数据，返回一个DataFrame, 否则返回None

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date is not None:
        start_date = ensure_date_int(start_date)
    if end_date is not None:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("get_ex_factor", order_book_ids, start_date, end_date, market=market)
    if not data:
        return None
    df = pd.DataFrame(data)
    df.sort_values(["order_book_id", "ex_date"], inplace=True)
    df.set_index("ex_date", inplace=True)
    return df


TURNOVER_FIELDS_MAP = OrderedDict()
TURNOVER_FIELDS_MAP["today"] = "turnover_rate"
TURNOVER_FIELDS_MAP["week"] = "week_turnover_rate"
TURNOVER_FIELDS_MAP["month"] = "month_turnover_rate"
TURNOVER_FIELDS_MAP["year"] = "year_turnover_rate"
TURNOVER_FIELDS_MAP["current_year"] = "year_sofar_turnover_rate"


def _get_maped_fields(fields):
    fields = ensure_list_of_string(fields, "fields")
    check_items_in_container(fields, TURNOVER_FIELDS_MAP, "fields")
    fields = ensure_order(fields, TURNOVER_FIELDS_MAP.keys())
    return fields, [TURNOVER_FIELDS_MAP[field] for field in fields]


@export_as_api
def get_turnover_rate(order_book_ids, start_date=None, end_date=None, fields=None, expect_df=False, market="cn"):
    """获取股票换手率数据

    :param order_book_ids: 股票代码或股票代码列表
    :param start_date: 开始时间
    :param end_date: 结束时间；在 start_date 和 end_date 都不指定的情况下，默认为最近3个月
    :param fields: str或list类型. 默认为None, 返回所有fields.
                   field 包括： 'today', 'week', 'month', 'year', 'current_year'
                   (Default value = None)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :param market: 地区代码, 如: 'cn' (Default value = "cn")
    :returns: 如果order_book_ids或fields为单个值 返回pandas.DataFrame, 否则返回pandas.Panel

    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is not None:
        fields, mapped_fields = _get_maped_fields(fields)
    else:
        fields, mapped_fields = list(TURNOVER_FIELDS_MAP.keys()),  list(TURNOVER_FIELDS_MAP.values())
    df = get_client().execute(
        "get_turnover_rate", order_book_ids, start_date, end_date, mapped_fields, market=market
    )
    if not df:
        return
    df = pd.DataFrame(df, columns=["tradedate", "order_book_id"] + mapped_fields)
    df.rename(columns={v: k for k, v in TURNOVER_FIELDS_MAP.items()}, inplace=True)

    if not expect_df and not is_panel_removed:
        df.set_index(["tradedate", "order_book_id"], inplace=True)
        df.sort_index(inplace=True)
        df = df.to_panel()
        df = pf_fill_nan(df, order_book_ids)
        if len(order_book_ids) == 1:
            df = df.minor_xs(*order_book_ids)
            if fields and len(fields) == 1:
                return df[fields[0]]
            return df
        if fields and len(fields) == 1:
            return df[fields[0]]
        warnings.warn("Panel is removed after pandas version 0.25.0."
                      " the default value of 'expect_df' will change to True in the future.")
        return df
    else:
        df.sort_values(["order_book_id", "tradedate"], inplace=True)
        df.set_index(["order_book_id", "tradedate"], inplace=True)
        if expect_df:
            return df

        if len(order_book_ids) != 1 and len(fields) != 1:
            raise_for_no_panel()

        if len(order_book_ids) == 1:
            df.reset_index(level=0, drop=True, inplace=True)
            if len(fields) == 1:
                df = df[fields[0]]
            return df
        else:
            df = df.unstack(0)[fields[0]]
            df.index.name = None
            df.columns.name = None
            return df


@export_as_api
def get_price_change_rate(order_book_ids, start_date=None, end_date=None, expect_df=False, market="cn"):
    """获取价格变化信息

    :param order_book_ids: 股票列表
    :param start_date: 开始日期: 如'2013-01-04'
    :param end_date: 结束日期: 如'2014-01-04'；在 start_date 和 end_date 都不指定的情况下，默认为最近3个月
    :param expect_df: 返回 DataFrame (Default value = False)
    :param market: 地区代码
    :returns: 如果输入一只股票, 则返回pandas.Series, 否则返回pandas.DataFrame

    """
    start_date, end_date = ensure_date_range(start_date, end_date)
    order_book_ids = ensure_order_book_ids(order_book_ids)
    data = get_client().execute("get_daily_returns", order_book_ids, start_date, end_date, market=market)
    if not data:
        return None
    df = pd.DataFrame(data)

    if len(order_book_ids) == 1 and not expect_df:
        df = df.set_index("date")
        series = df["daily_return"]
        series.name = order_book_ids[0]
        return series
    df = df.pivot(index="date", columns="order_book_id", values="daily_return")
    return df.sort_index()


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_yield_curve(start_date=None, end_date=None, tenor=None, market="cn"):
    """获取国债收益率曲线

    :param market: 地区代码, 如'cn', 'us' (Default value = "cn")
    :param start_date: 开始日期 (Default value = "2013-01-04")
    :param end_date: 结束日期 (Default value = "2014-01-04")
    :param tenor: 类别, 如 OS, 1M, 3M, 1Y (Default value = None)

    """
    start_date, end_date = ensure_date_range(start_date, end_date)
    all_tenor = (
        "0S",
        "1M",
        "2M",
        "3M",
        "6M",
        "9M",
        "1Y",
        "2Y",
        "3Y",
        "4Y",
        "5Y",
        "6Y",
        "7Y",
        "8Y",
        "9Y",
        "10Y",
        "15Y",
        "20Y",
        "30Y",
        "40Y",
        "50Y",
    )
    if tenor:
        tenor = ensure_list_of_string(tenor, "tenor")
        check_items_in_container(tenor, all_tenor, "tenor")
        tenor = ensure_order(tenor, all_tenor)
    df = get_client().execute("get_yield_curve", start_date, end_date, tenor, market=market)
    if not df:
        return
    columns = ["trading_date"]
    columns.extend(tenor or all_tenor)
    df = pd.DataFrame(df, columns=columns)
    df.set_index("trading_date", inplace=True)
    return df.sort_index()


@export_as_api
def get_block_trade(order_book_ids, start_date=None, end_date=None, market='cn'):
    """获取大宗交易信息
    :param order_book_ids: 股票代码
    :param start_date: 起始日期，默认为前三个月
    :param end_date: 截止日期，默认为今天
    :param market: (default value = 'cn')
    :return: pd.DataFrame or None
    """

    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)

    data = get_client().execute('get_block_trade', order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)[['order_book_id', 'trade_date', 'price', 'volume', 'total_turnover', 'buyer', 'seller']]
    df.set_index(["order_book_id", "trade_date"], inplace=True)
    df.sort_index(inplace=True)
    return df

EXCHANGE_DATE_FIELDS=[
    "currency_pair",
    "bid_referrence_rate",
    "ask_referrence_rate",
    "middle_referrence_rate",
    "bid_settlement_rate_sh",
    "ask_settlement_rate_sh",
    "bid_settlement_rate_sz",
    "ask_settlement_rate_sz",
]

@export_as_api
def get_exchange_rate(start_date=None, end_date=None, fields=None):
    """获取汇率信息

    :param start_date: 开始日期, 如 '2013-01-04' (Default value = None)
    :param end_date: 结束日期, 如 '2014-01-04' (Default value = None)
    :param fields: str or list 返回 字段名称:currency_pair、bid_referrence_rate、ask_referrence_rate、middle_referrence_rate
        bid_settlement_rate_sh、ask_settlement_rate_sh、bid_settlement_rate_sz、ask_settlement_rate_sz

    """
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, EXCHANGE_DATE_FIELDS, "fields")
    else:
        fields = EXCHANGE_DATE_FIELDS

    data = get_client().execute("get_exchange_rate", start_date, end_date, fields)
    if not data:
        return None
    df = pd.DataFrame(data)
    df.set_index("date", inplace=True)
    df = df[fields]
    return df


TEMPORARY_CODE_FIELDS = [
    "symbol",
    "temporary_trade_code",
    "temporary_symbol",
    "temporary_round_lot",
    "temporary_effective_date",
    "parallel_effective_date",
    "parallel_cancel_date"
]


@export_as_api
def get_temporary_code(order_book_ids, market="cn"):
    """临时交易代码查询

    :param order_book_ids: 股票 order_book_id or order_book_id list
    :param market:  (Default value = "cn")
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)

    data = get_client().execute("get_temporary_code", order_book_ids, market)
    if not data:
        return None
    df = pd.DataFrame(data)
    df.set_index("order_book_id", inplace=True)
    df = df[TEMPORARY_CODE_FIELDS]
    return df