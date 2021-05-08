# -*- coding: utf-8 -*-
import datetime
import warnings

import pandas as pd

from rqdatac.utils import to_datetime, is_panel_removed, to_date
from rqdatac.validators import (
    ensure_date_range,
    ensure_date_or_today_int,
    ensure_list_of_string,
    check_items_in_container,
    ensure_order,
    ensure_order_book_id,
    ensure_order_book_ids,
    ensure_dates_base_on_listed_date,
    ensure_string,
    ensure_date_int,
    raise_for_no_panel,
)
from rqdatac.services.basic import instruments
from rqdatac.services.calendar import (
    get_trading_dates,
    get_previous_trading_date,
    get_trading_dates_in_type,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, compatible_with_parm


@export_as_api
def current_stock_connect_quota(
        connect=None, fields=None
):
    """
    获取沪港通、深港通资金流向数据

    :param connect: 字符串，目前可从sh_to_hk, hk_to_sh, sz_to_hk, hk_to_sz多选, 默认为查询所有资金流向数据
    :param fields:  字符串列表，需要的字段，目前可从buy_turnover, sell_turnover, quota_balance, quota_balance_ratio多选, 默认取所有

    """
    DEFAULT_CONNECT = ["sh_to_hk", "hk_to_sh", "sz_to_hk", "hk_to_sz"]
    if connect is None:
        connect = DEFAULT_CONNECT
    else:
        connect = ensure_list_of_string(connect)
        check_items_in_container(connect, DEFAULT_CONNECT, 'connect')

    DEFAULT_FIELDS = ['buy_turnover', 'sell_turnover', 'quota_balance', 'quota_balance_ratio']
    if fields is None:
        fields = DEFAULT_FIELDS
    else:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, DEFAULT_FIELDS, 'fields')

    data = get_client().execute("current_stock_connect_quota", connect=connect)
    res = pd.DataFrame(data)
    if res.empty:
        return None
    res["datetime"] = pd.to_datetime(res["datetime"], format='%Y%m%d%H%M')
    res.set_index(['datetime', 'connect'], inplace=True)
    res = res[fields]
    return res


@export_as_api
def get_stock_connect_quota(connect=None, start_date=None, end_date=None, fields=None):
    """获取历史沪深港通额度日频数据

    :param connect: 默认返回全部content ["sh_to_hk", "hk_to_sh", "sz_to_hk", "hk_to_sz"]
    :param start_date: 默认为全部历史数据
    :param end_date: 默认为最新日期
    :param fields:  默认为所有字段 ['buy_turnover', 'sell_turnover', 'quota_balance', 'quota_balance_ratio']
    :return:
    """
    DEFAULT_CONNECT = ["sh_to_hk", "hk_to_sh", "sz_to_hk", "hk_to_sz"]
    if connect is None:
        connect = DEFAULT_CONNECT
    else:
        connect = ensure_list_of_string(connect)
        check_items_in_container(connect, DEFAULT_CONNECT, 'connect')

    DEFAULT_FIELDS = ['buy_turnover', 'sell_turnover', 'quota_balance', 'quota_balance_ratio']
    if fields is None:
        fields = DEFAULT_FIELDS
    else:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, DEFAULT_FIELDS, 'fields')

    start_date = ensure_date_int(start_date) if start_date else start_date
    end_date = ensure_date_int(end_date) if end_date else end_date

    if start_date and end_date and start_date > end_date:
        raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))

    data = get_client().execute(
        "get_stock_connect_quota", connect=connect, start_date=start_date, end_date=end_date, fields=fields
    )
    if not data:
        return None
    res = pd.DataFrame(data)
    res.set_index(['datetime', 'connect'], inplace=True)
    res.sort_index(ascending=True, inplace=True)
    return res


@export_as_api
def is_st_stock(order_book_ids, start_date=None, end_date=None, market="cn"):
    """判断股票在给定的时间段是否是ST股, 返回值为一个DataFrame

    :param order_book_ids: 股票 id
    :param start_date:  (Default value = None)
    :param end_date:  (Default value = None)
    :param market:  (Default value = "cn")

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market=market)

    if len(order_book_ids) == 1:
        instrument = instruments(order_book_ids[0], market=market)
        start_date, end_date = ensure_dates_base_on_listed_date(instrument, start_date, end_date, market)
        if start_date is None:
            return

    start_date, end_date = ensure_date_range(start_date, end_date)

    trading_dates = pd.to_datetime(get_trading_dates(start_date, end_date, market=market))
    data = get_client().execute(
        "get_st_days", order_book_ids, start_date=start_date, end_date=end_date
    )
    df = pd.DataFrame(data=False, columns=order_book_ids, index=trading_dates)
    for idx, dates in data.items():
        for date in dates:
            date = to_datetime(date)
            df.at[date, idx] = True
    return df


@export_as_api
def _is_st_stock(order_book_id, date=None, market="cn"):
    """判断股票在给定日期是否是ST股
    :param order_book_id: 股票id
    :param date:  (Default value = None)
    :param market:  (Default value = "cn")
    :returns: True or False
    """
    order_book_id = ensure_order_book_id(order_book_id, type="CS", market=market)
    date = ensure_date_or_today_int(date)
    df = is_st_stock(order_book_id, start_date=date, end_date=date, market=market)
    if df is None or df.empty:
        return False
    else:
        return df[order_book_id][0]


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def is_suspended(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取股票停牌信息

    :param order_book_ids: 股票名称
    :param start_date: 开始日期, 如'2013-01-04' (Default value = None)
    :param end_date: 结束日期，如'2014-01-04' (Default value = None)
    :param market: 地区代码, 如 'cn' (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market=market)

    if len(order_book_ids) == 1:
        instrument = instruments(order_book_ids[0], market=market)
        start_date, end_date = ensure_dates_base_on_listed_date(instrument, start_date, end_date, market)
        if start_date is None:
            return
    if end_date is None:
        end_date = datetime.date.today()
    start_date, end_date = ensure_date_range(start_date, end_date)

    trading_dates = pd.to_datetime(get_trading_dates(start_date, end_date, market=market))
    df = pd.DataFrame(data=False, columns=order_book_ids, index=trading_dates)
    data = get_client().execute("get_suspended_days", order_book_ids, start_date, end_date, market=market)
    for idx, dates in data.items():
        for date in dates:
            date = to_datetime(int(date))
            df.at[date, idx] = True
    df.sort_index(inplace=True)
    return df


stock_fields = {"shares_holding": "shares_holding", "holding_ratio": "holding_ratio"}
special_symbols = ["all_connect", "shanghai_connect", "shenzhen_connect"]
symbols_map = {"shanghai_connect": "SH", "shenzhen_connect": "SZ"}


@export_as_api
def get_stock_connect(order_book_ids, start_date=None, end_date=None, fields=None, expect_df=False):
    """获取"陆股通"的持股、持股比例

    :param order_book_ids: 股票列表
    :param start_date: 开始日期: 如'2017-03-17' (Default value = None)
    :param end_date: 结束日期: 如'2018-03-16' (Default value = None)
    :param fields: 默认为所有字段，可输入shares_holding或者holding_ratio (Default value = None)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :returns: 返回pandas.DataFrame or pandas.Panel

    """
    if order_book_ids not in ("shanghai_connect", "shenzhen_connect", "all_connect"):
        order_book_ids = ensure_order_book_ids(order_book_ids, type="CS")
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is not None:
        fields = ensure_list_of_string(fields)
        for f in fields:
            if f not in ("shares_holding", "holding_ratio"):
                raise ValueError("invalid field: {}".format(f))
    else:
        fields = ["shares_holding", "holding_ratio"]
    data = get_client().execute("get_stock_connect", order_book_ids, start_date, end_date, fields)
    if not data:
        return None
    df = pd.DataFrame(data, columns=["trading_date", "order_book_id"] + fields)

    if not expect_df and not is_panel_removed:
        df = df.set_index(["trading_date", "order_book_id"])
        df = df.to_panel()
        df.major_axis.name = None
        df.minor_axis.name = None
        if len(order_book_ids) == 1:
            df = df.minor_xs(order_book_ids[0])
        if len(fields) == 1:
            df = df[fields[0]]
        if len(order_book_ids) != 1 and len(fields) != 1:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
        return df
    else:
        df.sort_values(["order_book_id", "trading_date"], inplace=True)
        df.set_index(["order_book_id", "trading_date"], inplace=True)
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


MARGIN_FIELDS = (
    "margin_balance",
    "buy_on_margin_value",
    "short_sell_quantity",
    "margin_repayment",
    "short_balance_quantity",
    "short_repayment_quantity",
    "short_balance",
    "total_balance",
)

MARGIN_SUMMARY_MAP = {"SH": "XSHG", "XSHG": "XSHG", "SZ": "XSHE", "XSHE": "XSHE"}


@export_as_api
def get_securities_margin(
        order_book_ids, start_date=None, end_date=None, fields=None, expect_df=False, market="cn"
):
    """获取股票融资融券数据

    :param order_book_ids: 股票代码或代码列表
    :param start_date: 开始时间，支持 str, date, datetime, pandasTimestamp
        默认为 end_date 之前一个月 (Default value = None)
    :param end_date: 结束时间 默认为当前日期前一天 (Default value = None)
    :param fields: str 或 list 类型. 默认为 None, 返回所有字段。可选字段包括：
                   today, week, month, three_month, six_month, year, current_year, total
                   (Default value = None)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :param market: 地区代码, 如: 'cn' (Default value = "cn")
    :returns: 如果传入多个股票代码，且 fields 为多个或者 None，返回 pandas.Panel
        如果传入一只股票或者 fields 为单个字段，则返回 pandas.DataFrame
        如果传入的股票代码和字段数都是1，则返回 pandas.Series

    """

    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    all_list = []
    for order_book_id in order_book_ids:
        if order_book_id.upper() in MARGIN_SUMMARY_MAP:
            all_list.append(MARGIN_SUMMARY_MAP[order_book_id.upper()])
        else:
            inst = instruments(order_book_id, market)

            if inst.type in ["CS", "ETF", "LOF"]:
                all_list.append(inst.order_book_id)
            else:
                warnings.warn("{} is not stock, ETF, or LOF.".format(order_book_id))
    order_book_ids = all_list
    if not order_book_ids:
        raise ValueError("no valid securities in {}".format(order_book_ids))
    if fields is None:
        fields = list(MARGIN_FIELDS)
    else:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, MARGIN_FIELDS, "fields")
        fields = ensure_order(fields, MARGIN_FIELDS)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if end_date > ensure_date_or_today_int(None):
        end_date = ensure_date_or_today_int(get_previous_trading_date(datetime.date.today()))
    trading_dates = pd.to_datetime(get_trading_dates(start_date, end_date, market=market))

    data = get_client().execute(
        "get_securities_margin", order_book_ids, start_date, end_date, market=market
    )
    if not data:
        return

    if not expect_df and not is_panel_removed:

        pl = pd.Panel(items=fields, major_axis=trading_dates, minor_axis=order_book_ids)
        for r in data:
            for field in fields:
                value = r.get(field)
                pl.at[field, r["date"], r["order_book_id"]] = value

        if len(order_book_ids) == 1:
            pl = pl.minor_xs(order_book_ids[0])
        if len(fields) == 1:
            pl = pl[fields[0]]
        if len(order_book_ids) != 1 and len(fields) != 1:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
        return pl
    else:
        df = pd.DataFrame(data)
        df.sort_values(["order_book_id", "date"], inplace=True)
        df.set_index(["order_book_id", "date"], inplace=True)
        df = df.reindex(columns=fields)
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


MARGIN_TYPE = ("stock", "cash")
EXCHANGE_TYPE = {"SZ": "XSHE", "sz": "XSHE", "xshe": "XSHE", "SH": "XSHG", "sh": "XSHG", "xshg": "XSHG"}
EXCHANGE_CONTENT = ["XSHE", "XSHG"]


@export_as_api
def get_margin_stocks(date=None, exchange=None, margin_type='stock', market="cn"):
    """获取融资融券信息

    :param date: 查询日期，默认返回今天上一交易日，支持 str, timestamp, datetime 类型
    :param exchange: 交易所信息，默认不填写则返回全部。
                    str类型，默认为 None，返回所有字段。可选字段包括：
                    'XSHE', 'sz' 代表深交所；'XSHG', 'sh' 代表上交所，不区分大小写
                    (Default value = None)
    :param margin_type: 'stock' 代表融券卖出，'cash'，代表融资买入，默认为'stock'

    """
    if date:
        date = ensure_date_int(date)
    else:
        date = get_previous_trading_date(datetime.date.today())
        date = date.year * 10000 + date.month * 100 + date.day

    if exchange is None:
        exchange = EXCHANGE_CONTENT
    else:
        exchange = ensure_string(exchange, "exchange")
        if exchange in EXCHANGE_TYPE:
            exchange = EXCHANGE_TYPE[exchange]
        check_items_in_container(exchange, EXCHANGE_CONTENT, "exchange")
        exchange = [exchange]

    margin_type = ensure_string(margin_type, "margin_type")
    check_items_in_container(margin_type, MARGIN_TYPE, "margin_type")

    data = get_client().execute(
        "get_margin_stocks", date, exchange, margin_type, market=market
    )

    if not data:
        return []
    else:
        return sorted(data)


share_fields = {
    "total": "total_shares",
    "circulation_a": "a_cir_shares",
    "non_circulation_a": "a_non_cir_shares",
    "total_a": "a_total_shares",
}

anti_fields = {v: k for k, v in share_fields.items()}


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_shares(order_book_ids, start_date=None, end_date=None, fields=None, expect_df=False, market="cn"):
    """获取流通股本信息

    :param order_book_ids: 股票名称
    :param start_date: 开始日期, 如'2013-01-04' (Default value = None)
    :param end_date: 结束日期，如'2014-01-04' (Default value = None)
    :param fields: 如'total', 'circulation_a' (Default value = None)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :param market: 地区代码，如'cn' (Default value = "cn")
    :returns: 返回一个DataFrame

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields:
        fields = ensure_list_of_string(fields, "fields")
        if 'management_circulation' in fields:
            fields.remove('management_circulation')
            if fields:
                warnings.warn("management_circulation is removed")
            else:
                raise ValueError("management_circulation is removed")
        check_items_in_container(fields, set(share_fields), "fields")
        fields = [share_fields[i] for i in fields]
    else:
        fields = list(share_fields.values())

    all_shares = get_client().execute("get_shares", order_book_ids, fields, market=market)
    if not all_shares:
        return
    dates = get_trading_dates_in_type(start_date, end_date, expect_type="datetime", market=market)
    df = pd.DataFrame(all_shares)
    unique = set(df.order_book_id)
    for order_book_id in order_book_ids:
        if order_book_id not in unique:
            df = df.append(
                {"order_book_id": order_book_id, "date": df.date.iloc[-1]}, ignore_index=True
            )
    df.set_index(["date", "order_book_id"], inplace=True)
    df.sort_index(inplace=True)
    df = df.unstack(level=1)
    index = df.index.union(dates)
    df = df.reindex(index)
    df = df.fillna(method="ffill")
    df = df.loc[list(dates)]
    df = df.dropna(how="all")
    df = df[fields]
    if not is_panel_removed and not expect_df:
        pl = df.stack(1).to_panel()
        pl.items = [anti_fields[i] for i in pl.items]
        if len(order_book_ids) == 1:
            pl = pl.minor_xs(order_book_ids[0])
        if len(fields) == 1:
            pl = pl[anti_fields[fields[0]]]
        if len(order_book_ids) != 1 and len(fields) != 1:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
        return pl
    else:
        df = df.stack(1)
        df.index.set_names(["date", "order_book_id"], inplace=True)
        df = df.reorder_levels(["order_book_id", "date"]).sort_index()
        df = df.rename(columns=anti_fields)
        if expect_df:
            return df

        if len(order_book_ids) != 1 and len(fields) != 1:
            raise_for_no_panel()

        if len(order_book_ids) == 1:
            df.reset_index(level=0, drop=True, inplace=True)
            if len(fields) == 1:
                df = df[anti_fields[fields[0]]]
            return df
        else:
            df = df.unstack(0)[anti_fields[fields[0]]]
            df.index.name = None
            df.columns.name = None
            return df


allotment_fields = [
    "proportion",
    "allotted_proportion",
    "allotted_shares",
    "allotment_price",
    "book_closure_date",
    "ex_right_date", ]


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_allotment(order_book_ids, start_date=None, end_date=None, fields=None, market="cn"):
    """获取配股信息
    :param order_book_ids: 股票名称
    :param start_date: 开始日期, 如'1991-01-01' (Default value = 1991-01-01)
    :param end_date: 结束日期，如'2014-01-04' (Default value = None)
    :param market: 地区代码，如'cn' (Default value = "cn")
    :returns: 返回一个DataFrame
    'order_book_id': 股票合约代码
    'declaration_announcement_date': 首次信息发布日期
    'proportion': 配股比例(每一股对应的配股比例)
    'allotted_proportion': 实际配股比例(每一股对应的配股比例)
    'allotted_shares': 实际配股数量
    'allotment_price': 每股配股价格
    'book_closure_date': 股权登记日
    'ex_right_date': 除权除息日
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)

    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, allotment_fields, "fields")
    else:
        fields = allotment_fields

    all_allotment = get_client().execute("get_allotment", order_book_ids, fields, start_date, end_date, market=market)
    if not all_allotment:
        return
    df = pd.DataFrame(all_allotment)
    df.set_index(["order_book_id", "declaration_announcement_date"], inplace=True)
    df.sort_index(inplace=True)
    df = df[fields]
    return df
