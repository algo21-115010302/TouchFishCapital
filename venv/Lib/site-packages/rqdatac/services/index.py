# -*- coding: utf-8 -*-

import bisect
import pandas as pd

from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, compatible_with_parm
from rqdatac.validators import (
    ensure_date_int,
    ensure_date_range,
    ensure_list_of_string,
    ensure_order_book_ids,
    ensure_order_book_id
)
from rqdatac.utils import int8_to_datetime
from rqdatac.services.calendar import get_trading_dates_in_type


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def index_components(order_book_id, date=None, start_date=None, end_date=None, market="cn"):
    """获取指数成分
    :param order_book_id: 指数 id
    :param date: 指定日期；如不指定，返回最近一个交易日的数据
    :param start_date: 指定开始日期，不能和date同时指定
    :param end_date: 指定结束日期, 需和start_date同时指定并且应当不小于开始日期
    :param market:  (Default value = "cn")
    :returns list or dict
    """
    order_book_id = ensure_order_book_id(order_book_id)

    if date and (start_date or end_date):
        raise ValueError("date cannot be input together with start_date or end_date")
    elif (start_date and not end_date) or (end_date and not start_date):
        raise ValueError("start_date and end_date need to be applied together")

    if start_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
        trading_dates = get_trading_dates_in_type(start_date, end_date, expect_type="int")
        if not trading_dates:
            return
        data = get_client().execute(
            "index_components_v2", order_book_id, trading_dates[0], trading_dates[-1], market=market)
        if not data:
            return
        data = {d["trade_date"]: d["component_ids"] for d in data}
        dates = sorted(data.keys())
        for trading_date in trading_dates:
            if trading_date not in data:
                position = bisect.bisect_left(dates, trading_date) - 1
                data[trading_date] = data[dates[position]]
        return {int8_to_datetime(i): data[i] for i in trading_dates}

    if date:
        date = ensure_date_int(date)
    return get_client().execute("index_components", order_book_id, date, market=market)


@export_as_api
def index_weights(order_book_id, date=None, start_date=None, end_date=None, market="cn"):
    """获取指数的权重

    :param order_book_id: 如'000300.XSHG'和'000905.XSHG'
    :param market: 地区代码, 如 'cn' (Default value = "cn")
    :param date: 指定日期；如不指定，返回最近一个交易日的
    :param start_date: 指定开始日期，不能和date同时指定
    :param end_date: 指定结束日期, 需和start_date同时指定并且应当不小于开始日期
    :returns: 返回输入日期最近交易日该指数的权重

    """
    index_name = ensure_order_book_id(order_book_id)
    if date and (start_date or end_date):
        raise ValueError("date cannot be input together with start_date or end_date")
    elif (start_date and not end_date) or (end_date and not start_date):
        raise ValueError("start_date and end_date need to be applied together")

    if start_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
        trading_dates = get_trading_dates_in_type(start_date, end_date, expect_type="int")
        if not trading_dates:
            return
        data = get_client().execute("index_weights_v2", index_name, trading_dates[0], trading_dates[-1], market=market)
        if not data:
            return

        data = {ensure_date_int(d["date"]): d["data"] for d in data}
        dates = sorted(data.keys())
        for trading_date in trading_dates:
            if trading_date not in data:
                position = bisect.bisect_left(dates, trading_date) - 1
                data[trading_date] = data[dates[position]]

        data = [
            {"date": int8_to_datetime(date), "order_book_id": c["order_book_id"], "weight": c["weight"]}
            for date, component_ids in data.items() if date in trading_dates for c in component_ids
        ]
        return pd.DataFrame(data).set_index(["date", "order_book_id"]).sort_index()

    if date:
        date = ensure_date_int(date)
    data = get_client().execute("index_weights", index_name, date, market=market)
    if not data:
        return
    s = pd.Series({d["order_book_id"]: d["weight"] for d in data})
    s.index.name = "order_book_id"
    return s


@export_as_api
def index_indicator(order_book_ids, start_date=None, end_date=None, fields=None, market="cn"):
    """获取指数指标

    :param order_book_ids: 如'000016.XSHG'
    :param start_date: 如'2016-01-01' (Default value = None)
    :param end_date: 如'2017-01-01' (Default value = None)
    :param fields: 如'pb', 默认返回全部 fields (Default value = None)
    :param market:  (Default value = "cn")
    :returns: pd.DataFrame 或 None

    """
    all_fields = ("pe_ttm", "pe_lyr", "pb_ttm", "pb_lyr", "pb_lf")
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is not None:
        fields = ensure_list_of_string(fields)
        for f in fields:
            if f not in all_fields:
                raise ValueError("invalid field: {}".format(f))
    else:
        fields = all_fields

    df = get_client().execute(
        "index_indicator", order_book_ids, start_date, end_date, fields, market=market
    )
    if not df:
        return
    df = pd.DataFrame(df)
    df.set_index(["order_book_id", "trade_date"], inplace=True)
    return df
