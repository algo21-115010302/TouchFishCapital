# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from rqdatac.validators import (
    ensure_string,
    ensure_list_of_string,
    check_items_in_container,
    ensure_date_range,
    ensure_date_int,
    ensure_order_book_ids,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api

FIELDS = (
    "new_comments",
    "total_comments",
    "new_followers",
    "total_followers",
    "sell_actions",
    "buy_actions",
)


@export_as_api(namespace="xueqiu")
def top_stocks(field, date, frequency="1d", count=5, market="cn"):
    """获取雪球舆情数据

    :param field: 如 'new_comments', 'total_comments', 'new_followers',
                 'total_followers', 'sell_actions', 'buy_actions'
    :param date: 如 '2015-05-21', 必须在2015年4月23日之后
    :param frequency: 如 '1d', '1w', '1M' (Default value = "1d")
    :param count: 如 5, 10, 100 (Default value = 5)
    :param market: 地区代码, 如 'cn' (Default value = "cn")
    :returns: 如果有数据，返回一个DataFrame,否则返回None

    """
    field = ensure_string(field, "field")
    frequency = ensure_string(frequency, "frequency")
    check_items_in_container([field], FIELDS, "field")
    check_items_in_container([frequency], {"1d", "1w", "1M"}, "frequency")
    d = {"1d": "d", "1M": "m", "1w": "w"}
    frequency = d[frequency]
    date = ensure_date_int(date)
    if date < 20150423 or date > ensure_date_int(datetime.datetime.today()):
        raise ValueError("date out of range, start_date " "cannot be earlier than 2015-04-23")
    data = get_client().execute("xueqiu.top_stocks", field, date, frequency, count, market)

    if not data:
        return
    df = pd.DataFrame(data)
    df = df[["order_book_id", field]]
    return df


@export_as_api(namespace="xueqiu")
def history(
    order_book_ids,
    start_date="2015-05-21",
    end_date="2016-05-21",
    frequency="1d",
    fields=None,
    market="cn",
):
    """获取雪球历史舆情数据

    :param order_book_ids: 股票代码或代码列表
    :param start_date: 如 '2015-05-21', 必须在2015年4月23日之后 (Default value = "2015-05-21")
    :param end_date: 如 '2016-05-21' (Default value = "2016-05-21")
    :param frequency: 如 '1d' (Default value = "1d")
    :param fields: 如 'new_comments', 'total_comments', 'new_followers',
                 'total_followers', 'sell_actions', 'buy_actions' (Default value = None)
    :param market: 地区代码, 如 'cn' (Default value = "cn")
    :returns: 返回pd.Panel或pd.DataFrame或pd.Series

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, FIELDS, "fields")
    else:
        fields = FIELDS
    frequency = ensure_string(frequency, "frequency")
    check_items_in_container([frequency], {"1d"}, "frequency")
    start_date, end_date = ensure_date_range(start_date, end_date)

    if start_date < 20150423:
        raise ValueError("date out of range, start_date " "cannot be earlier than 2015-04-23")
    data = get_client().execute(
        "xueqiu.history", order_book_ids, start_date, end_date, fields, market
    )
    if not data:
        return
    df = pd.DataFrame(data)
    df = df.set_index(["date", "order_book_id"])
    df.sort_index(inplace=True)
    pl = df.to_panel()
    if len(pl.minor_axis) == 1:
        pl = pl.minor_xs(pl.minor_axis[0])
    if len(fields) == 1:
        pl = pl[fields[0]]
    return pl
