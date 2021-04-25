# -*- coding: utf-8 -*-
from rqdatac.validators import ensure_date_or_today_int
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api


@export_as_api
def concept_list(date=None, market="cn"):
    """获取所有股票概念.

    :param date: 可指定日期，默认按当前日期返回.
    :param market: 地区代码, 如 'cn' (Default value = "cn")
    :returns: 符合指定日期内出现过的所有概念列表

    """
    date = ensure_date_or_today_int(date)
    return get_client().execute("concept_list", date, market=market)


@export_as_api
def concept(*concepts, **kwargs):
    """获取对应某个概念的股票列表。

    可指定日期，默认按当前日期返回。目前支持的概念列表可以查询以下网址:
    https://www.ricequant.com/api/research/chn#concept-API-industry

    :param concepts: 概念字符串,如 '民营医院'
    :param date: 可指定日期，默认按当前日期返回.
    :param market: 地区代码, 如 'cn'
    :returns: 符合对应概念的股票列表

    """
    date = kwargs.pop("date", None)
    market = kwargs.pop("market", "cn")
    date = ensure_date_or_today_int(date)
    if kwargs:
        raise ValueError('unknown kwargs: {}'.format(kwargs))
    return get_client().execute("concept", concepts, date, market=market)


@export_as_api
def concept_names(order_book_id, date=None, expect_type="str", market="cn"):
    """获取证券所属的概念列表。

    :param order_book_id: 证券ID
    :param date: 可指定日期，默认按当前日期返回。
    :param expect_type: 期望返回结果类型，可选址为："str"：返回字符串，"list"：返回列表，默认为str。
    :param market: 地区代码, 如 "cn" (Default value = "cn")
    :returns: 概念列表

    """

    date = ensure_date_or_today_int(date)
    data = get_client().execute("concept_names", order_book_id, date, market=market)
    if expect_type == "str":
        return data
    elif expect_type == "list":
        return data.split("|")
    raise ValueError("expect_type should be str like 'str' or 'list'")

