# -*- coding: utf-8 -*-
import datetime
import re

import six
import pandas as pd

from rqdatac.client import get_client
from rqdatac.validators import (
    ensure_date_int,
    ensure_date_or_today_int,
    ensure_order_book_ids,
    ensure_string,
    ensure_string_in,
    check_items_in_container
)
from rqdatac.decorators import export_as_api
from rqdatac.utils import to_date_str


@export_as_api
def shenwan_industry(index_name, date=None, market="cn"):
    """获取申万行业组成

    :param index_name: 申万行业代码或名字, 如'801010.INDX'或'农林牧渔'
    :param date: 如 '2015-01-07' (Default value = None)
    :param market:  (Default value = "cn")
    :returns: 返回输入日期最近交易日的申万行业组成

    """
    if not isinstance(index_name, six.string_types):
        raise ValueError("string expected, got {!r}".format(index_name))

    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)
    return get_client().execute("shenwan_industry", index_name, date, market=market)


LEVEL_MAP = (
    None,
    ("index_code", "index_name"),
    ("second_index_code", "second_index_name"),
    ("third_index_code", "third_index_name"),
)


@export_as_api
def shenwan_instrument_industry(order_book_ids, date=None, level=1, expect_df=False, market="cn"):
    """获取股票对应的申万行业

    :param order_book_ids: 股票列表，如['000001.XSHE', '000002.XSHE']
    :param date: 如 '2015-01-07' (Default value = None)
    :param level:  (Default value = 1)
    :param expect_df: 返回 DataFrame，默认为 False
    :param market:  (Default value = "cn")
    :returns: code, name
        返回输入日期最近交易日的股票对应申万行业

    """

    if level not in [0, 1, 2, 3]:
        raise ValueError("level should be in 0,1,2,3")
    order_book_ids = ensure_order_book_ids(order_book_ids)

    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)

    r = get_client().execute("shenwan_instrument_industry", order_book_ids, date, level, market=market)
    if not r:
        return

    if len(order_book_ids) == 1 and not expect_df:
        r = r[0]
        if level != 0:
            return r[LEVEL_MAP[level][0]], r[LEVEL_MAP[level][1]]
        else:
            return (
                r["index_code"],
                r["index_name"],
                r["second_index_code"],
                r["second_index_name"],
                r["third_index_code"],
                r["third_index_name"],
            )

    df = pd.DataFrame(r).set_index("order_book_id")
    if level != 0 and level != 1:
        df.rename(columns=dict(zip(LEVEL_MAP[level], LEVEL_MAP[1])), inplace=True)
    return df


@export_as_api
def zx_industry(industry_name, date=None):
    """获取中信行业股票列表

    :param industry_name: 中信行业名称或代码
    :param date: 查询日期，默认为当前最新日期
    :return: 所属目标行业的order_book_id list or None
    """
    if not isinstance(industry_name, six.string_types):
        raise ValueError("string expected, got {!r}".format(industry_name))
    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)
    return get_client().execute("zx_industry", industry_name, date)


ZX_LEVEL_MAP = (
    None,
    "first_industry_name",
    "second_industry_name",
    "third_industry_name",
)


@export_as_api
def zx_instrument_industry(order_book_ids, date=None, level=1, expect_df=False):
    """获取股票对应的中信行业

    :param order_book_ids: 股票列表，如['000001.XSHE', '000002.XSHE']
    :param date: 如 '2015-01-07' (Default value = None)
    :param level:  (Default value = 1)
    :param expect_df: 返回 DataFrame，默认为 False
    :returns: code, name
        返回输入日期最近交易日的股票对应中信行业

    """

    if level not in [0, 1, 2, 3]:
        raise ValueError("level should be in 0,1,2,3")
    order_book_ids = ensure_order_book_ids(order_book_ids)

    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)

    r = get_client().execute("zx_instrument_industry", order_book_ids, date, level)
    if not r:
        return
    if len(order_book_ids) == 1 and not expect_df:
        r = r[0]
        if level != 0:
            return [r[ZX_LEVEL_MAP[level]], ]
        else:
            return [
                r["first_industry_name"],
                r["second_industry_name"],
                r["third_industry_name"],
            ]

    df = pd.DataFrame(r).set_index("order_book_id")
    return df


@export_as_api
def get_industry(industry, source='citics_2019', date=None, market="cn"):
    """获取行业股票列表

    :param industry: 行业名称或代码
    :param source: 分类来源。
                中国市场: citics 以及 citics_2019: 中信, gildata: 聚源
                港股: hsi: 恒生
    :param date: 查询日期，默认为当前最新日期
    :param market:  (Default value = "cn")
    :return: 所属目标行业的order_book_id list or None
    """

    industry = ensure_string(industry, "industry")
    source = ensure_string_in(source, ["sws", "citics", "gildata", "citics_2019", "hsi"], "source")
    date = ensure_date_or_today_int(date)

    res = get_client().execute("get_industry", industry, source, date, market=market)

    if not res:
        return res

    if res[-1] == "have_sector_name":
        # have_sector_name 代表 industry传入的是风格版块，产业板块或者上下游产业版块
        from rqdatac.services import basic
        res_list = basic.instruments(res[:-1])
        res = []
        date = to_date_str(date)
        for order_book in res_list:
            if order_book.de_listed_date == "0000-00-00" or order_book.de_listed_date is None:
                order_book.de_listed_date = "2099-12-31"
            if order_book.listed_date <= date <= order_book.de_listed_date:
                res.append(order_book.order_book_id)
    sub_pattern = re.compile('[A-Z]+')
    res = [sub_pattern.sub('', oid[:-4]) + oid[-4:] for oid in res]
    return sorted(res)


@export_as_api
def get_instrument_industry(order_book_ids, source='citics_2019', level=1, date=None, market="cn"):
    """获取股票对应的行业

    :param order_book_ids: 股票列表，如['000001.XSHE', '000002.XSHE']
    :param source: 分类来源。
                中国市场: citics 以及 citics_2019: 中信, gildata: 聚源
                港股: hsi: 恒生
    :param date: 如 '2015-01-07' (Default value = None)
    :param level:  (Default value = 1)
    :param market:  (Default value = "cn")
    :returns: code, name
        返回输入日期最近交易日的股票对应行业
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    source = ensure_string_in(source, ["sws", "citics", "gildata", "citics_2019", "hsi"], "source")
    if source == "citics_2019":
        check_items_in_container(level, [0, 1, 2, 3, "citics_sector"], 'level')
    else:
        check_items_in_container(level, [0, 1, 2, 3], 'level')
    date = ensure_date_or_today_int(date)

    r = get_client().execute("get_instrument_industry", order_book_ids, source, level, date, market=market)

    if not r:
        return
    res = [i['order_book_id'] for i in r]
    if source == "citics_2019" and level == "citics_sector":
        # is_special industry是否传入的是风格版块，产业板块和上下游产业版块
        from rqdatac.services import basic
        res_list = basic.instruments(res)
        date = to_date_str(date)
        for index, order_book in enumerate(res_list):
            if order_book.de_listed_date == "0000-00-00" or order_book.de_listed_date is None:
                order_book.de_listed_date = "2099-12-31"
            if not order_book.listed_date <= date <= order_book.de_listed_date:
                r.pop(index)

    return pd.DataFrame(r).set_index("order_book_id")


SHENWAN_COLUMNS = [
    "index_code",
    "index_name",
    "second_index_code",
    "second_index_name",
    "third_index_code",
    "third_index_name"
]
OTHER_COLUMNS = [
    "first_industry_code",
    "first_industry_name",
    "second_industry_code",
    "second_industry_name",
    "third_industry_code",
    "third_industry_name"
]


@export_as_api
def get_industry_mapping(source="citics_2019", date=None, market="cn"):
    """获取行业分类列表

    :param source: 分类来源。
                中国市场: citics 以及 citics_2019: 中信, gildata: 聚源
                港股: hsi: 恒生
    :param market:  (Default value = "cn")
    :return: DataFrame
    """
    source = ensure_string_in(source, ["sws", "citics", "gildata", "citics_2019", "hsi"], "source")
    if date is None:
        date = datetime.date.today()
    date = ensure_date_int(date)
    res = get_client().execute("get_industry_mapping_v2", source, date, market=market)
    if not res:
        return
    df = pd.DataFrame(res)

    if source == "sws":
        df.rename(columns=dict(zip(OTHER_COLUMNS, SHENWAN_COLUMNS)), inplace=True)
        columns = SHENWAN_COLUMNS
    else:
        columns = OTHER_COLUMNS

    df = df.dropna().drop_duplicates()
    df = df.sort_values(columns[::2]).reset_index(drop=True)
    return df[columns]