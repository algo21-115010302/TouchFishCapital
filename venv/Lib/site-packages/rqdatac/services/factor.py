# -*- coding: utf-8 -*-
import datetime
import warnings
from itertools import chain
from collections import OrderedDict

import pandas as pd
import numpy as np

from rqdatac.services.calendar import (
    get_previous_trading_date,
    get_next_trading_date,
    get_trading_dates,
)
from rqdatac.validators import (
    ensure_list_of_string,
    ensure_date_int,
    ensure_date_range,
    ensure_string,
    ensure_string_in,
    check_items_in_container,
    ensure_order_book_ids,
    ensure_order_book_id,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api


@export_as_api
def get_all_factor_names(market="cn"):
    """获取因子列表

    :return:
        list

    :param market:  (Default value = "cn")

    """
    return get_client().execute("get_all_factor_names", market)


@export_as_api
def get_factor(order_book_ids, factor, start_date=None, end_date=None, universe=None, expect_df=False, **kwargs):
    """获取因子

    :param order_book_ids: 股票代码或代码列表
    :param factor: 如 'total_income'
    :param date: 如 date='2015-01-05', 默认为前一交易日
    :param start_date: 开始日期'2015-01-05', 默认为前一交易日, 最小起始日期为'2000-01-04'
    :param end_date: 结束日期
    :param universe: 股票池，默认为全A股
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :returns: pd.DataFrame
    """

    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS")
    order_book_ids = list(set(order_book_ids))

    factor = ensure_list_of_string(factor)
    factor = list(OrderedDict.fromkeys(factor))

    if start_date and end_date:
        start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=15))
        if start_date < 20000104:
            warnings.warn("start_date is earlier than 2000-01-04, adjusted to 2000-01-04")
            start_date = 20000104
    elif start_date:
        raise ValueError("Expect end_date")
    elif end_date:
        raise ValueError("Expect start_date")
    else:
        date = kwargs.pop("date", None)
        date = ensure_date_int(date or get_previous_trading_date(datetime.date.today()))
        start_date = end_date = date

    if kwargs:
        raise ValueError('unknown kwargs: {}'.format(kwargs))

    if universe is not None:
        universe = ensure_string(universe, "universe")
        if universe != "all":
            universe = ensure_order_book_id(universe, type="INDX")
            from rqdatac import index_components
            allowed_order_book_ids = set(index_components(universe, date=end_date) or [])
            not_permit_order_book_ids = [
                order_book_id
                for order_book_id in order_book_ids if order_book_id not in allowed_order_book_ids
            ]
            if not_permit_order_book_ids:
                warnings.warn(
                    "%s not in universe pool, value of those order_book_ids will always be NaN"
                    % not_permit_order_book_ids
                )

    data = get_client().execute(
        "get_factor_from_store", order_book_ids, factor, start_date, end_date, universe=universe
    )

    if not data:
        return

    factor_value_length = len(data[0][2])
    if factor_value_length == 0:
        return

    dates = pd.to_datetime(get_trading_dates(start_date, end_date))
    days = len(dates)
    if days > factor_value_length:
        _get_factor_warning_msg(dates[factor_value_length], dates[-1])
        dates = dates[0:factor_value_length]

    if expect_df or len(factor) > 1:
        order_book_id_index_map = {o: i for i, o in enumerate(order_book_ids)}
        factor_index_map = {f: i for i, f in enumerate(factor)}
        arr = np.full((len(order_book_ids) * days, len(factor)), np.nan)

        for order_book_id, factor_name, values in data:
            if not values:
                continue
            value_length = min(days, len(values))
            order_book_id_index = order_book_id_index_map[order_book_id]
            factor_index = factor_index_map[factor_name]
            start = order_book_id_index * days
            arr[start: start + value_length, factor_index] = values[-value_length:]

        multi_index = pd.MultiIndex.from_product([order_book_ids, dates], names=["order_book_id", "date"])
        df = pd.DataFrame(index=multi_index, columns=factor, data=arr)
        return df

    order_book_id_index_map = {o: i for i, o in enumerate(order_book_ids)}
    arr = np.full((days, len(order_book_ids)), np.nan)
    for order_book_id, _, values in data:
        arr[:len(values), order_book_id_index_map[order_book_id]] = values
    df = pd.DataFrame(index=dates, columns=order_book_ids, data=arr)

    if len(df.index) == 1:
        return df.iloc[0]
    if len(df.columns) == 1:
        return df[df.columns[0]]
    return df


def _get_factor_warning_msg(start_date, end_date):
    if start_date == end_date:
        end_date = end_date.strftime("%Y%m%d")
        warnings.warn("{} calculation not completed".format(end_date))
    else:
        start_date = start_date.strftime("%Y%m%d")
        end_date = end_date.strftime("%Y%m%d")
        warnings.warn(
            "{} - {} calculation not completed".format(start_date, end_date))


_UNIVERSE_MAPPING = {
    "whole_market": "whole_market",
    "000300.XSHG": "csi_300",
    "000905.XSHG": "csi_500",
    "000906.XSHG": "csi_800",
}

_METHOD_MAPPING = {"explicit": "explicit_factor_return", "implicit": "implicit_factor_return"}


@export_as_api
def get_factor_return(
    start_date, end_date, factors=None, universe="whole_market", method="implicit", industry_mapping=True, market="cn",
):
    """获取因子收益率数据

    :param start_date: 开始日期（例如：‘2017-03-03’)
    :param end_date: 结束日期（例如：‘2017-03-20’)
    :param factors: 因子。默认获取全部因子的因子收益率
        当 method 参数取值为'implicit' ，可返回全部因子（风格、行业、市场联动）的隐式因子收益率；
        当 method 参数取值为'explicit' , 只返回风格因子的显式因子收益率。具体因子名称见说明文档 (Default value = None)
    :param universe: 股票池。默认调用全市场收益率。可选沪深300（‘000300.XSHG’）、中证500（'000905.XSHG'）
        、以及中证800（'000906.XSHG'） (Default value = "whole_market")
    :param method: 计算方法。默认为'implicit'（隐式因子收益率），可选'explicit'（显式风格因子收益率) (Default value = "implicit")
    :param market: 地区代码， 现在仅支持 'cn' (Default value = "cn")
    :param industry_mapping(bool): 是否按 2014 年后的申万行业分类标 准计算行业收益率.默认为 True.
        若取值为 False,则 2014 年前的行业 收益率按旧行业分类标准计算
    :returns: pd.DataFrame. index 为日期，column 为因子字段名称。

    Usage example::
        # 获取介于2017-03-03 到 2017-03-20到隐式因子收益率数据
        get_factor_return('2017-03-03', '2017-03-20')

    """
    start_date, end_date = ensure_date_range(start_date, end_date)

    if factors:
        factors = ensure_list_of_string(factors)

    method = ensure_string(method)
    if method not in _METHOD_MAPPING:
        raise ValueError("invalid method: {!r}, valid: explicit, implicit".format(method))
    method = _METHOD_MAPPING[method]

    if universe not in _UNIVERSE_MAPPING:
        raise ValueError(
            "invalid universe: {!r}, valid: {}".format(universe, list(_UNIVERSE_MAPPING.keys()))
        )
    universe = _UNIVERSE_MAPPING[universe]

    df = get_client().execute(
        "get_factor_return", start_date, end_date, factors, universe, method, market=market, industry_mapping=industry_mapping
    )
    if not df:
        return None
    df = pd.DataFrame(df)
    # convert to required format.
    df = df.pivot(index="date", columns="factor")[universe]
    df.sort_index(inplace=True)
    return df


_EXPOSURE_FACTORS = (
    "beta",
    "growth",
    "liquidity",
    "leverage",
    "momentum",
    "reversal",
    "size",
    "yield",
    "volatility",
    "value",
)


@export_as_api
def get_factor_exposure(order_book_ids, start_date=None, end_date=None, factors=None, industry_mapping=True, market="cn"):
    """获取因子暴露度

    :param order_book_ids: 股票代码或代码列表
    :param start_date: 如'2013-01-04' (Default value = None)
    :param end_date: 如'2014-01-04' (Default value = None)
    :param factors: 如'yield', 'beta', 'volatility' (Default value = None)
    :param market: 地区代码, 如'cn' (Default value = "cn")
    :param industry_mapping (bool): 是否按 2014 年后的申万行业分类标 准计算行业暴露度.默认为 True.
        若取值为 False,则 2014 年前的行业 暴露度按旧行业分类标准计算
    :returns: MultiIndex DataFrame. index 第一个 level 为 order_book_id，第 二个 level 为 date，columns 为因子字段名称
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if not order_book_ids:
        raise ValueError("no valid order_book_id found")

    start_date, end_date = ensure_date_range(start_date, end_date)

    if factors is not None:
        factors = ensure_list_of_string(factors)
        check_items_in_container(factors, exposure_factors, "factors")
    results = get_client().execute(
        "get_factor_exposure", order_book_ids, start_date, end_date, factors, market,
        industry_mapping
    )

    if not results:
        return None
    index_pairs = []
    data = []

    fields = [
        field for field in results[0].keys() if field not in ("order_book_id", "date", "industry")
    ]

    industry_factors = _get_all_industries(start_date, end_date, industry_mapping)
    for result in results:
        index_pairs.append((result["date"], result["order_book_id"]))
        row_data = [result.get(field, np.nan) for field in fields]

        # 填充行业因子数据
        for industry in industry_factors:
            if result["industry"] == industry:
                industry_label = 1
            else:
                industry_label = 0
            row_data.append(industry_label)

        data.append(row_data)

    index = pd.MultiIndex.from_tuples(index_pairs, names=["date", "order_book_id"])
    fields.extend(industry_factors)
    result_df = pd.DataFrame(columns=fields, index=index, data=data)
    result_df.sort_index(level=1, inplace=True)

    no_data_book_id = set(order_book_ids) - set(result_df.index.levels[1])
    if no_data_book_id:
        warnings.warn("No data for this order_book_id :{}".format(no_data_book_id))

    if factors is not None:
        exists_factors = list(set(factors) & set(result_df))
        # shenwan industry has re-structured on 2014-01-01, so if industry_mapping is true
        # and given factor is `old shenwan industry`, the `old shenwan industry` will not exists
        # in the result dataframe columns.
        if not exists_factors:
            return None
        return result_df[exists_factors]
    return result_df


def _get_all_industries(start_date, end_date, industry_mapping=True):
    """ 获取在指定时间区间内申万的所有行业分类

    :param start_date (int): 开始日期
    :param end_date (int): 结束日期
    :param industry_mapping (bool): 是否要使用行业映射。如果使用行业映射的话, 那么2014年
        以前的行业分类将会全部都用2014年以后的行业分类代替
    """
    if industry_mapping is True or start_date >= 20140101:
        return SHENWAN_INDUSTRY_NAME_AFTER_2014
    # 申万在2014年1月1号进行过一次行业重组
    if end_date <= 20140101:
        return SHENWAN_INDUSTRY_NAME_BEFORE_2014
    else:
        # 时间区间会跨过 2014-01-01
        return list(
            set(chain(SHENWAN_INDUSTRY_NAME_BEFORE_2014, SHENWAN_INDUSTRY_NAME_AFTER_2014))
        )


exposure_factors = [
    "residual_volatility",
    "growth",
    "liquidity",
    "beta",
    "non_linear_size",
    "leverage",
    "earnings_yield",
    "size",
    "momentum",
    "book_to_price",
    "comovement",
]

SHENWAN_INDUSTRY_NAME_BEFORE_2014 = [
    u"金融服务",
    u"房地产",
    u"医药生物",
    u"有色金属",
    u"餐饮旅游",
    u"综合",
    u"建筑建材",
    u"家用电器",
    u"交运设备",
    u"食品饮料",
    u"电子",
    u"信息设备",
    u"交通运输",
    u"轻工制造",
    u"公用事业",
    u"机械设备",
    u"纺织服装",
    u"农林牧渔",
    u"商业贸易",
    u"化工",
    u"信息服务",
    u"采掘",
    u"黑色金属",
]

SHENWAN_INDUSTRY_NAME_AFTER_2014 = [
    u"农林牧渔",
    u"采掘",
    u"化工",
    u"钢铁",
    u"有色金属",
    u"电子",
    u"家用电器",
    u"食品饮料",
    u"纺织服装",
    u"轻工制造",
    u"医药生物",
    u"公用事业",
    u"交通运输",
    u"房地产",
    u"商业贸易",
    u"休闲服务",
    u"综合",
    u"建筑材料",
    u"建筑装饰",
    u"电气设备",
    u"国防军工",
    u"计算机",
    u"传媒",
    u"通信",
    u"银行",
    u"非银金融",
    u"汽车",
    u"机械设备",
]

exposure_factors.extend(SHENWAN_INDUSTRY_NAME_BEFORE_2014)
exposure_factors.extend(SHENWAN_INDUSTRY_NAME_AFTER_2014)

_STYLE_FACTORS = {
    "residual_volatility",
    "growth",
    "liquidity",
    "beta",
    "non_linear_size",
    "leverage",
    "earnings_yield",
    "size",
    "momentum",
    "book_to_price"
}


@export_as_api
def get_style_factor_exposure(order_book_ids, start_date, end_date, factors=None, market="cn"):
    """获取个股风格因子暴露度

    :param order_book_ids: 证券代码（例如：‘600705.XSHG’）
    :param start_date: 开始日期（例如：‘2017-03-03’）
    :param end_date: 结束日期（例如：‘2017-03-20’）
    :param factors: 风格因子。默认调用全部因子的暴露度（'all'）。
        具体因子名称见说明文档 (Default value = None)
    :param market:  (Default value = "cn")

    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if factors is not None:
        factors = ensure_list_of_string(factors)
        check_items_in_container(factors, _STYLE_FACTORS, "factors")

    df = get_client().execute(
        "get_style_factor_exposure", order_book_ids, start_date, end_date, factors, market=market
    )
    if not df:
        return
    return pd.DataFrame(df).set_index(["order_book_id", "date"]).sort_index(level=1)


_DESCRIPTORS = {
    "daily_standard_deviation",
    "cumulative_range",
    "historical_sigma",
    "one_month_share_turnover",
    "three_months_share_turnover",
    "twelve_months_share_turnover",
    "earnings_to_price_ratio",
    "cash_earnings_to_price_ratio",
    "market_leverage",
    "debt_to_assets",
    "book_leverage",
    "sales_growth",
    "earnings_growth",
}


@export_as_api
def get_descriptor_exposure(order_book_ids, start_date, end_date, descriptors=None, market="cn"):
    """获取个股细分因子暴露度

    :param order_book_ids: 证券代码（例如：‘600705.XSHG’）
    :param start_date: 开始日期（例如：‘2017-03-03’）
    :param end_date: 结束日期（例如：‘2017-03-20’）
    :param descriptors: 细分风格因子。默认调用全部因子的暴露度（'all'）。
        具体细分因子名称见说明文档 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: MultiIndex DataFrame. index 第一个 level 为 order_book_id，第 二个 level 为 date，column 为细分风格因子字段名称。
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if descriptors is not None:
        if descriptors == "all":
            descriptors = None
        else:
            descriptors = ensure_list_of_string(descriptors)
            check_items_in_container(descriptors, _DESCRIPTORS, "descriptors")

    df = get_client().execute(
        "get_descriptor_exposure", order_book_ids, start_date, end_date, descriptors, market=market
    )
    if not df:
        return
    return pd.DataFrame(df).set_index(["order_book_id", "date"]).sort_index(level=1)


@export_as_api
def get_stock_beta(order_book_ids, start_date, end_date, benchmark="000300.XSHG", market="cn"):
    """获取个股相对于基准的贝塔

    :param order_book_ids: 证券代码（例如：‘600705.XSHG’）
    :param start_date: 开始日期（例如：‘2017-03-03’)
    :param end_date: 结束日期（例如：‘2017-03-20’）
    :param benchmark: 基准指数。默认为沪深300（‘000300.XSHG’）
        可选上证50（'000016.XSHG'）、中证500（'000905.XSHG'）、
        中证800（'000906.XSHG'）以及中证全指（'000985.XSHG'） (Default value = "000300.XSHG")
    :param market:  (Default value = "cn")
    :returns: pandas.DataFrame，index 为日期，column 为个股的 order_book_id
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)

    all_benchmark = ("000300.XSHG", "000016.XSHG", "000905.XSHG", "000906.XSHG", "000985.XSHG")
    benchmark = ensure_string(benchmark, "benchmark")
    check_items_in_container(benchmark, all_benchmark, "benchmark")
    benchmark = benchmark.replace(".", "_")
    df = get_client().execute(
        "get_stock_beta", order_book_ids, start_date, end_date, benchmark, market=market
    )
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index="date", columns="order_book_id", values=benchmark).sort_index()
    return df


def get_eigenfactor_adjusted_covariance(date, horizon='daily'):
    """ 获取因子协方差矩阵（特征因子调整）

    :param date: str 日期（例如：‘2017-03-20’）
    :param horizon: str 预测期限。默认为日度（'daily'），可选月度（‘monthly’）或季度（'quarterly'）。

    :return: pandas.DataFrame，其中 index 和 column 均为因子名称。
    """
    date = get_previous_trading_date(get_next_trading_date(date))
    date = ensure_date_int(date)
    ensure_string_in(horizon, HORIZON_CONTAINER, 'horizon')

    df = get_client().execute('get_eigenfactor_adjusted_covariance', date, horizon)
    if not df:
        return
    df = pd.DataFrame(df)
    df.drop("date", axis=1, inplace=True)
    return df.reindex(columns=df.index)


@export_as_api
def get_factor_covariance(date, horizon='daily'):
    """ 获取因子协方差矩阵

    :param date: str 日期（例如：‘2017-03-20’）
    :param horizon: str 预测期限。默认为日度（'daily'），可选月度（‘monthly’）或季度（'quarterly'）。

    :return: pandas.DataFrame，其中 index 和 column 均为因子名称。
    """
    date = get_previous_trading_date(get_next_trading_date(date))
    date = ensure_date_int(date)
    ensure_string_in(horizon, HORIZON_CONTAINER, 'horizon')

    df = get_client().execute('get_factor_covariance', date, horizon)
    if not df:
        return
    df = pd.DataFrame(df)
    df.drop("date", axis=1, inplace=True)
    return df.reindex(columns=df.index)


@export_as_api
def get_specific_return(order_book_ids, start_date, end_date):
    """ 获取个股特异收益率

    :param order_book_ids	str or [list of str]	证券代码（例如：‘600705.XSHG’）
    :param start_date	    str                 	开始日期（例如：‘2017-03-03’）
    :param end_date	        str	                    结束日期（例如：‘2017-03-20’）

    :return: pandas.DataFrame，其中 index 为date, column 为 order_book_ids。
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)

    df = get_client().execute('get_specific_return', order_book_ids, start_date, end_date)
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index='date', columns='order_book_id', values="specific_return").sort_index()
    return df


@export_as_api
def get_specific_risk(order_book_ids, start_date, end_date, horizon='daily'):
    """ 获取个股特异方差

    :param order_book_ids	str or [list of str]	证券代码（例如：‘600705.XSHG’）
    :param start_date	    str                 	开始日期（例如：‘2017-03-03’）
    :param end_date	        str	                    结束日期（例如：‘2017-03-20’）
    :param horizon	        str	    预测期限。默认为日度（'daily'），可选月度（‘monthly’）或季度（'quarterly'）

    :return: pandas.DataFrame，其中 index 为date, column 为 order_book_ids。
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    ensure_string_in(horizon, HORIZON_CONTAINER, 'horizon')

    df = get_client().execute('get_specific_risk', order_book_ids, start_date, end_date, horizon)
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index="date", columns="order_book_id", values="specific_risk").sort_index()
    return df


def get_cross_sectional_bias(start_date, end_date, type='factor'):
    """ 获取横截面偏差系数

    :param order_book_ids	str or [list of str]	证券代码（例如：‘600705.XSHG’）
    :param start_date	    str                 	开始日期（例如：‘2017-03-03’）
    :param end_date	        str	                    结束日期（例如：‘2017-03-20’）
    :param type	            str	                    默认为 'factor'，可选 'specific'

    :return: pandas.DataFrame，其中 index 为date, column 包含 'daily'、'monthly'  和 'quarterly' 三个字段。
    """
    start_date, end_date = ensure_date_range(start_date, end_date)
    ensure_string_in(type, ['factor', 'specific'], 'horizon')

    df = get_client().execute('get_cross_sectional_bias', start_date, end_date, type)
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index='date', columns='horizon', values="bias").sort_index()
    return df


HORIZON_CONTAINER = ['daily', 'monthly', 'quarterly']


@export_as_api
def get_index_factor_exposure(
    order_book_ids, start_date=None, end_date=None, factors=None, market="cn"
):
    """获取因子暴露度

    :param order_book_ids: 股票代码或代码列表
    :param start_date: 如'2013-01-04' (Default value = None)
    :param end_date: 如'2014-01-04' (Default value = None)
    :param factors: 如'yield', 'beta', 'volatility' (Default value = None)
    :param market: 地区代码, 如'cn' (Default value = "cn")
    """
    try:
        order_book_ids = ensure_order_book_ids(order_book_ids, type="INDX")
    except ValueError:
        return

    start_date, end_date = ensure_date_range(start_date, end_date)

    if factors is not None:
        factors = ensure_list_of_string(factors)
        check_items_in_container(factors, exposure_factors, "factors")

    results = get_client().execute(
        "get_index_factor_exposure", order_book_ids, start_date, end_date, factors, market=market
    )

    if not results:
        return None
    df = pd.DataFrame.from_records(results, index=['date', 'order_book_id'])
    df.sort_index(inplace=True)
    return df
