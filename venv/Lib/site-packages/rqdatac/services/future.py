# -*- coding: utf-8 -*-
import six
import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from rqdatac.validators import (
    ensure_string,
    ensure_list_of_string,
    ensure_date_int,
    ensure_date_or_today_int,
    ensure_date_range,
)
from rqdatac import get_trading_dates
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api
from rqdatac.utils import int8_to_datetime, to_datetime
from rqdatac.validators import check_items_in_container


@export_as_api
def get_dominant_future(underlying_symbol, start_date=None, end_date=None, rule=0, rank=1, market="cn"):
    import warnings

    msg = "'get_dominant_future' is deprecated, please use 'futures.get_dominant' instead"
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    return get_dominant(underlying_symbol, start_date, end_date, rule, rank, market)


@export_as_api(namespace='futures')
def get_dominant(underlying_symbol, start_date=None, end_date=None, rule=0, rank=1, market="cn"):
    """获取指定期货品种当日对应的主力合约

    :param underlying_symbol: 如'IF' 'IC'
    :param start_date: 如 '2015-01-07' (Default value = None)
    :param end_date: 如 '2015-01-08' (Default value = None)
    :param market:  (Default value = "cn")
    :param rule:  主力合约规则 (Default value = 0)
        0：在rule=1的规则上，增加约束(曾做过主力合约的合约，一旦被换下来后，不会再被选上)
        1：合约首次上市时，以当日收盘同品种持仓量最大者作为从第二个交易日开始的主力合约，当同品种其他合约持仓量在收盘后
           超过当前主力合约1.1倍时，从第二个交易日开始进行主力合约的切换。日内不会进行主力合约的切换
    :param rank:  (Default value = 1):
        1: 主力合约
        2: 次主力合约
        3：次次主力合约
    :returns: pandas.Series
        返回参数指定的具体主力合约名称

    """
    if not isinstance(underlying_symbol, six.string_types):
        raise ValueError("invalid underlying_symbol: {}".format(underlying_symbol))

    check_items_in_container(rule, [0, 1], 'rule')
    check_items_in_container(rank, [1, 2, 3], 'order')

    underlying_symbol = underlying_symbol.upper()

    if start_date:
        start_date = ensure_date_int(start_date)

    if end_date:
        end_date = ensure_date_int(end_date)
    elif start_date:
        end_date = start_date

    if rank == 1:
        result = get_client().execute(
            "futures.get_dominant", underlying_symbol, start_date, end_date, rule, market=market)
    else:
        result = get_client().execute(
            "futures.get_dominant_v2", underlying_symbol, start_date, end_date, rule, rank, market=market)

    if not result:
        return
    df = pd.DataFrame(result)
    df["date"] = df["date"].apply(int8_to_datetime)
    return df.set_index("date").sort_index()["dominant"]


_FIELDS = [
    "margin_type",
    "long_margin_ratio",
    "short_margin_ratio",
    "commission_type",
    "open_commission_ratio",
    "close_commission_ratio",
    "close_commission_today_ratio",
]


@export_as_api
def future_commission_margin(order_book_ids=None, fields=None, hedge_flag="speculation"):
    import warnings

    msg = "'future_commission_margin' is deprecated, please use 'futures.get_commission_margin' instead"
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    return get_commission_margin(order_book_ids, fields, hedge_flag)


@export_as_api(namespace='futures')
def get_commission_margin(order_book_ids=None, fields=None, hedge_flag="speculation"):
    """获取期货保证金和手续费数据

    :param order_book_ids: 期货合约, 支持 order_book_id 或 order_book_id list,
        若不指定则默认获取所有合约 (Default value = None)
    :param fields: str 或 list, 可选字段有： 'margin_type', 'long_margin_ratio', 'short_margin_ratio',
            'commission_type', 'open_commission_ratio', 'close_commission_ratio',
            'close_commission_today_ratio', 若不指定则默认获取所有字段 (Default value = None)
    :param hedge_flag: str, 账户对冲类型, 可选字段为: 'speculation', 'hedge',
            'arbitrage', 默认为'speculation', 目前仅支持'speculation' (Default value = "speculation")
    :returns: pandas.DataFrame

    """
    if order_book_ids:
        order_book_ids = ensure_list_of_string(order_book_ids)

    if fields is None:
        fields = _FIELDS
    else:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, _FIELDS, "fields")

    hedge_flag = ensure_string(hedge_flag, "hedge_flag")
    if hedge_flag not in ["speculation", "hedge", "arbitrage"]:
        raise ValueError("invalid hedge_flag: {}".format(hedge_flag))

    ret = get_client().execute("futures.get_commission_margin", order_book_ids, fields, hedge_flag)
    return pd.DataFrame(ret)


@export_as_api
def get_future_member_rank(order_book_id, trading_date=None, info_type='volume'):
    import warnings

    msg = "'get_future_member_rank' is deprecated, please use 'futures.get_member_rank' instead"
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    return get_member_rank(order_book_id, trading_date, info_type)


@export_as_api(namespace='futures')
def get_member_rank(obj, trading_date=None, rank_by='volume', **kwargs):
    """获取指定日期最近的期货会员排名数据
    :param obj： 期货合约或品种代码
    :param trading_date: 日期
    :param rank_by: 排名依据字段
    :keyword start_date
    :keyword end_date
    :returns pandas.DataFrame or None
    """
    if not kwargs:
        trading_date = ensure_date_or_today_int(trading_date)
        ret = get_client().execute("futures.get_member_rank", obj, trading_date, rank_by)
    else:
        start_date = kwargs.pop("start_date", None)
        end_date = kwargs.pop("end_date", None)
        if kwargs:
            raise ValueError('unknown kwargs: {}'.format(kwargs))
        elif start_date and end_date:
            start_date, end_date = ensure_date_int(start_date), ensure_date_int(end_date)
            ret = get_client().execute("futures.get_member_rank_v2", obj, start_date, end_date, rank_by)
        else:
            raise ValueError('please ensure start_date and end_date exist')

    if not ret:
        return

    df = pd.DataFrame(ret).sort_values(by=['trading_date', 'rank'])
    df.set_index('trading_date', inplace=True)
    return df


@export_as_api(namespace="futures")
def get_warehouse_stocks(underlying_symbols, start_date=None, end_date=None, market="cn"):
    """获取时间区间内期货的注册仓单

    :param underlying_symbols: 期货品种, 支持列表查询
    :param start_date: 如'2015-01-01', 如果不填写则为去年的当日日期
    :param end_date: 如'2015-01-01', 如果不填写则为当日日期
    :param market: 市场, 默认为"cn"
    :return: pd.DataFrame

    """
    underlying_symbols = ensure_list_of_string(underlying_symbols, name="underlying_symbols")
    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))

    # 有新老两种 symbol 时对传入的 underlying_symbols 需要对应成新的 symbol, 并对并行期结束后仍使用老的 symbol 予以警告
    multi_symbol_map = {'RO': 'OI', 'WS': 'WH', 'ER': 'RI', 'TC': 'ZC', 'ME': 'MA'}
    symbol_date_map = {'RO': 20130515, 'WS': 20130523, 'ER': 20130523, 'TC': 20160408, 'ME': 20150515}
    for symbol in set(underlying_symbols) & set(multi_symbol_map):
        date_line = symbol_date_map[symbol]
        if end_date > date_line:
            import warnings
            msg = 'You are using the old symbol: {}, however the new symbol: {} is available after {}.'.format(symbol, multi_symbol_map[symbol], date_line)
            warnings.warn(msg, category=DeprecationWarning, stacklevel=2)

    # 对传入的 underlying_symbols 依照 multi_symbol_map 生成一个对照 DataFrame
    symbol_map_df = pd.DataFrame([(symbol, multi_symbol_map.get(symbol, symbol)) for symbol in set(underlying_symbols)],
                                 columns=['origin', 'new'])
    # 将 underlying_symbols 中 所有老的 symbol 对应为新的再去 mongo 查询
    underlying_symbols = list(symbol_map_df.new.unique())
    ret = get_client().execute("futures.get_warehouse_stocks", underlying_symbols, start_date, end_date, market=market)
    if not ret:
        return
    columns = ["date", "underlying_symbol", "on_warrant", "exchange", 'effective_forecast', 'warrant_units',
               'contract_multiplier', 'deliverable']
    df = pd.DataFrame(ret, columns=columns)

    df = df.merge(symbol_map_df, left_on='underlying_symbol', right_on='new')
    df.drop(['underlying_symbol', 'new'], axis=1, inplace=True)
    df.rename(columns={'origin': 'underlying_symbol'}, inplace=True)
    df.set_index(['date', 'underlying_symbol'], inplace=True)
    return df.sort_index()


@export_as_api(namespace="futures")
def get_contract_multiplier(underlying_symbols, start_date=None, end_date=None, market="cn"):
    """获取时间区间内期货的合约乘数

    :param underlying_symbols: 期货品种, 支持列表查询
    :param start_date: 开始日期, 如'2015-01-01', 如果不填写则取underlying_symbols对应实际数据最早范围
    :param end_date: 结束日期, 如'2015-01-01', 如果不填写则为当日前一天
    :param market: 市场, 默认为"cn", 当前仅支持中国市场
    :return: pd.DataFrame

    """
    underlying_symbols = ensure_list_of_string(underlying_symbols, name="underlying_symbols")
    ret = get_client().execute("futures.get_contract_multiplier", underlying_symbols)
    if not ret:
        return

    # 因 mongo 数据为时间范围，要返回每一天的数据，需复制合约乘数数据至至范围内所有 trading_date
    if start_date:
        start_date = to_datetime(start_date)
    if not end_date:
        end_date = datetime.datetime.today() - datetime.timedelta(days=1)
    end_date = to_datetime(end_date)

    def fill(group_df):
        # 根据当前合约日期范围及给定范围内获取所有 trading_date
        date_min, date_max = group_df['effective_date'].min(), group_df['cancel_date'].max()
        if start_date is not None:
            date_min = max(start_date, date_min)
        date_max = min(date_max, end_date)
        trading_dates = pd.to_datetime(get_trading_dates(date_min, date_max)+group_df['effective_date'].to_list()).unique()

        # 使用 trading_dates 作为 index 插入并填充数据
        everyday_df = group_df.set_index(['effective_date']).reindex(trading_dates).sort_index().ffill().reset_index().rename(columns={'index': 'date'})
        everyday_df = everyday_df[(everyday_df['date'] >= date_min) & (everyday_df['date'] <= date_max)]

        return everyday_df

    df = pd.DataFrame(ret).groupby(by=['underlying_symbol']).apply(fill)

    df = df[['date', 'underlying_symbol', 'exchange', 'contract_multiplier']]
    df.set_index(['underlying_symbol', 'date'], inplace=True)
    df.sort_index(inplace=True)
    return df
