# -*- coding: utf-8 -*-
import datetime
import bisect
import re
import warnings
from copy import deepcopy

import six
import pandas as pd

from rqdatac.client import get_client
from rqdatac.utils import to_date, datetime_to_int14, to_date_str, str_to_dt_time, int8_to_date
from rqdatac.validators import (
    ensure_list_of_string,
    ensure_date_int,
    check_type,
    ensure_date_str,
    ensure_order_book_id,
    ensure_order_book_ids,
    check_items_in_container,
    ensure_date_range,
    ensure_int,
    ensure_string,
    ensure_date_or_today_int,
    ensure_string_in,
)
from rqdatac.services.concept import concept_names as get_concept_names
from rqdatac.services.shenwan import get_instrument_industry
from rqdatac.services.constant import SectorCode, SectorCodeItem, IndustryCode, IndustryCodeItem
from rqdatac.services.calendar import get_previous_trading_date, is_trading_date, has_night_trading
from rqdatac.decorators import export_as_api, ttl_cache, compatible_with_parm
from dateutil.relativedelta import relativedelta


def _id_convert_one(order_book_id):  # noqa: C901
    # hard code
    if order_book_id in {"T00018", "T00018.SH", "T00018.XSHG", "SH.T00018"}:
        return "990018.XSHG"

    if order_book_id.isdigit():
        if order_book_id.startswith("0") or order_book_id.startswith("3"):
            return order_book_id + ".XSHE"
        elif (
                order_book_id.startswith("5")
                or order_book_id.startswith("6")
                or order_book_id.startswith("9")
                or order_book_id.startswith("15")
        ):
            return order_book_id + ".XSHG"
        else:
            raise ValueError("order_book_ids should be str like 000001, 600000")
    order_book_id = order_book_id.upper()
    if order_book_id.endswith(".XSHG") or order_book_id.endswith(".XSHE"):
        return order_book_id

    if order_book_id.startswith("SZ"):
        return order_book_id.replace(".", "")[2:] + ".XSHE"
    elif order_book_id.startswith("SH"):
        return order_book_id.replace(".", "")[2:] + ".XSHG"
    elif order_book_id.endswith("SZ"):
        return order_book_id.replace(".", "")[:-2] + ".XSHE"
    elif order_book_id.endswith("SH"):
        return order_book_id.replace(".", "")[:-2] + ".XSHG"

    # 期货
    order_book_id = order_book_id.replace("-", "").split(".")[0]
    try:
        res = re.findall(r"^([A-Z]+)(\d+)([PC]\w+)?", order_book_id)[0]
    except IndexError:
        raise ValueError("unknown order_book_id: {}".format(order_book_id))
    if len(res[1]) is 3 and res[1] != '888':
        year = str(datetime.datetime.now().year + 3)
        # 按照当前年份+3之后的年份取十位位置上数字递减重新组合为 order_book_id列表 去查询 trading_code 相等的最大合约(即返回结果 trading_code 相等的首个合约)
        # bug：当 year[-2] = 0(2097年)，若(设输入为'TA312')没有查到此 order_book_id('TA0312')， 则不会对三位代码补位而直接返回原 trading_code('TA312')
        ins_infos = instruments([res[0] + str(n) + res[1] + res[2] for n in range(int(year[-2]), -1, -1)])
        for ins_info in ins_infos:
            if ins_info is None:
                continue
            trading_code = getattr(ins_info, "trading_code", "")
            if trading_code.upper() == order_book_id.upper():
                return ins_info.order_book_id
    return order_book_id


@export_as_api
def id_convert(order_book_ids):
    """合约格式转换

    :param order_book_ids: str 或 str list, 如'000001', 'SZ000001', '000001SZ',
        '000001.SZ', 纯数字str默认为股票类型
    :returns: str 或 str list, 米筐格式的合约

    """
    if isinstance(order_book_ids, six.string_types):
        return _id_convert_one(order_book_ids)
    elif isinstance(order_book_ids, list):
        return [_id_convert_one(o) for o in order_book_ids]
    else:
        raise ValueError("order_book_ids should be str or list")


def _id_compatible(order_book_id):
    if order_book_id.endswith("XSHE"):
        return order_book_id[:-4] + "SZ"
    elif order_book_id.endswith("XSHG"):
        return order_book_id[:-4] + "SH"
    else:
        return order_book_id


def _all_instruments_list(market):
    ins = [Instrument(i) for i in get_client().execute("all_instruments", market=market)]

    extra_hk_instruments = []
    if market == 'hk':  # 对港股需要根据 stock_connect 字段做拆分并替换该字段
        suffix_map = {'sz': 'XSEC', 'sh': 'XSSC'}
        for i in ins:
            if not i.stock_connect:  # 港股中 stcok_connect 字段为空则设置该字段为 ''
                setattr(i, 'stock_connect', '')
                continue
            for stock_connect in i.stock_connect:
                _temp_instruments = deepcopy(i)
                _temp_instruments.unique_id = _temp_instruments.unique_id[:-4] + suffix_map.get(stock_connect)
                _temp_instruments.stock_connect = stock_connect + '_connect'
                extra_hk_instruments.append(_temp_instruments)
            i.stock_connect = i.stock_connect[0] if len(i.stock_connect) == 1 else '_and_'.join(i.stock_connect)

    ins += extra_hk_instruments
    return ins


@ttl_cache(3 * 3600)
def _all_cached_instruments_list(market):
    return _all_instruments_list(market)


@ttl_cache(3 * 3600)
def _all_instruments_dict(market):
    ins = _all_cached_instruments_list(market)
    result = dict()
    for i in ins:
        result[i.symbol] = i
        if i.type == "Convertible":
            result[_id_compatible(i.order_book_id)] = i

        if getattr(i, "unique_id", None):  # 对港股 unique_id 作为 key 添加到 result dict
            result[i.unique_id] = i

        if i.order_book_id in result:  # 对港股存在退市后 order_book_id 复用的情况只存上市日期最晚的信息
            if i.listed_date > result[i.order_book_id].listed_date:
                result[i.order_book_id] = i
        else:
            result[i.order_book_id] = i

    try:
        result["沪深300"] = result["000300.XSHG"]
        result["中证500"] = result["000905.XSHG"]
        result[result["SSE180.INDX"].symbol] = result["000010.XSHG"]
    except KeyError:
        pass

    return result


def get_underlying_listed_date(underlying_symbol, ins_type, market="cn"):
    """ 获取期货或者期权的某个品种的上市日期"""
    ins_list = _all_cached_instruments_list(market)
    listed_dates = [i.listed_date for i in ins_list
                    if (getattr(i, "underlying_symbol", "") == underlying_symbol
                        and i.type == ins_type and i.listed_date != "0000-00-00")]

    return min(listed_dates)


def get_tick_size(order_book_id, market="cn"):
    """获取合约价格最小变动单位

    :param order_book_id: 如: FU1703
    :param market: 如：'cn' (Default value = "cn")
    :returns: float

    """
    return get_client().execute("get_tick_size", order_book_id, market=market)


HK_STOCK_PRICE_SECTIONS = [0.25, 0.5, 10, 20, 100, 200, 500, 1000, 2000, 5000]
HK_STOCK_TICK_SIZES = [0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5]


# noinspection All
class Instrument(object):
    def __init__(self, d):
        self.__dict__ = d

    def __repr__(self):
        if self.has_citics_info() and not hasattr(self, "_citics_industry_code"):
            self.citics_industry()

        return "{}({})".format(
            type(self).__name__,
            ", ".join(
                [
                    "{}={!r}".format(k.lstrip("_"), v)
                    for k, v in self.__dict__.items()
                    if v is not None
                ]
            ),
        )

    @property
    def concept_names(self):
        return get_concept_names(self.order_book_id)

    def days_from_listed(self, date=None):
        if self.listed_date == "0000-00-00":
            return -1

        date = to_date(date) if date else datetime.date.today()
        if self.de_listed_date != "0000-00-00" and date > to_date(self.de_listed_date):
            # 晚于退市日期
            return -1

        listed_date = to_date(self.listed_date)
        ipo_days = (date - listed_date).days
        return ipo_days if ipo_days >= 0 else -1

    def days_to_expire(self, date=None):
        if getattr(self, 'maturity_date', '0000-00-00') == '0000-00-00':
            return -1

        date = to_date(date) if date else datetime.date.today()

        maturity_date = to_date(self.maturity_date)
        days = (maturity_date - date).days
        return days if days >= 0 else -1

    def tick_size(self, price=None):
        if self.exchange == "XHKG":
            check_type(price, (int, float), "price")
            index = bisect.bisect_left(HK_STOCK_PRICE_SECTIONS, price)
            return HK_STOCK_TICK_SIZES[index]
        elif self.type in ["CS", "INDX"]:
            return 0.01
        elif self.type in ["ETF", "LOF", "FenjiB", "FenjiA", "FenjiMu"]:
            return 0.001
        elif self.type == "Convertible":
            if self.exchange == "XSHG":
                return 0.01
            else:
                return 0.001
        elif self.type not in ["Future", "Option", "Spot"]:
            return -1
        return get_tick_size(self.order_book_id)

    def has_citics_info(self):
        return self.type == "CS" and self.exchange in {"XSHE", "XSHG"}

    def citics_industry(self, date=None):
        if self.has_citics_info():
            if date is None:
                if hasattr(self, "_citics_industry_code"):
                    return (self._citics_industry_code, self._citics_industry_name)

            if self.de_listed_date != '0000-00-00':
                date = get_previous_trading_date(self.de_listed_date)

            result = get_instrument_industry(self.order_book_id, date=date, level=1, source='citics_2019')
            if result is None:
                self._citics_industry_code, self._citics_industry_name = (None, None)
                return None

            self._citics_industry_code = result['first_industry_code'][0]
            self._citics_industry_name = result['first_industry_name'][0]

            return {"code": result.iloc[0, 0], "name": result.iloc[0, 1]}

    @property
    def citics_industry_code(self):
        if not self.has_citics_info():
            return None

        if not hasattr(self, "_citics_industry_code"):
            self.citics_industry()
        return self._citics_industry_code

    @property
    def citics_industry_name(self):
        if not self.has_citics_info():
            return None

        if not hasattr(self, "_citics_industry_name"):
            self.citics_industry()
        return self._citics_industry_name


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def instruments(order_book_ids, market="cn"):
    """获取证券详细信息

    :param order_book_ids: 证券ID列表, 如'000001.XSHE', 'AAPL.US'. 注意, 所有列表中的证券需要属于同一个国家。
    :param market: 证券所属国家, 如 cn, us, hk (Default value = "cn")
    :returns: 对应证券的列表

    """

    all_dict = _all_instruments_dict(market)
    if isinstance(order_book_ids, six.string_types):
        try:
            return all_dict[order_book_ids]
        except KeyError:
            warnings.warn('unknown order_book_id: {}'.format(order_book_ids))
            return
    all_list = (all_dict.get(i) for i in order_book_ids)
    return [i for i in all_list if i]


VALID_TYPES = {"CS", "ETF", "LOF", "INDX", "Future", "Spot", "Option", "Convertible", "Repo"}


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def all_instruments(type=None, date=None, market="cn", **kwargs):
    """获得某个国家的全部证券信息

    :param type:  (Default value = None)
    :param date:  (Default value = None)
    :param market: cn, hk (Default value = "cn")
    :kwargs
        trading_market: [hk, all] (Default value = "hk")
            hk: 港交所可购买的股票。对应返回stock_connect = null、sh、sz 的记录
            all: 包括港交所、上交所、深交所可购买的港股。（对沪深港通支持股票均展示一条独立的unique_id捆绑的信息）,对应返回全部列表，即stock_connect = null、sz_and_sh、sh、sz、sz_connect、sh_connect
    """

    if type is None:
        itype = VALID_TYPES
    else:
        type = ensure_list_of_string(type)
        itype = set()
        for t in type:
            if t.upper() == "STOCK":
                itype.add("CS")
            elif t.upper() == "FUND":
                itype = itype.union({"ETF", "LOF"})
            elif t.upper() == "INDEX":
                itype.add("INDX")
            elif t not in VALID_TYPES:
                raise ValueError("invalid type: {}, chose any in {}".format(type, VALID_TYPES))
            else:
                itype.add(t)

    if date:
        date = ensure_date_str(date)
        cond = lambda x: (  # noqa: E731
                x.type in itype
                and (x.listed_date <= date or x.listed_date == "0000-00-00")
                and (
                        x.de_listed_date == "0000-00-00"
                        or (
                                x.de_listed_date >= date
                                and x.type in ("Future", "Option")
                                or (x.de_listed_date > date and x.type not in ("Future", "Option"))
                        )
                )
        )
    else:
        cond = lambda x: x.type in itype  # noqa: E731

    cached = kwargs.pop("cached", True)
    trading_market = kwargs.pop("trading_market", 'hk')
    if kwargs:
        raise ValueError("Unknown kwargs: {}".format(kwargs))

    if cached:
        get_instrument_list = _all_cached_instruments_list
    else:
        get_instrument_list = _all_instruments_list

    ins_ret = filter(cond, get_instrument_list(market))

    if market == 'hk' and trading_market == 'hk':
        ins_ret = filter(lambda x: x.unique_id.endswith('.XHKG'), ins_ret)

    if len(itype) == 1:
        df = pd.DataFrame([v.__dict__ for v in ins_ret])
        internal_fields = [f for f in df.columns if f.startswith('_')]
        for f in internal_fields:
            del df[f]
    else:
        df = pd.DataFrame(
            [
                (
                    v.order_book_id,
                    v.symbol,
                    getattr(v, "abbrev_symbol", None),
                    v.type,
                    v.listed_date,
                    v.de_listed_date,
                )
                for v in ins_ret
            ],
            columns=[
                "order_book_id",
                "symbol",
                "abbrev_symbol",
                "type",
                "listed_date",
                "de_listed_date",
            ],
        )
    return df


def to_sector_name(s):
    for _, v in SectorCode.__dict__.items():
        if isinstance(v, SectorCodeItem):
            if v.cn == s or v.en == s or v.name == s:
                return v.name
    return s


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def sector(code, market="cn"):
    """获取某个板块的股票列表。目前支持的板块分类具体可以查询以下网址:
    https://www.ricequant.com/api/research/chn#research-API-sector

    :param code: 可以使用板块英文名字如'Energy', 或者 sector_code.Energy
    :param market: 地区代码, 如'cn' (Default value = "cn")
    :returns: 板块全部股票列表

    """
    check_type(code, (SectorCodeItem, six.string_types), "code")
    if isinstance(code, SectorCodeItem):
        code = code.name
    else:
        code = to_sector_name(code)
    return [
        v.order_book_id
        for v in _all_cached_instruments_list(market)
        if v.type == "CS" and v.sector_code == code
    ]


def to_industry_code(s):
    for _, v in IndustryCode.__dict__.items():
        if isinstance(v, IndustryCodeItem):
            if v.name == s:
                return v.code
    return s


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def industry(code, market="cn"):
    """获取某个行业的股票列表。目前支持的行业列表具体可以查询以下网址:
    https://www.ricequant.com/api/research/chn#research-API-industry

    :param code: 行业代码,如 A01, 或者 industry_code.A01
    :param market: 地区代码, 如'cn' (Default value = "cn")
    :returns: 行业全部股票列表

    """
    if not isinstance(code, six.string_types):
        code = code.code
    else:
        code = to_industry_code(code)
    return [
        v.order_book_id
        for v in _all_cached_instruments_list(market)
        if v.type == "CS" and v.industry_code == code
    ]


@export_as_api
def get_future_contracts(underlying_symbol, date=None, market="cn"):
    import rqdatac
    import warnings

    msg = "'get_future_contracts' is deprecated, please use 'futures.get_contracts' instead"
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    return rqdatac.futures.get_contracts(underlying_symbol, date, market)


@export_as_api(namespace='futures')
def get_contracts(underlying_symbol, date=None, market="cn"):
    """获得中国市场某个时间某个期货品种正在交易的合约列表

    :param underlying_symbol: 期货品种
    :param date: 日期，可以为str，datetime，date，pd.Timestamp 等类型
    :param market:  (Default value = "cn")
    :returns: list of order book id

    """
    if date is None:
        date = datetime.date.today()
    date = ensure_date_str(date)

    return sorted(
        v.order_book_id
        for v in _all_cached_instruments_list(market)
        if v.type == "Future"
        and v.underlying_symbol == underlying_symbol
        and v.listed_date != "0000-00-00"
        and v.listed_date <= date <= v.de_listed_date
    )


@export_as_api
def jy_instrument_industry(order_book_ids, date=None, level=1, expect_df=False, market="cn"):
    """获取股票对应的聚源行业

    :param order_book_ids: 股票列表，如['000001.XSHE', '000002.XSHE']
    :param date: 如 '2015-01-07' (Default value = None)
    :param level: 聚源等级，0, 1, 2, 3, 'customized' (Default value = 1)
    :param expect_df: 返回 DataFrame，默认为 False
    :param market:  (Default value = "cn")
    :returns: code, name
        返回输入日期最近交易日的股票对应聚源行业以及对应的聚源等级

    """
    if level not in (0, 1, 2, 3, "customized"):
        raise ValueError("level should in 0, 1, 2, 3, 'customized'")
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if not date:
        date = ensure_date_int(get_previous_trading_date(datetime.date.today(), market=market))
    else:
        date = ensure_date_int(date)

    df = get_client().execute("jy_instrument_industry", order_book_ids, date, level, market=market)
    if not df:
        return
    if len(order_book_ids) == 1 and not expect_df:
        r = df[0]
        if level == 0:
            return r["first_industry_name"], r["second_industry_name"], r["third_industry_name"]
        return r["industry_name"]
    return pd.DataFrame(df).set_index("order_book_id")


@export_as_api(namespace="econ")
def get_reserve_ratio(reserve_type="all", start_date=None, end_date=None, market="cn"):
    """获取存款准备金率

    :param reserve_type: 存款准备金详细类别，默认为'all'，目前仅支持'all'、'major'、'other'类别的查询
    :param start_date: 开始查找时间，如'20180501'，默认为上一年的当天
    :param end_date: 截止查找时间，如'20180501'，默认为当天
    :param market:  (Default value = "cn")
    :return: pd.DataFrame

    """
    check_items_in_container(reserve_type, ["all", "major", "other"], "reserve_type")

    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))

    ret = get_client().execute(
        "econ.get_reserve_ratio", reserve_type, start_date, end_date, market
    )
    if not ret:
        return
    columns = ["info_date", "effective_date", "reserve_type", "ratio_floor", "ratio_ceiling"]
    df = pd.DataFrame(ret, columns=columns).set_index("info_date").sort_index(ascending=True)
    return df


@export_as_api(namespace="econ")
def get_money_supply(start_date=None, end_date=None, market="cn"):
    """获取货币供应量信息

    :param start_date: 开始日期，默认为一年前
    :param end_date: 结束日期，默认为今天
    :param market:  (Default value = "cn")

    """
    check_items_in_container("info_date", ["info_date", "effective_date"], "date_type")
    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))

    data = get_client().execute("econ.get_money_supply", start_date, end_date, market=market)
    if not data:
        return
    columns = [
        "info_date",
        "effective_date",
        "m2",
        "m1",
        "m0",
        "m2_growth_yoy",
        "m1_growth_yoy",
        "m0_growth_yoy",
    ]
    df = pd.DataFrame(data, columns=columns).set_index("info_date").sort_index(ascending=True)
    return df


@export_as_api
def get_main_shareholder(
        order_book_ids=None, start_date=None, end_date=None, is_total=False, market="cn", **kwargs
):
    """获取十大股东信息

    :param order_book_ids: 股票代码
    :param start_date: 开始日期，默认为一年前
    :param end_date: 结束日期，默认为今天
    :param is_total: 是否十大股东, True 和 False，默认为False
    :param market:  (Default value = "cn")

    """
    if order_book_ids is None:
        # 兼容rqdatah，支持传入 order_book_id 字段
        order_book_ids = kwargs.pop('order_book_id', None)
        if not order_book_ids:
            raise TypeError('get_main_shareholder missing 1 required positional argument: order_book_ids')
    order_book_ids = ensure_order_book_ids(order_book_ids)
    check_items_in_container(is_total, [True, False], "is_total")
    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))

    ret = get_client().execute(
        "get_main_shareholder", order_book_ids, start_date, end_date, is_total, market=market
    )
    if not ret:
        return
    columns = ['info_date', 'end_date', 'rank', 'shareholder_name', 'shareholder_attr', 'shareholder_kind',
               'shareholder_type', 'hold_percent_total', 'hold_percent_float', 'share_pledge', 'share_freeze',
               'order_book_id']
    df = pd.DataFrame(ret, columns=columns).sort_values(by=['info_date', 'rank']).\
        set_index(['order_book_id', 'info_date'])
    return df


@export_as_api
def get_current_news(n=None, start_time=None, end_time=None, channels=None):
    """获取新闻
    :param n: 新闻条数, n 和 start_time/end_time 只能指定其一
    :param start_time: 开始日期，默认为None,格式为%Y-%m-%d %H:%M:%S，如"2018-10-20 09:10:20"
    :param end_time: 结束日期，默认为None,格式为%Y-%m-%d %H:%M:%S，如"2018-10-20 19:10:20"
    :param channels: 新闻大类, 默认为None,返回每个大类n条新闻, 如 "global"，"forex", "commodity", "a-stock"
    """

    if start_time is not None or end_time is not None:
        try:
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise ValueError('start_time should be str format like "%Y-%m-%d %H:%M:%S"')
        try:
            end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise ValueError('end_time should be str format like "%Y-%m-%d %H:%M:%S"')
        start_time = datetime_to_int14(start_time)
        end_time = datetime_to_int14(end_time)
        if n is not None:
            raise ValueError(
                "please either specify parameter n, or specify both start_time and end_time"
            )
        n = 1200
    elif n is None:
        n = 1
    else:
        n = ensure_int(n, "n")
        if n < 1 or n > 1200:
            raise ValueError("n should be in [0, 1200]")

    if channels is not None:
        channels = ensure_list_of_string(channels)
        check_items_in_container(channels, ["global", "forex", "a-stock", "commodity"], "channels")
    else:
        channels = ["global", "forex", "a-stock", "commodity"]

    data = get_client().execute("get_current_news", n, start_time, end_time, channels)
    if not data:
        return
    df = pd.DataFrame(data, columns=["channel", "datetime", "content"])
    return df.set_index("channel")


@export_as_api(namespace="econ")
def get_factors(factors, start_date, end_date, market="cn"):
    start_date, end_date = ensure_date_range(start_date, end_date)
    factors = ensure_list_of_string(factors, "factors")
    data = get_client().execute("econ.get_factors", factors, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df = df.reindex(columns=["factor", "info_date", "start_date", "end_date", "value"])
    df.set_index(["factor", "info_date"], inplace=True)
    return df


@export_as_api
def get_trading_hours(order_book_id, date=None, expected_fmt="str", frequency="1m", market="cn"):
    """获取合约指定日期交易时间
      :param order_book_id: 合约代码
      :param date: 日期，默认为今天
      :param expected_fmt: 返回格式，默认为str, 也支持datetime.time和datetime.datetime格式
      :param frequency: 频率，默认为1m, 对应米筐分钟线时间段的起始, tick和1m相比区别在于每个交易时间段开盘往前移一分钟
      :param market:  (Default value = "cn")

      :return: trading_hours str or list of datetime.time/datetime.datetime list or None
      """
    date = ensure_date_or_today_int(date)
    if not is_trading_date(date, market):
        warnings.warn(" %d is not a trading date" % date)
        return

    ensure_string(order_book_id, "order_book_id")
    ins = instruments(order_book_id)
    if ins is None:
        return

    ensure_string_in(expected_fmt, ("str", "time", "datetime"), "expected_fmt")
    ensure_string_in(frequency, ("1m", "tick"), "frequency")
    date_str = to_date_str(date)

    if ins.listed_date > date_str:
        return

    if ins.type in ("Future", "Option") and ins.de_listed_date < date_str and ins.de_listed_date != "0000-00-00":
        return

    if ins.type not in ("Future", "Option") and ins.de_listed_date <= date_str and ins.de_listed_date != "0000-00-00":
        return
    if ins.type == "Repo":
        trading_hours = "09:31-11:30,13:01-15:30"
    elif ins.type == "Spot":
        if has_night_trading(date, market):
            trading_hours = "20:01-02:30,09:01-15:30"
        else:
            trading_hours = "09:01-15:30"
    elif ins.type not in ("Future", "Option") or (ins.type == "Option" and ins.exchange in ("XSHG", "XSHE")):
        trading_hours = "09:31-11:30,13:01-15:00"
    else:
        trading_hours = get_client().execute("get_trading_hours", ins.underlying_symbol, date, market=market)
        if trading_hours is None:
            return
        # 前一天放假或者该品种上市首日没有夜盘
        no_night_trading = (not has_night_trading(date, market) or
                            get_underlying_listed_date(ins.underlying_symbol, ins.type) == date_str)

        if no_night_trading and not trading_hours.startswith("09"):
            trading_hours = trading_hours.split(",", 1)[-1]

    if frequency == "tick":
        trading_hours = ",".join([s[:4] + str(int(s[4]) - 1) + s[5:] for s in trading_hours.split(",")])

    if expected_fmt != "str":
        trading_hours = [t.split("-", 1) for t in trading_hours.split(",")]
        for i, (start, end) in enumerate(trading_hours):
            trading_hours[i][0] = str_to_dt_time(start)
            trading_hours[i][1] = str_to_dt_time(end)

        if expected_fmt == "datetime":
            td = int8_to_date(date)
            prev_td = get_previous_trading_date(date)
            prev_td_next = prev_td + datetime.timedelta(days=1)

            for i, (start, end) in enumerate(trading_hours):
                if start.hour > 16:
                    start_dt = prev_td
                    end_dt = start_dt if end.hour > 16 else prev_td_next
                else:
                    start_dt = end_dt = td
                trading_hours[i][0] = datetime.datetime.combine(start_dt, start)
                trading_hours[i][1] = datetime.datetime.combine(end_dt, end)

    return trading_hours


@export_as_api
def get_private_placement(order_book_ids, start_date=None, end_date=None, progress="complete", issue_type="private", market="cn"):
    """获取定增数据
    :param order_book_ids: 合约代码
    :param start_date: 开始日期，默认为None
    :param end_date: 结束日期，默认为None
    :param progress: 是否已完成定增，默认为complete。可选参数["complete", "incomplete", "all"]
    :param issue_type: 默认为all。可选参数["private", "public", "all"]
    :param market: (Default value = "cn")
    :return:
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date and end_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
    elif start_date:
        start_date = ensure_date_int(start_date)
    elif end_date:
        end_date = ensure_date_int(end_date)
    ensure_string_in(progress, ["complete", "incomplete", "all"], "progress")
    ensure_string_in(issue_type, ["private", "public", "all"], "issue_type")
    issue_type_change = {"private": (21, 23), "public": (22,), "all": (21, 22, 23)}
    issue_type = issue_type_change[issue_type]
    data = get_client().execute(
        "get_private_placement", order_book_ids, start_date, end_date, progress, issue_type=issue_type, market=market
    )
    if not data:
        return
    progress_map = {
        10: "董事会预案", 20: "股东大会通过", 21: "国资委通过", 22: "发审委通过", 23: "证监会通过",
        29: "实施中", 30: "实施完成", 40: "国资委否决", 41: "股东大会否决", 42: "证监会否决",
        43: "发审委否决", 50: "延期实施", 60: "停止实施", 70: "暂缓发行"}
    issue_type_map = {21: "非公开发行", 22: "公开发行", 23: "非公开发行配套融资"}
    df = pd.DataFrame(data)
    df["progress"] = df["progress"].map(progress_map)
    df["issue_type"] = df["issue_type"].map(issue_type_map)
    df.set_index(["order_book_id", "initial_info_date"], inplace=True)
    return df


@export_as_api
def get_share_transformation(predecessor=None, market="cn"):
    """
    获取转股信息
    :param predecessor: 换股前的股票代码。默认为空，返回所有转股信息
    :param market:  (Default value = "cn")
    :return pd.DataFrame
    """
    if predecessor:
        predecessor = ensure_order_book_id(predecessor)
    data = get_client().execute("get_share_transformation", predecessor, market=market)
    if not data:
        return
    columns = [
        "predecessor", "successor", "effective_date", "share_conversion_ratio", "predecessor_delisted",
        "discretionary_execution", "predecessor_delisted_date", "event"
    ]
    df = pd.DataFrame(data, columns=columns)
    df = df.sort_values('predecessor').reset_index(drop=True)
    return df


@export_as_api(namespace="user")
def get_quota():
    """
    获取用户配额信息
    :return dict
        bytes_limit：每日流量使用上限（单位为字节），如为0则表示不受限
        bytes_used：当日已用流量（单位为字节）
        remaining_days：账号剩余有效天数, 如为0则表示不受限
        license_type：账户类型(FULL: 付费类型，TRIAL: 试用类型， EDU: 教育网类型, OTHER: 其他类型)
    """
    data = get_client().execute("user.get_quota")
    if data['bytes_limit'] > 0 and data["bytes_used"] >= data["bytes_limit"]:
        warnings.warn("Traffic usage has been exceeded quota,"
                      "Please call us at 0755-22676337 to upgrade"
                      "your contract.")
    return data


_CHECK_CATEGORIES = ("stock_1d", "stock_1m", "future_1d", "future_1m", "index_1d", "index_1m")


@export_as_api()
def get_update_status(categories):
    """
    获取数据最新日期
    :param categories: str or list or str, 数据类型，支持类型有:
        stock_1d: 股票日线
        stock_1m: 股票分钟线
        future_1d: 期货日线
        future_1m: 期货分钟线
        index_1d：指数日线
        index_1m：指数分钟线

    :return datetime.datetime or dict(category=datetime.datetime)
    """
    categories = ensure_list_of_string(categories, "categories")
    check_items_in_container(categories, _CHECK_CATEGORIES, "categories")
    ret = get_client().execute("get_update_status", categories)
    if len(categories) == 1:
        return ret[0]["date"]
    return {r["category"]: r["date"] for r in ret}


@export_as_api()
def info():
    """
    打印账户信息
    :return None
    """
    get_client().info()


@export_as_api()
def get_basic_info(order_book_ids=None, fields=("order_book_id", "symbol"), market='cn'):
    if order_book_ids is not None:
        order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    if fields is not None:
        fields = ensure_list_of_string(fields, "fields")

    ret = get_client().execute("get_basic_info", order_book_ids, fields, market=market)
    if not ret:
        return
    columns, data = ret
    return pd.DataFrame(data, columns=columns)
