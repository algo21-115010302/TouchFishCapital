# -*- coding: utf-8 -*-
import datetime
from itertools import chain

import pandas as pd
import numpy as np

from rqdatac.services.basic import instruments
from rqdatac.services.future import get_dominant_future
from rqdatac.services.calendar import get_trading_dates_in_type, get_previous_trading_date, is_trading_date, get_next_trading_date
from rqdatac.utils import (to_datetime, int8_to_date, int9_to_time, int14_to_datetime,
                           get_tick_value, datetime_to_int17, relativedelta, int17_to_datetime)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, ttl_cache
from rqdatac.validators import (ensure_instruments_in_order,
                                check_items_in_container, ensure_list_of_string)
from rqdatac.services.stock_status import is_suspended


@ttl_cache(3600)
def _to_real_contract(ob, market):
    today = datetime.date.today()
    r = get_dominant_future(ob, today, today, market=market)
    if isinstance(r, pd.Series) and r.size == 1:
        return r[0]
    return None


@export_as_api
def current_snapshot(order_book_ids, market="cn"):
    """获取实时行情

    :param order_book_ids: 股票或期货代码或代码列表
    :param market: 地区代码, 如'cn' (Default value = "cn")
    :returns: Snapshot Object或Snapshot Object列表

    """
    _instruments = ensure_instruments_in_order(order_book_ids)
    order_book_ids = []
    for ins in _instruments:
        if ins.type == "Future" and ins.order_book_id.endswith(("88", "89")):
            real_contract = _to_real_contract(ins.underlying_symbol, market) or ins.order_book_id
            order_book_ids.append(real_contract)
        else:
            order_book_ids.append(ins.order_book_id)

    snapshots = get_client().execute("current_snapshots", order_book_ids, market=market)
    tick_objects = [TickObject(ins.order_book_id, snapshots[i]) for i, ins in enumerate(_instruments)]
    if len(order_book_ids) == 1:
        return tick_objects[0]
    return tick_objects


class TickObject(object):
    _STOCK_FIELDS = [
        ("datetime", np.uint64),
        ("open", np.float64),
        ("high", np.float64),
        ("low", np.float64),
        ("last", np.float64),
        ('limit_up', np.float64),
        ('limit_down', np.float64),
        ("iopv", np.float64),
        ("pre_iopv", np.float64),
        ("volume", np.int32),
        ("total_turnover", np.int64),
        ("prev_close", np.float64),
        ("ask", list),
        ("ask_vol", list),
        ("bid", list),
        ("bid_vol", list),
        ("trading_phase_code", str),
    ]

    _FUTURE_FIELDS = _STOCK_FIELDS + [("open_interest", np.int32), ("prev_settlement", np.float64)]

    _STOCK_FIELD_NAMES = [_n for _n, _ in _STOCK_FIELDS]
    _FUTURE_FIELD_NAMES = [_n for _n, _ in _FUTURE_FIELDS]

    _NANDict = {_n: np.nan for _n in _FUTURE_FIELD_NAMES}

    def __init__(self, order_book_id, data, dt=None):
        self._dt = dt
        if data is None:
            self._data = self._NANDict
        else:
            self._data = data
        self._order_book_id = order_book_id

    @property
    def order_book_id(self):
        return self._order_book_id

    @property
    def open(self):
        return self._data.get("open", 0)

    @property
    def last(self):
        return self._data.get("last", 0)

    @property
    def low(self):
        return self._data.get("low", 0)

    @property
    def high(self):
        return self._data.get("high", 0)

    @property
    def limit_up(self):
        return self._data.get('limit_up', 0)

    @property
    def limit_down(self):
        return self._data.get('limit_down', 0)

    @property
    def num_trades(self):
        return self._data.get('num_trades', 0)

    @property
    def prev_close(self):
        return self._data.get("prev_close", 0)

    @property
    def iopv(self):
        return self._data.get("iopv", np.nan)

    @property
    def prev_iopv(self):
        return self._data.get("prev_iopv", np.nan)

    @property
    def volume(self):
        return self._data.get("volume", 0)

    @property
    def total_turnover(self):
        return self._data.get("total_turnover", 0)

    @property
    def datetime(self):
        if self._dt is not None:
            return self._dt
        if not self._isnan:
            dt = self._data["datetime"]
            return to_datetime(dt)
        return datetime.datetime.min

    @property
    def prev_settlement(self):
        try:
            return self._data["prev_settlement"]
        except KeyError:
            return None

    @property
    def open_interest(self):
        try:
            return self._data["open_interest"]
        except KeyError:
            return None

    @property
    def trading_phase_code(self):
        try:
            return self._data["trading_phase_code"]
        except KeyError:
            return None

    @property
    def asks(self):
        return self._data.get("ask", [0, 0, 0, 0, 0])

    @property
    def ask_vols(self):
        return self._data.get("ask_vol", [0, 0, 0, 0, 0])

    @property
    def bids(self):
        return self._data.get("bid", [0, 0, 0, 0, 0])

    @property
    def bid_vols(self):
        return self._data.get("bid_vol", [0, 0, 0, 0, 0])

    @property
    def _isnan(self):
        return np.isnan(self._data["last"])

    def __repr__(self):
        items = []
        for name in dir(self):
            if name.startswith("_"):
                continue
            items.append((name, getattr(self, name)))
        return "Tick({0})".format(
            ", ".join("{0}: {1}".format(k, v) for k, v in items if k is not None)
        )

    def __getitem__(self, key):
        return getattr(self, key)


EQUITIES_FIELDS = (
    "datetime",
    "open",
    "last",
    "high",
    "low",
    "iopv",
    "prev_iopv",
    "limit_up",
    "limit_down",
    "prev_close",
    "volume",
    "total_turnover",
    "a1",
    "a2",
    "a3",
    "a4",
    "a5",
    "b1",
    "b2",
    "b3",
    "b4",
    "b5",
    "a1_v",
    "a2_v",
    "a3_v",
    "a4_v",
    "a5_v",
    "b1_v",
    "b2_v",
    "b3_v",
    "b4_v",
    "b5_v",
)

FUTURE_FIELDS = (
    "datetime",
    "trading_date",
    "update_time",
    "open",
    "last",
    "high",
    "low",
    "limit_up",
    "limit_down",
    "prev_settlement",
    "prev_close",
    "volume",
    "total_turnover",
    "open_interest",
    "a1",
    "a2",
    "a3",
    "a4",
    "a5",
    "b1",
    "b2",
    "b3",
    "b4",
    "b5",
    "a1_v",
    "a2_v",
    "a3_v",
    "a4_v",
    "a5_v",
    "b1_v",
    "b2_v",
    "b3_v",
    "b4_v",
    "b5_v",
)


@export_as_api
def get_ticks(order_book_id, start_date=None, end_date=None, expect_df=False, market="cn"):
    """获取实时tick

    :param order_book_id: 股票代码, 如'000001.XSHE'
    :param start_date: 开始日期, 不指定则为今天
    :param end_date: 结束日期, 不指定则为今天
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :param market:  (Default value = "cn")
    :returns: pd.DataFrame or None

    """
    instrument = instruments(order_book_id, market)
    if not instrument:
        raise ValueError("invalid order_book_id: {}".format(order_book_id))

    order_book_id = instrument.order_book_id
    future_like = instrument.type in ("Future", "Option", "Spot")

    now = datetime.datetime.now()
    if is_trading_date(now):
        if (now.hour, now.minute) >= (19, 30):
            day = get_next_trading_date(now)
        else:
            day = now.date()
    else:
        day = get_next_trading_date(now)

    dates = get_trading_dates_in_type(
        start_date or day,
        end_date or day,
        expect_type="str",
        fmt="%Y%m%d",
        market=market,
    )
    ticks = get_client().execute("get_ticks_v2", order_book_id, dates, market=market)
    if future_like:
        fields = FUTURE_FIELDS
    else:
        fields = EQUITIES_FIELDS

    if future_like and instrument.exchange in ("XSHE", "XSHG"):
        fields = list(fields)
        fields.remove("trading_date")

    dtype = np.dtype([(f, _field_type(f)) for f in fields])
    bars = np.array([tuple([get_tick_value(t, f, np.nan) if f in ("iopv", "prev_iopv") else get_tick_value(t, f)
                            for f in fields]) for t in chain(*ticks)], dtype=dtype)
    df = pd.DataFrame(bars)

    if df.empty:
        return None
    if "trading_date" in df.columns:
        df.trading_date = df.trading_date.apply(int8_to_date)
        df.update_time = df.update_time.apply(int9_to_time)
    df.datetime = df.datetime.apply(to_datetime)
    if expect_df:
        df["order_book_id"] = order_book_id
        df.set_index(["order_book_id", "datetime"], inplace=True)
    else:
        df.set_index("datetime", inplace=True)
    return df


def _field_type(field_name):
    return np.uint64 if field_name in ("datetime", "trading_date", "update_time") else np.float


@export_as_api
def current_minute(order_book_ids, skip_suspended=False, fields=None, market="cn"):
    """获取实时分钟行情
    :param order_book_ids: 合约代码或代码列表
    :param skip_suspended: 是否过滤停复牌. (Default value = False)
    :param fields: 可选参数。默认为所有字段。 (Default value = None)
    :param market: 地区代码, 如'cn' (Default value = "cn")
    :returns: None or pd.DataFrame
    """
    from rqdatac.services.get_price import classify_order_book_ids, _ensure_fields
    from rqdatac.services.detail.get_price_df import MINBAR_FIELDS

    (order_book_ids, stocks, funds, indexes, futures, futures888, spots, options,
        convertibles, repos) = classify_order_book_ids(order_book_ids, market)
    if skip_suspended and stocks:
        date = get_previous_trading_date(datetime.date.today() + datetime.timedelta(days=1))
        df_suspended = is_suspended(stocks, date, date)
        if not df_suspended.empty:
            df_suspended_t = df_suspended.T
            suspended_obids = set(df_suspended_t[df_suspended_t[df_suspended_t.columns[0]]].index)
            inspection = suspended_obids & set(stocks)
            if inspection:
                stocks = set(stocks) - inspection
                order_book_ids = list(set(order_book_ids) - inspection)

    fields, _ = _ensure_fields(fields, MINBAR_FIELDS, stocks, funds, futures, futures888, spots, options, convertibles, indexes, repos)

    data = get_client().execute("current_minute", order_book_ids, fields + ["datetime"], market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df["datetime"] = df["datetime"].map(int14_to_datetime, na_action="ignore")
    df.set_index(["order_book_id", "datetime"], inplace=True)
    return df


LIVE_EQUITIES_FIELDS = [
    "open",
    "last",
    "high",
    "low",
    "iopv",
    "prev_iopv",
    "limit_up",
    "limit_down",
    "prev_close",
    "volume",
    "total_turnover",
    "a1",
    "a2",
    "a3",
    "a4",
    "a5",
    "b1",
    "b2",
    "b3",
    "b4",
    "b5",
    "a1_v",
    "a2_v",
    "a3_v",
    "a4_v",
    "a5_v",
    "b1_v",
    "b2_v",
    "b3_v",
    "b4_v",
    "b5_v",
]

LIVE_FUTURE_FIELDS = [
    "trading_date",
    "update_time",
    "open",
    "last",
    "high",
    "low",
    "limit_up",
    "limit_down",
    "prev_settlement",
    "prev_close",
    "volume",
    "total_turnover",
    "open_interest",
    "a1",
    "a2",
    "a3",
    "a4",
    "a5",
    "b1",
    "b2",
    "b3",
    "b4",
    "b5",
    "a1_v",
    "a2_v",
    "a3_v",
    "a4_v",
    "a5_v",
    "b1_v",
    "b2_v",
    "b3_v",
    "b4_v",
    "b5_v",
]


@export_as_api
def get_live_ticks(order_book_ids, start_dt=None, end_dt=None, fields=None, market="cn"):
    """获取实时tick
    :param order_book_ids: 股票代码或代码列表, 如'000001.XSHE'
    :param start_dt: 开始时间，如20200102213000 或 "2020-01-02 21:30:00"， 默认为今天
    :param end_dt: 结束时间，如20200102153000 或 "2020-01-03 15:30:00"， 默认为今天
    :param fields: 可选参数。默认为所有字段。 (Default value = None)
    :param market:  (Default value = "cn")

    :returns: pd.DataFrame or None
    """

    if start_dt is None and end_dt is None:
        now = datetime.datetime.now()
        if is_trading_date(now):
            if (now.hour, now.minute) >= (19, 30):
                day = get_next_trading_date(now)
            else:
                day = now.date()
        else:
            day = get_next_trading_date(now)

        dates = get_trading_dates_in_type(
            day, day,
            expect_type="str",
            fmt="%Y%m%d",
            market=market,
        )
        start_dt, end_dt = 0, 29991231240000000
    elif start_dt and end_dt:
        start_datetime = to_datetime(start_dt)
        end_datetime = to_datetime(end_dt)
        start_dt = datetime_to_int17(start_datetime)
        end_dt = datetime_to_int17(end_datetime)

        dates = get_trading_dates_in_type(
            start_datetime + relativedelta(days=1 if start_datetime.hour > 18 else 0),
            end_datetime + relativedelta(days=1 if start_datetime.hour > 18 else 0),
            expect_type="str",
            fmt="%Y%m%d",
            market=market,
        )
    else:
        raise ValueError("please specify start_dt/end_dt in the same time")
    order_book_ids = ensure_list_of_string(order_book_ids)
    ins = instruments(order_book_ids, market)
    if not ins:
        raise ValueError("invalid order_book_id: {}".format(order_book_ids))
    obids = [i.order_book_id for i in ins]
    ins_types = {i.type for i in ins}

    future_like = ins_types & {"Future", "Option", "Spot"}
    equities_like = ins_types - {"Future", "Option", "Spot"}
    if future_like and equities_like:
        base_fields = LIVE_EQUITIES_FIELDS + list(set(LIVE_FUTURE_FIELDS) - set(LIVE_EQUITIES_FIELDS))
    elif future_like:
        base_fields = LIVE_FUTURE_FIELDS
    else:
        base_fields = LIVE_EQUITIES_FIELDS

    if fields is not None:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, base_fields, "fields")
    else:
        fields = base_fields
    fields = ["order_book_id", "datetime"] + fields
    ret = get_client().execute("get_live_ticks", obids, start_dt, end_dt, dates, fields, market=market)
    if not ret:
        return None
    header, ticks = ret
    df = pd.DataFrame(ticks, columns=header)
    df = df[fields]
    if "trading_date" in df.columns:
        df.trading_date = df.trading_date.apply(lambda x: pd.NaT if x == 0 else int8_to_date(x))
    if "update_time" in df.columns:
        df.update_time = df.update_time.apply(int9_to_time)
    df["datetime"] = df["datetime"].apply(int17_to_datetime)
    df.set_index(["order_book_id", "datetime"], inplace=True)
    return df
