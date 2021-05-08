# -*- coding: utf-8 -*-
import datetime

import pandas as pd
import numpy as np

from rqdatac.services.calendar import get_previous_trading_date
from rqdatac.validators import (
    ensure_string,
    ensure_string_in,
    ensure_order_book_ids,
    ensure_date_range
)
from rqdatac.utils import (
    int8_to_datetime_v,
    int14_to_datetime_v,
    int17_to_datetime_v,
    int17_to_datetime,
    today_int,
    date_to_int8,
    get_tick_value,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api
from rqdatac.share.errors import PermissionDenied, MarketNotSupportError


DAYBAR_FIELDS = MINBAR_FIELDS = ["buy_volume", "buy_value", "sell_volume", "sell_value"]
TICKBAR_FIELDS = ["datetime", "direction", "volume", "value"]


def convert_bar_to_multi_df(data, dt_name, fields, convert_dt, default=0):
    line_no = 0
    dt_set = set()
    obid_level = []
    obid_slice_map = {}
    for obid, d in data:
        dts = d[dt_name]
        dts_len = len(dts)
        if dts_len == 0:
            continue
        obid_slice_map[obid] = slice(line_no, line_no + dts_len, None)
        dt_set.update(dts)
        line_no += dts_len

        obid_level.append(obid)

    if line_no == 0:
        return

    obid_idx_map = {o: i for i, o in enumerate(obid_level)}
    obid_label = np.empty(line_no, dtype=object)
    dt_label = np.empty(line_no, dtype=object)
    arr = np.full((line_no, len(fields)), default)
    r_map_fields = {f: i for i, f in enumerate(fields)}

    dt_arr_sorted = np.array(sorted(dt_set))
    dt_level = convert_dt(dt_arr_sorted)

    for obid, d in data:
        dts = d[dt_name]
        if len(dts) == 0:
            continue
        slice_ = obid_slice_map[obid]
        for f, value in d.items():
            if f == dt_name:
                dt_label[slice_] = dt_arr_sorted.searchsorted(dts, side='left')
            else:
                arr[slice_, r_map_fields[f]] = value
        obid_label[slice_] = [obid_idx_map[obid]] * len(dts)
    try:
        # func 'is_datetime_with_singletz_array'  is the most time consuming part in multi_index constructing
        # it is useless for our situation. skip it.
        func_is_singletz = getattr(pd._libs.lib, 'is_datetime_with_singletz_array')
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', lambda *args: True)
    except AttributeError:
        func_is_singletz = None

    multi_idx = pd.MultiIndex([obid_level, dt_level], [obid_label, dt_label],
                              names=('order_book_id', dt_name))

    if func_is_singletz is not None:
        # recovery
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', func_is_singletz)

    df = pd.DataFrame(data=arr, index=multi_idx, columns=fields)
    return df


def get_capital_flow_daybar(order_book_ids, start_date, end_date, fields, duration=1, market="cn"):
    data = get_client().execute(
        "get_capital_flow_daybar", order_book_ids, start_date, end_date, fields, duration, market=market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    res = convert_bar_to_multi_df(data, 'date', fields, int8_to_datetime_v)
    return res


def get_today_capital_flow_minbar(order_book_ids, date, fields, duration, market="cn"):
    data = get_client().execute("get_today_capital_flow_minbar", order_book_ids, date, fields, duration, market=market)
    return convert_bar_to_multi_df(data, "datetime", fields, int14_to_datetime_v)


def get_capital_flow_minbar(order_book_ids, start_date, end_date, fields, duration, market):
    history_permission_denied = realtime_permission_denied = False
    try:
        data = get_client().execute(
            "get_capital_flow_minbar", order_book_ids, start_date, end_date, fields, duration, market=market
        )
    except PermissionDenied:
        history_permission_denied = True
        data = []

    if data:
        data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
        df = convert_bar_to_multi_df(data, 'datetime', fields, int14_to_datetime_v)
    else:
        df = None

    today = today_int()
    if df is None:
        history_latest_date = date_to_int8(get_previous_trading_date(today, market=market))
    else:
        history_latest_date = date_to_int8(df.index.get_level_values(1).max())

    if history_latest_date >= end_date or start_date > today or history_latest_date >= today:
        return df
    try:
        live_df = get_today_capital_flow_minbar(order_book_ids, today, fields, duration, market)
    except PermissionDenied:
        live_df = None
        realtime_permission_denied = True
    except MarketNotSupportError:
        live_df = None

    if history_permission_denied and realtime_permission_denied:
        raise PermissionDenied("get_capital_flow_minbar")

    if live_df is None:
        return df
    if df is None:
        return live_df
    df = pd.concat([df, live_df])
    df.sort_index(inplace=True)
    return df


def get_today_capital_flow_tick(order_book_id, date, market="cn"):
    data = get_client().execute("get_today_capital_flow_tick", order_book_id, date, market=market)
    df = pd.DataFrame(data[0])
    if df.empty:
        return None
    del df["order_book_id"]
    df.datetime = df.datetime.apply(int17_to_datetime)
    df = df.astype({"direction": "i1", "volume": "u8", "value": "u8"})
    df.set_index("datetime", inplace=True)
    return df


def get_capital_flow_tickbar(order_book_id, start_date, end_date, fields,  market):
    ensure_string(order_book_id, "order_book_id")
    start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=3))
    history_permission_denied = realtime_permission_denied = False
    try:
        data = get_client().execute(
            "get_capital_flow_tickbar", order_book_id, start_date, end_date, fields, market=market
        )
    except PermissionDenied:
        data = []
        history_permission_denied = True
    today = today_int()

    if data:
        data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
        df_list = []
        for obid, d in data:
            df = pd.DataFrame(d)
            df_list.append(df)

        df = pd.concat(df_list)  # type: pd.DataFrame
        df["datetime"] = int17_to_datetime_v(df["datetime"].values)
        history_latest_date = date_to_int8(df.iloc[-1]["datetime"])
        df.set_index("datetime", inplace=True)
    else:
        df = None
        history_latest_date = date_to_int8(get_previous_trading_date(today, market=market))

    if history_latest_date >= end_date or start_date > today or history_latest_date >= today:
        return df

    try:
        live_df = get_today_capital_flow_tick(order_book_id, today, market=market)
    except PermissionDenied:
        live_df = None
        realtime_permission_denied = True
    except MarketNotSupportError:
        live_df = None

    if history_permission_denied and realtime_permission_denied:
        raise PermissionDenied("get_capital_flow_tick")

    if live_df is None:
        return df
    if df is None:
        return live_df
    return pd.concat([df, live_df])


@export_as_api
def get_capital_flow(order_book_ids, start_date=None, end_date=None, frequency="1d", market="cn"):
    """获取资金流入流出数据
    :param order_book_ids: 股票代码or股票代码列表, 如'000001.XSHE'
    :param start_date: 开始日期
    :param end_date: 结束日期
    :param frequency: 默认为日线。日线使用 '1d', 分钟线 '1m'  快照 'tick' (Default value = "1d"),
    :param market:  (Default value = "cn")
    :returns: pandas.DataFrame or None
    """
    ensure_string_in(frequency, ("1d", "1m", "tick"), "frequency")
    if frequency == "tick":
        return get_capital_flow_tickbar(order_book_ids, start_date, end_date, TICKBAR_FIELDS, market)

    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if frequency == "1d":
        return get_capital_flow_daybar(order_book_ids, start_date, end_date, DAYBAR_FIELDS, 1, market)

    return get_capital_flow_minbar(order_book_ids, start_date, end_date, MINBAR_FIELDS, 1, market)


def _open_auction_filed_type(field_name):
    return (np.object_ if field_name == "order_book_id"
            else np.uint64 if field_name == "datetime"
            else np.float)


OA_FIELDS = [
    "open",
    "last",
    "high",
    "low",
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


@export_as_api
def get_open_auction_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取盘前集合竞价数据
    :param order_book_ids: 股票代码
    :param start_date: 起始日期，默认为今天
    :param end_date: 截止日期，默认为今天
    :param market:  (Default value = "cn")
    :returns: pd.DataFrame or None
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=0))

    history_permission_denied = realtime_permission_denied = False
    try:
        # obid add prefix 'OA_'
        data = get_client().execute("get_open_auction_info_daybar", ["OA_" + obid for obid in order_book_ids],
                                    start_date, end_date, OA_FIELDS + ["datetime", "date"], market=market)
    except PermissionDenied:
        data = []
        history_permission_denied = True

    today = today_int()
    prev_trading_date = date_to_int8(get_previous_trading_date(today, market=market))
    if data:
        data = [(obid[3:], {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
        df = convert_bar_to_multi_df(data, 'datetime', OA_FIELDS + ["date"], int17_to_datetime_v, default=0.0)
        if df is None:
            history_latest_date = prev_trading_date
        else:
            history_latest_date = df["date"].max()
            del df["date"]
    else:
        df = None
        history_latest_date = prev_trading_date
    if history_latest_date >= end_date or start_date > today or history_latest_date >= today or end_date < today:
        return df

    try:
        live_df = get_today_open_auction(order_book_ids, today, market=market)
    except PermissionDenied:
        live_df = None
        realtime_permission_denied = True
    except MarketNotSupportError:
        live_df = None

    if history_permission_denied and realtime_permission_denied:
        raise PermissionDenied("get_open_auction_info")

    if live_df is None:
        return df
    if df is None:
        return live_df
    df = pd.concat([df, live_df])
    df.sort_index(inplace=True)
    return df


def get_today_open_auction(order_book_ids, today,  market="cn"):
    ticks = get_client().execute("get_today_open_auction", order_book_ids, today, market=market)
    if not ticks:
        return

    fields = ["order_book_id", "datetime"] + OA_FIELDS
    dtype = np.dtype([(f, _open_auction_filed_type(f)) for f in fields])
    bars = np.array([tuple([get_tick_value(t, f) for f in fields]) for t in ticks], dtype=dtype)

    df = pd.DataFrame(bars)
    df.datetime = df.datetime.apply(int17_to_datetime)
    df.set_index(["order_book_id", "datetime"], inplace=True)
    return df

