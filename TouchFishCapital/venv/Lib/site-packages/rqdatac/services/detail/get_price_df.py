# -*- coding: utf-8 -*-
import warnings
import numpy as np
import pandas as pd
from rqdatac.services.detail.resample_helper import resample_week_df


from rqdatac.services.get_price import (
    _ensure_fields,
    get_current_trading_date
)
from rqdatac.utils import (
    int14_to_datetime_v,
    int8_to_datetime_v,
    today_int,
    date_to_int8,
)
from rqdatac.client import get_client
from rqdatac.services.future import get_dominant_future
from rqdatac.services.stock_status import is_suspended
from rqdatac.share.errors import PermissionDenied, MarketNotSupportError

DAYBAR_FIELDS = {
    "future": ["settlement", "prev_settlement", "open_interest", "limit_up", "limit_down"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": ["limit_up", "limit_down", "num_trades"],
    "fund": ["limit_up", "limit_down", "num_trades", "iopv"],
    "spot": ["settlement", "prev_settlement", "open_interest", "limit_up", "limit_down"],
    "option": ["open_interest", "strike_price", "contract_multiplier", "prev_settlement", "settlement", "limit_up",
               "limit_down"],
    "convertible": ["num_trades"],
    "index": ["num_trades"],
    "repo": ["num_trades"],
}

WEEKBAR_FIELDS = {
    "future": ["settlement", "prev_settlement", "open_interest"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": ["num_trades"],
    "fund": ["num_trades", "iopv"],
    "spot": ["settlement", "prev_settlement", "open_interest"],
    "option": ["open_interest", "strike_price", "contract_multiplier", "settlement"],
    "convertible": ["num_trades"],
    "index": ["num_trades"],
    "repo": ["num_trades"],
}

MINBAR_FIELDS = {
    "future": ["trading_date", "open_interest"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": [],
    "fund": ["iopv"],
    "spot": ["trading_date", "open_interest"],
    "option": ["trading_date", "open_interest"],
    "convertible": [],
    "index": [],
    "repo": [],
}


ZERO_FILL_FIELDS = frozenset({"total_turnover", "open_interest", "volume"})

SPOT_DIRECTION_MAP = {0: "null", 1: "多支付空", 2: "空支付多", 3: "交收平衡"}


def get_price_df(
        order_book_ids,
        start_date,
        end_date,
        frequency,
        duration,
        fields,
        adjust_type,
        skip_suspended,
        stocks,
        funds,
        indexes,
        futures,
        futures888,
        spots,
        options,
        convertibles,
        repos,
        market
):
    if frequency == "d":
        fields, has_dominant_id = _ensure_fields(fields, DAYBAR_FIELDS, stocks, funds, futures, futures888, spots, options, convertibles, indexes, repos)
        pf, obid_slice_map = get_daybar(order_book_ids, start_date, end_date, fields, duration, market)
        if pf is None:
            return
    else:
        fields, has_dominant_id = _ensure_fields(fields, MINBAR_FIELDS, stocks, funds, futures, futures888, spots, options, convertibles, indexes, repos)
        history_permission_denied, today_permission_denied = False, False
        try:
            pf, obid_slice_map = get_minbar(order_book_ids, start_date, end_date, fields, duration, market)
        except (PermissionDenied, MarketNotSupportError):
            pf = obid_slice_map = None
            history_permission_denied = True
        history_latest_day = 0 if pf is None else date_to_int8(pf.index.get_level_values(1).max())
        if history_latest_day < end_date and end_date >= today_int():
            try:
                today_pf, today_obid_slice_map = get_today_minbar(order_book_ids, fields, duration, market)
            except (PermissionDenied, MarketNotSupportError):
                today_pf = None
                today_permission_denied = True
            if today_pf is None:
                today_pf_latest_day = 0
            else:
                today_pf_latest_day = date_to_int8(get_current_trading_date(today_pf.index.get_level_values(1).max()))
            if today_pf_latest_day > history_latest_day and today_pf_latest_day >= start_date:
                if history_latest_day == 0:
                    pf = today_pf
                    obid_slice_map = today_obid_slice_map
                else:
                    pf = pd.concat([pf, today_pf])
                    line_no, obid_slice_map = 0, {}
                    obids, counts = np.unique(pf.index.get_level_values(0), return_counts=True)
                    for obid, ct in zip(obids, counts):
                        obid_slice_map[obid] = slice(line_no, line_no + ct, None)
                        line_no += ct
        if pf is None:
            if history_permission_denied and today_permission_denied:
                raise PermissionDenied("Not permit to get minbar price ")
            elif history_permission_denied:
                warnings.warn("Not permit to get history minbar price")
            elif today_permission_denied:
                warnings.warn("Not permit to get realtime minbar price")
            return

    result = _adjust_pf(
        pf,
        order_book_ids,
        stocks,
        funds,
        convertibles,
        futures888,
        start_date,
        end_date,
        has_dominant_id,
        adjust_type,
        skip_suspended,
        obid_slice_map,
        market,
    )
    return result


def get_daybar(order_book_ids, start_date, end_date, fields, duration, market):
    data = get_client().execute(
        "get_daybar_v", order_book_ids, start_date, end_date, fields, duration, market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    return convert_bar_to_multi_df(data, 'date', fields, int8_to_datetime_v)


def get_minbar(order_book_ids, start_date, end_date, fields, duration, market):
    data = get_client().execute(
        "get_minbar_v", order_book_ids, start_date, end_date, fields, duration, market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    return convert_bar_to_multi_df(data, "datetime", fields, int14_to_datetime_v)


def get_today_minbar(order_book_ids, fields, duration, market="cn"):
    data = get_client().execute("get_today_minbar", order_book_ids, fields, duration, market)
    return convert_bar_to_multi_df(data, "datetime", fields, int14_to_datetime_v)


def convert_bar_to_multi_df(data, dt_name, fields, convert_dt):
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
        return None, obid_slice_map

    obid_idx_map = {o: i for i, o in enumerate(obid_level)}
    obid_label = np.empty(line_no, dtype=object)
    dt_label = np.empty(line_no, dtype=object)
    arr = np.full((line_no, len(fields)), np.nan)
    r_map_fields = {f: i for i, f in enumerate(fields)}
    for f in ZERO_FILL_FIELDS:
        if f in fields:
            arr[:, r_map_fields[f]] = 0

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
        func_is_singletz = getattr(pd._libs.lib, 'is_datetime_with_singletz_array')
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', lambda *args: True)
    except AttributeError:
        func_is_singletz = None
    multi_idx = pd.MultiIndex(
        [obid_level, dt_level],
        [obid_label, dt_label],
        names=('order_book_id', dt_name)
    )
    if func_is_singletz is not None:
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', func_is_singletz)

    df = pd.DataFrame(data=arr, index=multi_idx, columns=fields)
    return df, obid_slice_map


def _adjust_pf(
        pf,
        order_book_ids,
        stocks,
        funds,
        convertibles,
        futures888,
        start_date,
        end_date,
        has_dominant_id,
        adjust_type,
        skip_suspended,
        obid_slice_map,
        market,
):
    adjust = (stocks or funds) and adjust_type in {"pre", "post", "pre_volume", "post_volume"}
    if adjust:
        from rqdatac.services.detail.adjust_price import adjust_price_multi_df
        adjust_price_multi_df(pf, stocks + funds, adjust_type, obid_slice_map, market)
    if has_dominant_id:
        # 1.全为非正常合约 2.有期货类型合约并且指定dominant_id字段
        # 只有满足其中一种才在返回字段中增加dominant_id
        add_dominant_id(pf, futures888, obid_slice_map)
    if skip_suspended and len(order_book_ids) == 1 and (stocks or convertibles):
        pf = filter_suspended(pf, order_book_ids[0], start_date, end_date, len(convertibles) > 0, market)

    if "trading_date" in pf:

        def convert_to_timestamp(v):
            if np.isnan(v):
                return pd.NaT
            return pd.Timestamp(str(int(v)))

        if hasattr(pf.trading_date, "applymap"):
            pf.trading_date = pf.trading_date.applymap(convert_to_timestamp)
        else:
            pf.trading_date = pf.trading_date.apply(convert_to_timestamp)

    if "settlement_direction" in pf:

        def convert_direction(key):
            if np.isnan(key):
                return key
            return SPOT_DIRECTION_MAP[key]

        if hasattr(pf.settlement_direction, "applymap"):
            pf.settlement_direction = pf.settlement_direction.applymap(convert_direction)
        else:
            pf.settlement_direction = pf.settlement_direction.apply(convert_direction)

    return pf


def add_dominant_id(result, futures888, obid_slice_map):
    for order_book_id, underlying in futures888.items():
        if order_book_id in obid_slice_map:
            slice_ = obid_slice_map[order_book_id]
            dts = result.index.get_level_values(1)[slice_]
            dominants = get_dominant_future(
                underlying, dts[0].date(), dts[-1].date())
            if dominants is not None:
                result.at[slice_, "dominant_id"] = np.take(
                    dominants.values, dominants.index.searchsorted(dts, side="right") - 1)


def filter_suspended(ret, order_book_id, start_date, end_date, is_convertible, market):
    if is_convertible:
        from rqdatac.services.convertible import is_suspended as is_convertible_suspend
        s = is_convertible_suspend(order_book_id, start_date, end_date)
    else:
        s = is_suspended(order_book_id, start_date, end_date, market)
    ret_date_index = ret.index.get_level_values(1)
    index = s.index.union(ret_date_index)
    s = s.reindex(index)
    s = s.fillna(method="ffill")
    s = s.loc[ret_date_index]
    s = s[order_book_id] == False
    return ret[s.values]


def get_week_df(order_book_ids, start_date, end_date, fields, adjust_type, market, stocks, funds, indexes, futures,
                futures888, spots, options, convertibles, repos):
    fields, has_dominant_id = _ensure_fields(fields, WEEKBAR_FIELDS, stocks, funds, futures, futures888, spots,
                                             options, convertibles, indexes, repos)
    has_volume_field = 'volume' in fields
    if not has_volume_field:
        fields.append('volume')
    df = get_price_df(
        order_book_ids, start_date, end_date, 'd', 1, fields, adjust_type, False,
        stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos, market
    )
    if df is None:
        return 
    res = resample_week_df(df, fields)
    if not has_volume_field:
        res.drop(columns=['volume'], inplace=True)
    return res
