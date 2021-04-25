# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
import numpy as np
import pandas as pd
from rqdatac.services.calendar import (get_previous_trading_date,
                                       is_trading_date)

FIELD_METHOD_MAP = {
    "open": "first",
    "close": "last",
    "iopv": "last",
    "high": np.maximum,
    "low": np.minimum,
    "limit_up": np.maximum,
    "limit_down": np.minimum,
    "total_turnover": np.add,
    "volume": np.add,
    "num_trades": np.add,
    "acc_net_value": "last",
    "unit_net_value": "last",
    "discount_rate": "last",
    "settlement": "last",
    "prev_settlement": "last",
    "open_interest": "last",
    "basis_spread": "last",
    "date": "last",
    "trading_date": "last",
    "datetime": "last",
}
FIELD_METHOD_MAP2 = {
    "open": "first",
    "close": "last",
    "iopv": "last",
    "high": "max",
    "low": "min",
    "total_turnover": "sum",
    "volume": "sum",
    "num_trades": "sum",
    "acc_net_value": "last",
    "unit_net_value": "last",
    "discount_rate": "last",
    "settlement": "last",
    "prev_settlement": "last",
    "open_interest": "last",
    "basis_spread": "last",
    "contract_multiplier": "last",
    "strike_price": "last",
}


def _update_weekly_trading_date_index(idx):
    if is_trading_date(idx[1]):
        return idx
    return idx[0], get_previous_trading_date(idx[1])


def resample_week_df(df, fields):
    hows = {field: FIELD_METHOD_MAP2[field] for field in fields}
    res1 = df[df['volume'] > 0]
    if not res1.empty:
        res1 = res1.groupby(level='order_book_id').resample('W-Fri', level=1).agg(hows)
    res2 = df[df['volume'] == 0]
    if not res2.empty:
        res2 = res2.groupby(level='order_book_id').resample('W-Fri', level=1).agg(hows)
    res = pd.concat([res1, res2])
    res.index = res.index.map(_update_weekly_trading_date_index)
    res = res[~res.index.duplicated(keep='first')]
    res.sort_index(inplace=True)
    return res
