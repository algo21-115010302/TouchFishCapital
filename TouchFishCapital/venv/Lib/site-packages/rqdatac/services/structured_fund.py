# -*- coding: utf-8 -*-
from decimal import Decimal, DecimalTuple


import pandas as pd

from rqdatac.client import get_client
from rqdatac.decorators import export_as_api
from rqdatac.validators import ensure_list_of_string

_SIGN = "sign"
_DIGITS = "digits"
_EXPONENT = "exponent"


def _encode_decimal(num, prefix):
    decimal_tuple = Decimal(str(num)).normalize().as_tuple()
    prefix += "."
    encoded_decimal = {
        prefix + _SIGN: decimal_tuple.sign,
        prefix + _DIGITS: decimal_tuple.digits,
        prefix + _EXPONENT: decimal_tuple.exponent,
    }
    return encoded_decimal


def _decode_decimal(encoded_decimal):
    sign = encoded_decimal[_SIGN]
    digits = tuple(encoded_decimal[_DIGITS])
    exponent = encoded_decimal[_EXPONENT]
    return Decimal(DecimalTuple(sign=sign, digits=digits, exponent=exponent))


def _remake_decimal(dlist):
    for one in dlist:
        for key, value in one.items():
            if isinstance(value, dict) and value.get(_DIGITS):
                one[key] = _decode_decimal(value)


@export_as_api(namespace="fenji")
def get_a_by_yield(current_yield, listing=True, market="cn"):
    if listing is not None:
        listing = bool(listing)

    data = get_client().execute("fenji.get_a_by_yield", current_yield, listing, market)
    return data


@export_as_api(namespace="fenji")
def get_a_by_interest_rule(interest_rule, listing=True, market="cn"):
    if listing is not None:
        listing = bool(listing)
    data = get_client().execute("fenji.get_a_by_interest_rule", interest_rule, listing, market)
    return data


@export_as_api(namespace="fenji")
def get_all(field_list=None, market="cn"):
    data = get_client().execute("fenji.get_all", field_list, market)
    _remake_decimal(data)
    df = pd.DataFrame(data)
    df.reindex(columns=sorted(df.columns))
    return df


@export_as_api(namespace="fenji")
def get(order_book_ids, field_list=None, market="cn"):
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    data = get_client().execute("fenji.get", order_book_ids, field_list, market)
    if not data:
        return
    _remake_decimal(data)
    df = pd.DataFrame(data)
    df.reindex(columns=sorted(df.columns))
    return df
