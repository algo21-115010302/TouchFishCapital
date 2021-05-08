# -*- coding: utf-8 -*-
import datetime
import six
import warnings
from itertools import islice
from functools import reduce

import pandas as pd
import numpy as np
import math
from dateutil.relativedelta import relativedelta
from sqlalchemy import Column
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import UnaryExpression
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import DeclarativeMeta
from pymysql.converters import conversions, escape_item, encoders

from rqdatac.client import get_client
from rqdatac.services.orm.balance_sheet_sql import StkBalaGen
from rqdatac.services.orm.cash_flow_sql import StkCashGen
from rqdatac.services.orm.financial_indicator_sql import AnaStkFinIdx
from rqdatac.services.orm.fundamental_base_sql import FundamentalBase
from rqdatac.services.orm.eod_derivative_indicator_sql import AnaStkValIdx
from rqdatac.services.orm.income_statement_sql import StkIncomeGen
from rqdatac.services.orm.ttm_sql import (
    CashFlowStatementTTM,
    IncomeStatementTTM,
    FinancialIndicatorTTM,
)
from rqdatac.services.orm.pit_financials import (
    BalanceSheet,
    CashFlowsStatement,
    IncomeStatement,
    MainData,
    TTM,
    PIT_TABLES,
    PIT_FIELDS,
    PIT_BASIC_FIELDS,
    PIT_BASIC_FIELDS_SET
)

from rqdatac.services.orm.pit_financials_ex import FIELDS_LIST_EX
from rqdatac.share.errors import MarketNotSupportError
from rqdatac.services.calendar import get_previous_trading_date
from rqdatac.utils import int8_to_date, to_date_int, to_datetime, is_panel_removed
from rqdatac.validators import (
    ensure_list_of_string,
    ensure_string,
    check_items_in_container,
    ensure_date_int,
    ensure_order_book_id,
    raise_for_no_panel,
    check_quarter,
    ensure_date_or_today_int,
    quarter_string_to_date,
    ensure_order_book_ids,
)
from rqdatac.decorators import export_as_api


@export_as_api(name="financials")
@export_as_api
class Financials:
    stockcode = FundamentalBase.stockcode
    announce_date = FundamentalBase.announce_date
    income_statement = StkIncomeGen
    balance_sheet = StkBalaGen
    cash_flow_statement = StkCashGen
    financial_indicator = AnaStkFinIdx
    cash_flow_statement_TTM = CashFlowStatementTTM
    income_statement_TTM = IncomeStatementTTM
    financial_indicator_TTM = FinancialIndicatorTTM


@export_as_api
def get_financials(query, quarter, interval=None, expect_df=False, market="cn"):
    """获取季度报表数据

    :param query: SQLAlchemy Query对象
    :param quarter: 季度, 如: '2016q1'
    :param interval: 2y', '4q', 与 get_fundamentals 类似，但只接受 y(year) 和 q(quarter)
        (Default value = None)
    :param market: 市场 (Default value = "cn")
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :returns: 如果返回结果中股票代码和查询指标为单个值, 返回Series;
    如果返回结果中股票代码或查询指标中有且只有一个为单个值, 返回pandas.DataFrame; 否则返回pandas.Panel

    """

    msg = "'get_financials' is deprecated, and will be removed soon. use get_pit_financials_ex instead."
    warnings.warn(msg, stacklevel=2)

    sql, quarters = _parse_arguments(query, quarter, interval)
    records = get_client().execute("get_financial", sql, market=market)
    return parse_results(records, quarters, expect_df)


def classify_fields(fields):
    if isinstance(fields, six.string_types) or fields in (
            BalanceSheet, CashFlowsStatement, IncomeStatement, TTM, MainData):
        fields = [fields]
    result = [
        # table_name fields
        ('balance_sheet_new', []),
        ('cash_flow_statement_new', []),
        ('income_statement_new', []),
        ('ttm_new', []),
        ('main_data', []),
        ('rnd_expenditure_sheet', []),
    ]

    for f in fields:
        if f in PIT_BASIC_FIELDS_SET:
            continue
        elif f in PIT_FIELDS[0]:
            result[0][1].append(f)
        elif f in PIT_FIELDS[1]:
            result[1][1].append(f)
        elif f in PIT_FIELDS[2]:
            result[2][1].append(f)
        elif f in PIT_FIELDS[3]:
            result[3][1].append(f)
        elif f in PIT_FIELDS[4]:
            result[4][1].append(f)
        elif f in PIT_FIELDS[5]:
            result[5][1].append(f)
        elif f in (BalanceSheet, CashFlowsStatement, IncomeStatement, TTM, MainData):
            index = PIT_TABLES.index(f.__name__)
            result[index][1].extend(PIT_FIELDS[index])
        else:
            raise ValueError('invalid field: {}'.format(f))
    return result


@export_as_api(name="pit_financials")
@export_as_api
class PitFinancials:
    balance_sheet = BalanceSheet
    cash_flows_statement = CashFlowsStatement
    income_statement = IncomeStatement
    ttm = TTM
    main_data = MainData


ENTERPRISE_TYPE_MAP = {
    13: "business_bank",
    31: "securities_firms",
    33: "trust",
    35: "insurance_company",
    39: "other_financial_institution",
    99: "general_enterprise",
}

INFO_TYPE_MAP = {
    10: "发行上市书",
    20: "定期报告",
    30: "业绩快报",
    50: "章程制度",
    70: "临时公告",
    90: "交易所通报",
    91: "交易所临时停(复)牌公告",
    99: "其他",
    110101: "定期报告:年度报告",
    110102: "定期报告:半年度报告",
    110103: "定期报告:第一季报",
    110104: "定期报告:第三季报",
    110105: "定期报告:审计报告",
    110106: "定期报告:第二季报",
    110107: "定期报告:第四季报",
    110108: "定期报告:第五季报",
    110109: "定期报告:第二季报（更正后）",
    110110: "定期报告:第四季报（更正后）",
    110111: "定期报告:第五季报（更正后）",
    110201: "定期报告:年度报告(关联方)",
    110202: "定期报告:半年度报告(关联方)",
    110203: "定期报告:第一季报(关联方)",
    110204: "定期报告:第三季报(关联方)",
    120101: "临时公告:审计报告(更正后)",
    120102: "临时公告:年度报告(更正后)",
    120103: "临时公告:半年度报告(更正后)",
    120104: "临时公告:第一季报(更正后)",
    120105: "临时公告:第三季报(更正后)",
    120106: "临时公告:公开转让说明书(更正后)",
    120107: "临时公告:业绩快报",
    120108: "临时公告:业绩快报(更正后)",
    120201: "临时公告:跟踪评级报告",
    120202: "临时公告:同业存单发行计划",
    120203: "临时公告:比较式财务报表",
    120204: "临时公告:关联方",
    120205: "临时公告:其他",
    120206: "临时公告:前期差错更正",
    120207: "临时公告:第一季度报告",
    120208: "临时公告:第二季度报告",
    120209: "临时公告:第三季度报告",
    120210: "临时公告:第四季度报告",
    120211: "临时公告：年度报告",
    130101: "发行上市书:募集说明书",
    130102: "发行上市书:招股说明书(申报稿)",
    130103: "发行上市书:招股意向书",
    130104: "发行上市书:上市公告书",
    130105: "发行上市书:审阅报告",
    130106: "发行上市书:招股说明书",
    130107: "发行上市书:公开转让说明书",
    130108: "发行上市书:发行公告",
    130109: "发行上市书:审计报告",
    130110: "发行上市书:关联方",
    130111: "发行上市书:其他",
    140101: "发行披露文件:第一季报",
    140102: "发行披露文件:半年度报告",
    140103: "发行披露文件：第三季报",
    140104: "发行披露文件：审计报告",
    140105: "发行披露文件：募集说明书",
    140106: "发行披露文件：跟踪评级报告"
}


@export_as_api
def get_pit_financials(fields, quarter, interval=None, order_book_ids=None,
                       if_adjusted='all', max_info_date=None, market='cn'):
    """
    获取pit季度报表数据
    :param fields: 财务指标 or 财务指标 list
    :param quarter: 季度, 如: '2016q1'
    :param interval: '2y', '4q', 默认只返回当季
    :param order_book_ids: 股票列表, 默认为None返回所有股票数据
    :param if_adjusted: 是否调整
        0: 每个order_book_id每个季度返回一条发布日期最新的未调整的数据
        1: 每个order_book_id每个季度返回一条发布日期最新的调整的数据
        'ignore': 每个order_book_id每个季度返回一条发布日期最新的数据
        'all': 返回所有数据,无论发布日期新旧和是否调整
    :param max_info_date: 指定最大发布日期, 如20180430，则所取数据的发布日期均不大于20180430，默认为None
    :return: pandas.DataFrame or None
    """

    msg = "'get_pit_financials' is deprecated, and will be removed soon. use get_pit_financials_ex instead."
    warnings.warn(msg, stacklevel=2)

    if if_adjusted not in [0, 1, '0', '1', 'all', 'ignore']:
        raise ValueError("if_adjusted should in [0, 1, 'all', 'ignore']")

    if order_book_ids is not None:
        order_book_ids = ensure_list_of_string(order_book_ids)
    quarters = get_quarters(quarter, interval)
    quarter_dates = [str(quarter_to_date(y, q)) for y, q in quarters]
    result = []
    for i, (table_name, fields) in enumerate(classify_fields(fields)):
        if not fields:
            continue

        if i == 3:  # ttm, exclude ['info_type', 'is_complete', 'enterprise_type']
            extra_fields = PIT_BASIC_FIELDS[0:-3]
        elif i == 4:  # main_data, exclude ['is_complete', 'enterprise_type']
            extra_fields = PIT_BASIC_FIELDS[0:-2]
        elif i == 5:
            extra_fields = ("order_book_id", "end_date", "if_adjusted", "info_date", "info_type")
        else:
            extra_fields = PIT_BASIC_FIELDS
        fields.extend(extra_fields)

        sql = """SELECT {}
        FROM {}
        """.format(', '.join(fields), table_name)
        where = ["end_date IN ('{}')".format("', '".join(quarter_dates))]
        if if_adjusted not in ['all', 'ignore']:
            where.append('if_adjusted {} 2'.format('=' if if_adjusted in (0, '0') else '<>'))
        if order_book_ids is not None:
            where.append("order_book_id IN ('{}')".format("', '".join(order_book_ids)))
        if max_info_date is not None:
            where.append("info_date <= '{}'".format(max_info_date))

        sql += """WHERE {}""".format(' AND '.join(where))
        records = get_client().execute("get_pit_financials", sql, market=market)
        if records:
            df = pd.DataFrame(records)
            df['if_adjusted'] = df['if_adjusted'].apply(lambda x: 0 if x == 2 else 1)

            if 'accounting_standards' in df.columns:
                df['accounting_standards'] = df['accounting_standards'].apply(lambda x: 1 if x == 1 else 0)
            if 'if_complete' in df.columns:
                df['if_complete'] = df['if_complete'].apply(lambda x: 1 if x == 1 else 0)

            if if_adjusted != 'all':
                if if_adjusted == 'ignore':
                    subset = ['order_book_id', 'end_date']
                else:
                    subset = ['order_book_id', 'end_date', 'if_adjusted']
                df.sort_values('info_date', inplace=True)
                df.drop_duplicates(subset=subset, keep='last', inplace=True)
            df.fillna(np.inf, inplace=True)
            result.append(df)
    if not result:
        return
    if len(result) == 1:
        result = result[0]
    elif len(result) > 1:
        result = reduce(
            lambda left, right: pd.merge(
                left, right, how='outer', on=[f for f in left.columns if f in right.columns]
            ), result)

    result.sort_values(by=['order_book_id', 'end_date', 'info_date'], inplace=True)
    result.set_index(['order_book_id', 'end_date'], inplace=True)

    base_columns = ['info_date', 'if_adjusted']
    if 'accounting_standards' in result.columns:
        base_columns.append('accounting_standards')
    if 'if_complete' in result.columns:
        result.rename(columns={'if_complete': 'is_complete'}, inplace=True)
        result['enterprise_type'] = result['enterprise_type'].map(ENTERPRISE_TYPE_MAP)
        base_columns.extend(['is_complete', 'enterprise_type'])
    if 'info_type' in result.columns:
        result['info_type'] = result['info_type'].map(INFO_TYPE_MAP)
        base_columns.extend(['info_type'])

    return result[base_columns + [c for c in result.columns if c not in base_columns]]


@export_as_api
def get_pit_financials_ex(order_book_ids, fields, start_quarter, end_quarter,
                          date=None, statements='latest', market='cn'):
    """
        获取股票财务数据(Point In Time)
    :param order_book_ids: 股票合约代码列表
    :param fields: 指定返回财报字段
    :param start_quarter: 财报季度 - 起始，如 2020q1
    :param end_quarter: 财报季度 - 截止
    :param date: 财报发布日期，默认为当前日期, 如 '2020-01-01' | '20200101'
    :param statements: 可选 latest/all, 默认为 latest
            latest: 仅返回在date时点所能观察到的最新数据；
            all：返回在date时点所能观察到的所有版本，从第一次发布直到观察时点的所有修改。
    :param market: 股票市场范围
    :return:
    """
    fields = ensure_list_of_string(fields, 'fields')
    check_items_in_container(fields, FIELDS_LIST_EX, "fields")
    fields.extend(['order_book_id', 'info_date', 'end_date'])
    fields = list(set(fields))
    fields[fields.index("info_date")], fields[0] = fields[0], fields[fields.index("info_date")]

    check_quarter(start_quarter, 'start_quarter')
    start_quarter_int = ensure_date_int(quarter_string_to_date(start_quarter))

    check_quarter(end_quarter, 'end_quarter')
    end_quarter_int = ensure_date_int(quarter_string_to_date(end_quarter))

    if start_quarter > end_quarter:
        raise ValueError(
            'invalid quarter range: [{!r}, {!r}]'.format(
                start_quarter, end_quarter))

    date = ensure_date_or_today_int(date)

    order_book_ids = ensure_list_of_string(order_book_ids, 'order_book_ids')

    if statements not in ['all', 'latest']:
        raise ValueError("invalid statements , got {!r}".format(statements))

    pit_financial_df = pd.DataFrame(
        get_client().execute("get_pit_financials_ex", order_book_ids, fields, start_quarter_int, end_quarter_int, date,
                             statements, market))
    if pit_financial_df.empty:
        return
    pit_financial_df = pit_financial_df.reindex(columns=fields)
    pit_financial_df.sort_values(['order_book_id', 'end_date', 'info_date'])
    pit_financial_df["end_date"] = pit_financial_df["end_date"].apply(
        lambda d: "{}q{}".format(d.year, math.ceil(d.month / 3)))
    pit_financial_df.rename(columns={"end_date": "quarter"}, inplace=True)
    pit_financial_df.set_index(['order_book_id', 'quarter'], inplace=True)
    pit_financial_df.sort_index(inplace=True)
    return pit_financial_df


@export_as_api
def get_fundamentals(query, entry_date, interval=None, report_quarter=False, expect_df=False, market="cn"):
    """获取财务数据

    :param query: query 对象
    :param entry_date: 日期
    :param interval:  (Default value = None)
    :param report_quarter:  (Default value = False)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :param market:  (Default value = "cn")

    """

    msg = "'get_fundamentals' is deprecated, and will be removed soon. use get_factor instead."
    warnings.warn(msg, stacklevel=2)

    if market != "cn":
        raise MarketNotSupportError("don't support market {} yet.", market)

    if not isinstance(query, Query):
        raise ValueError("a sqlalchemy's Query object expected: {}".format(type(query)))

    raise_for_no_panel(expect_df)
    entry_date = to_datetime(entry_date)
    delta = 0
    duration = 0
    if interval is not None:
        if not isinstance(interval, str):
            raise ValueError(
                "invalid interval: {} should be a string like 1d, 5y, 3m, 2q".format(interval)
            )
        if interval[-1] not in __TIME_DELTA_MAP__:
            raise ValueError(
                "invalid interval: {}, interval unit should be d(day), "
                "m(month), q(quarter) or y(year)".format(interval)
            )
        delta = __TIME_DELTA_MAP__[interval[-1]]

        try:
            duration = int(interval[:-1])
        except ValueError:
            raise ValueError(
                "invalid interval: {}, should be a string like 1d, 5y, 3m, 2q".format(interval)
            )

    trading_dates = [get_previous_trading_date(entry_date + __TIME_DELTA_MAP__["d"], market=market)]
    if duration > 0:
        current_date = trading_dates[0]
        one_day = __TIME_DELTA_MAP__["d"]
        for i in range(duration - 1):
            current_date = get_previous_trading_date(current_date - delta + one_day, market=market)
            trading_dates.append(current_date)

    query = _unsafe_apply_query_filter(query, trading_dates)
    sql = _compile_query(query)
    records = get_client().execute("get_fundamentals", sql, market=market)

    if not records:
        warnings.warn("No record found")
        return None

    base_fields = ["STOCKCODE", "TRADEDATE", "RPT_YEAR", "RPT_QUARTER"]
    field_names = base_fields + list(set(records[0].keys()) - set(base_fields))
    items = ["report_quarter"] + field_names[4:] if report_quarter else field_names[4:]

    if expect_df:
        df = pd.DataFrame(records)
        df.rename(columns={"STOCKCODE": "order_book_id", "TRADEDATE": "date"}, inplace=True)
        df["report_quarter"] = df["RPT_YEAR"].map(str) + "q" + df["RPT_QUARTER"].map(str)
        df.sort_values(["order_book_id", "date"], ascending=[True, False], inplace=True)
        df.set_index(["order_book_id", "date"], inplace=True)
        for item in items:
            if item != "report_quarter":
                df[item] = df[item].astype(np.float64)
        return df[items]

    # 只有一个查询日期时, 保持顺序
    if len(trading_dates) > 1:
        stocks = list(set([r[field_names[0]] for r in records]))
    else:
        stocks = [r[field_names[0]] for r in records]

    stock_index = {s: i for i, s in enumerate(stocks)}
    day_index = {d: i for i, d in enumerate(trading_dates)}

    removed_items_size = 3 if report_quarter else 4

    array = np.ndarray(
        ((len(records[0]) - removed_items_size), len(trading_dates), len(stocks)), dtype=object
    )
    array.fill(np.nan)
    for r in records:
        istock = stock_index[r[field_names[0]]]
        iday = day_index[int8_to_date(r[field_names[1]])]
        for i in range(4, len(r)):
            array[(i - removed_items_size, iday, istock)] = np.float64(r[field_names[i]])
        if report_quarter:
            array[(0, iday, istock)] = (
                np.nan
                if None in (r[field_names[2]], r[field_names[3]])
                else str(r[field_names[2]]) + "q" + str(r[field_names[3]])
            )

    trading_dates = pd.to_datetime(trading_dates)

    warnings.warn("Panel is  removed after pandas version 0.25.0."
                  "the  default value of 'expect_df' will change to True in the future.")
    return pd.Panel(data=array, items=items, major_axis=trading_dates, minor_axis=stocks)


deprecated_fields = {"data_point": "data_point", "table": "table", "comment": "comment"}


@export_as_api
def deprecated_fundamental_data(fields=None, market="cn"):
    fields = _check_deprecated_fields(fields)
    df = pd.DataFrame(get_client().execute("deprecated_fundamental_data", fields, market=market))

    if len(df) < 1:
        return None
    if len(fields) == 1:
        df = list(df[fields[0]])
    return df


@export_as_api
def current_performance(
        order_book_id, info_date=None, quarter=None, interval="1q", fields=None, market="cn"
):
    """获取A股快报

    :param order_book_id: 股票代码, 如'000001.XSHE'
    :param info_date: 发布日期, 如'20180501', 默认为最近的交易日 (Default value = None)
    :param quarter: 发布季度, 如'2018q1' (Default value = None)
    :param interval: 数据区间， 发布日期, 如'2y', '4q' (Default value = "1q")
    :param fields: str 或 list 类型. 默认为 None, 返回所有字段 (Default value = None)
    :param market: 地区代码, 如'cn' (Default value = "cn")
    :returns: pd.DataFrame

    """
    order_book_id = ensure_order_book_id(order_book_id, market=market)
    end_date = None
    if info_date:
        info_date = ensure_date_int(info_date)
    elif quarter:
        splited = quarter.lower().split("q")
        if len(quarter) != 6 or len(splited) != 2:
            raise ValueError(
                "invalid argument {}: {}, valid parameter: {}".format(
                    "quarter", quarter, "string format like '2016q1'"
                )
            )

        year, quarter = int(splited[0]), int(splited[1])
        if not 1 <= quarter <= 4:
            raise ValueError(
                "invalid argument {}: {}, valid parameter: {}".format(
                    "quarter", quarter, "quarter should be in [1, 4]"
                )
            )
        month, day = QUARTER_DATE_MAP[quarter]
        end_date = ensure_date_int(datetime.datetime(year, month, day))
    else:
        info_date = ensure_date_int(datetime.date.today())
    ensure_string(interval, "interval")
    if interval[-1] not in ("y", "q", "Y", "Q"):
        raise ValueError(
            "invalid argument {}: {}, valid parameter: {}".format(
                "interval", interval, "interval unit should be q(quarter) or y(year)"
            )
        )

    try:
        int(interval[:-1])
    except ValueError:
        raise ValueError(
            "invalid argument {}: {}, valid parameter: {}".format(
                "interval", interval, "string like 4q, 2y"
            )
        )
    interval = interval.lower()

    if fields is not None:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, PERFORMANCE_FIELDS, "fields")
    else:
        fields = PERFORMANCE_FIELDS

    data = get_client().execute(
        "current_performance", order_book_id, info_date, end_date, fields, market=market
    )
    if not data:
        return
    df = pd.DataFrame(data)
    sort_field = "info_date" if info_date else "end_date"
    df.sort_values(by=[sort_field, "mark"], ascending=[False, True], inplace=True)
    df.drop_duplicates(subset="end_date", keep="first", inplace=True)
    num = int(interval[:-1])
    unit = interval[-1]
    if unit == "y":
        latest_month = df.loc[0, "end_date"].month
        df["month"] = df.end_date.apply(lambda x: x.month)
        df = df[df.month == latest_month]
    df.reset_index(drop=True, inplace=True)
    return df.loc[: num - 1, ["end_date", "info_date"] + fields]


PERFORMANCE_FORECAST_FIELDS = [
    "forecast_type",
    "forecast_description",
    "forecast_growth_rate_floor",
    "forecast_growth_rate_ceiling",
    "forecast_earning_floor",
    "forecast_earning_ceiling",
    "forecast_np_floor",
    "forecast_np_ceiling",
    "forecast_eps_floor",
    "forecast_eps_ceiling",
    "net_profit_yoy_const_forecast",
]


@export_as_api
def performance_forecast(order_book_ids, info_date=None, end_date=None, fields=None, market="cn"):
    """获取业绩预报

    :param order_book_ids: 股票代码，如['000001.XSHE', '000002.XSHE']
    :param info_date: 信息发布日期，如'20180501'，默认为最近的交易日 (Default value = None)
    :param end_date: 业绩预计报告期，如'20180501'，默认为最近的交易日 (Default value = None)
    :param fields: str或list类型. 默认为None，返回所有字段 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: pd.DataFrame

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type='CS')
    if info_date:
        info_date = ensure_date_int(info_date)
    elif end_date:
        end_date = ensure_date_int(end_date)
    else:
        info_date = ensure_date_int(datetime.datetime.today())

    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, PERFORMANCE_FORECAST_FIELDS, "fields")
    else:
        fields = PERFORMANCE_FORECAST_FIELDS

    data = get_client().execute(
        "performance_forecast", order_book_ids, info_date, end_date, fields, market=market
    )
    if not data:
        return
    if len(order_book_ids) > 1:
        df = pd.DataFrame(data, columns=["order_book_id", "info_date", "end_date"] + fields)
        return df.set_index("order_book_id")

    return pd.DataFrame(data, columns=["info_date", "end_date"] + fields)


def _check_deprecated_fields(fields):
    try:
        if fields is None:
            fields = list(deprecated_fields.values())
        elif isinstance(fields, str):
            fields = [deprecated_fields[fields]]
        elif isinstance(fields, list):
            fields = [deprecated_fields[i] for i in fields]
        else:
            raise ValueError("fields should be string or list of strings")
    except KeyError:
        raise ValueError(
            "invalid argument fields: {!r}, valid parameter: {!r}".format(
                fields, list(deprecated_fields)
            )
        )
    return fields


PERFORMANCE_FIELDS = [
    "operating_revenue",
    "gross_profit",
    "operating_profit",
    "total_profit",
    "np_parent_owners",
    "net_profit_cut",
    "net_operate_cashflow",
    "total_assets",
    "se_without_minority",
    "total_shares",
    "basic_eps",
    "eps_weighted",
    "eps_cut_epscut",
    "eps_cut_weighted",
    "roe",
    "roe_weighted",
    "roe_cut",
    "roe_cut_weighted",
    "net_operate_cashflow_per_share",
    "equity_per_share",
    "operating_revenue_yoy",
    "gross_profit_yoy",
    "operating_profit_yoy",
    "total_profit_yoy",
    "np_parent_minority_pany_yoy",
    "ne_t_minority_ty_yoy",
    "net_operate_cash_flow_yoy",
    "total_assets_to_opening",
    "se_without_minority_to_opening",
    "basic_eps_yoy",
    "eps_weighted_yoy",
    "eps_cut_yoy",
    "eps_cut_weighted_yoy",
    "roe_yoy",
    "roe_weighted_yoy",
    "roe_cut_yoy",
    "roe_cut_weighted_yoy",
    "net_operate_cash_flow_per_share_yoy",
    "net_asset_psto_opening",
]

QUARTER_DATE_MAP = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}

__TIME_DELTA_MAP__ = {
    "y": relativedelta(years=1),
    "m": relativedelta(months=1),
    "q": relativedelta(months=3),
    "d": relativedelta(days=1),
}


@export_as_api(name="fundamentals")
@export_as_api
class Fundamentals:
    stockcode = FundamentalBase.stockcode
    announce_date = FundamentalBase.announce_date
    income_statement = StkIncomeGen
    balance_sheet = StkBalaGen
    cash_flow = StkCashGen
    cash_flow_statement = StkCashGen
    financial_indicator = AnaStkFinIdx
    eod_derivative_indicator = AnaStkValIdx
    fundamental_base = FundamentalBase
    cash_flow_statement_TTM = CashFlowStatementTTM
    income_statement_TTM = IncomeStatementTTM
    financial_indicator_TTM = FinancialIndicatorTTM


@export_as_api(name="query")
def query_entities(*entities):
    base_list = ["stockcode", "tradedate", "end_date", "announce_date", "rpt_year", "rpt_quarter"]
    columns = [
        Fundamentals.fundamental_base.stockcode,
        Fundamentals.fundamental_base.tradedate,
        Fundamentals.fundamental_base.rpt_year,
        Fundamentals.fundamental_base.rpt_quarter,
    ]
    for ele in entities:
        if isinstance(ele, DeclarativeMeta):
            deprecated_list = deprecated_fundamental_data("data_point")
            query_list = [
                v
                for k, v in ele.__dict__.items()
                if not k.startswith("_") and k not in base_list and k not in deprecated_list
            ]
            columns.extend(query_list)
        elif isinstance(ele, InstrumentedAttribute):
            name = str(ele).split(".")[-1]
            if name in ["stockcode", "tradedate", "rpt_year", "rpt_quarter"]:
                continue
            columns.append(ele)
        else:
            raise ValueError(
                "Invalid metrics to query, it maybe not specify metrics, "
                "please check the metrics in query."
            )

    return Query(columns)


def _compile_query(query):
    comp = query.statement.compile(dialect=mysql.dialect())
    comp_params = comp.params
    params = []
    for k in comp.positiontup:
        v = comp_params[k]
        params.append(escape_item(v, conversions, encoders))

    comp = comp.string
    if "equity_preferred_stock" in comp:
        if "equity_prefer_stock" in comp:
            comp = comp.replace("fundamental_view.equity_prefer_stock,", "")
        comp = comp.replace("equity_preferred_stock", "equity_prefer_stock as equity_preferred_stock")
    elif "equity_prefer_stock" in comp:
        warnings.warn("'equity_prefer_stock' has been deprecated, please use 'equity_preferred_stock'.")

    return comp % tuple(params)


def _unsafe_apply_query_filter(query, trading_dates):
    # TODO this is a hack
    limit, offset = query._limit, query._offset
    query = query.limit(None).offset(None)

    if query._order_by:
        # 对于存在order_by的, 过滤掉 NaN
        def _filter(q, column):
            if isinstance(column, Column):
                return q.filter(column is not None)
            if isinstance(column, UnaryExpression):
                return _filter(q, column.element)
            return q

        for creterion in query._order_by:
            query = _filter(query, creterion)

    query = query.filter(FundamentalBase.tradedate.in_([to_date_int(d) for d in trading_dates]))

    if len(trading_dates) > 1:
        query = query.order_by(FundamentalBase.tradedate.desc())
    return query.limit(limit).offset(offset)


def _parse_arguments(query, quarter, interval):
    if not isinstance(query, Query):
        raise ValueError(
            "The first argument must be a sqlalchemy's Query object. "
            "But what passed in was: " + str(type(query))
        )
    quarters = get_quarters(quarter, interval)
    quarter_dates = [quarter_to_date(y, q) for y, q in quarters]
    query = adjust_query(query, quarter_dates)
    return _compile_query(query), quarters


def quarter_generator(year, quarter):
    while True:
        yield year, quarter
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1


def year_generator(year, quarter):
    while True:
        yield year, quarter
        year -= 1


def quarter_to_date(year, quarter):
    dates = (331, 630, 930, 1231)
    return year * 10000 + dates[quarter - 1]


def get_quarters(quarter, interval):
    if quarter is None:
        raise ValueError("quarter is required.")

    splited = quarter.lower().split("q")
    if len(splited) != 2:
        raise ValueError("wrong quarter format, use format like '2016q1'")

    year, quarter = int(splited[0]), int(splited[1])
    if not 1 <= quarter <= 4:
        raise ValueError("quarter should be in [1, 4]")

    if interval is not None:
        if not isinstance(interval, str):
            raise ValueError("interval should be a string like 4q, 2y")

        if interval[-1] not in ("y", "q", "Y", "Q"):
            raise ValueError("interval unit should be y(year) or q(quarter)")
        try:
            int(interval[:-1])
        except ValueError:
            raise ValueError("interval should be a string like 4q, 2y")
    if interval is not None:
        generator = (quarter_generator if interval[-1].lower() == "q" else year_generator)(
            year, quarter
        )
        n = int(interval[:-1])
        return list(islice(generator, n))
    else:
        return [(year, quarter)]


def adjust_query(query, quarter_dates):
    # hack
    if str(query._entities[1]).split(".")[-1] == "tradedate":
        query._entities.pop(1)  # remove tradedate entity
    limit, offset = query._limit, query._offset
    query.limit(None).offset(None)

    query = query.filter(FundamentalBase.end_date.in_(quarter_dates))

    if len(quarter_dates) > 1:
        query = query.order_by(FundamentalBase.end_date.desc())
    return query.limit(limit).offset(offset)


def parse_results(records, quarters, expect_df):
    if not records:
        return None

    removed_items_size = 3
    base_fields = ["stockcode", "rpt_year", "rpt_quarter"]

    field_names = base_fields + list(set(records[0].keys()) - set(base_fields))
    items = field_names[removed_items_size:]

    if not expect_df and not is_panel_removed:
        # 只有一个查询日期时, 保持顺序
        stocks = list(set(r[field_names[0]] for r in records))
        stock_index = {s: i for i, s in enumerate(stocks)}
        quarter_index = {q: i for i, q in enumerate(quarters)}

        array = np.ndarray(
            (len(records[0]) - removed_items_size, len(quarters), len(stocks)), dtype=object
        )
        array.fill(np.nan)
        for r in records:
            istock = stock_index[r[field_names[0]]]
            iquarter = quarter_index[(r[field_names[1]], r[field_names[2]])]
            for i in range(removed_items_size, len(r)):
                if field_names[i] == "announce_date":
                    announce_date = r[field_names[i]]
                    array[i - removed_items_size, iquarter, istock] = (
                        np.nan if announce_date is None else announce_date
                    )
                else:
                    array[i - removed_items_size, iquarter, istock] = np.float64(r[field_names[i]])

        s_quarters = ["%dq%d" % (year, quarter) for year, quarter in quarters]
        results = pd.Panel(data=array, items=items, major_axis=s_quarters, minor_axis=stocks)
        item_size, major_size, minor_size = results.shape
        if minor_size == 1:
            ret = results.minor_xs(*stocks)
            return ret[field_names[removed_items_size]] if item_size == 1 else ret
        elif item_size == 1:
            return results[field_names[removed_items_size]]
        elif major_size == 1:
            return results.major_xs(*s_quarters)
        else:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
            return results
    else:
        df = pd.DataFrame(records)
        df.rename(columns={"stockcode": "order_book_id"}, inplace=True)
        df["quarter"] = df["rpt_year"].map(str) + "q" + df["rpt_quarter"].map(str)
        df.sort_values(["order_book_id", "quarter"], ascending=[True, False], inplace=True)
        df.set_index(["order_book_id", "quarter"], inplace=True)
        for item in items:
            if item != "announce_date":
                df[item] = df[item].astype(np.float64)
        df = df[items]
        if expect_df:
            return df
        else:
            if len(df.index.get_level_values(0).unique()) == 1:
                df.reset_index(level=0, inplace=True, drop=True)
                df.index.name = None
                df.sort_index(ascending=False, inplace=True)
                if len(df.columns) == 1:
                    df = df[df.columns[0]]
                return df
            elif len(df.columns) == 1:
                field = df.columns[0]
                df = df.unstack(0)[field]
                df.index.name = None
                df.columns.name = None
                df.sort_index(ascending=False, inplace=True)
                return df
            elif len(df.index.get_level_values(1).unique()) == 1:
                df.reset_index(level=1, inplace=True, drop=True)
                df.index.name = None
                return df
            raise_for_no_panel()
