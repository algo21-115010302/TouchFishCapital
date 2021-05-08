# coding: utf-8
from sqlalchemy import Numeric, Column
from .fundamental_base_sql import FundamentalBase


class AnaStkValIdx(FundamentalBase):
    pe_ratio = Column(Numeric(18, 4))
    pcf_ratio = Column(Numeric(18, 4))
    pb_ratio = Column(Numeric(18, 4))
    market_cap = Column(Numeric(21, 4))
    market_cap_2 = Column(Numeric(21, 4))
    a_share_market_val = Column(Numeric(21, 4))
    a_share_market_val_2 = Column(Numeric(21, 4))
    val_of_stk_right = Column(Numeric(21, 4))
    dividend_yield = Column(Numeric(18, 4))
    pe_ratio_1 = Column(Numeric(18, 4))
    pe_ratio_2 = Column(Numeric(18, 4))
    peg_ratio = Column(Numeric(18, 4))
    pcf_ratio_1 = Column(Numeric(18, 4))
    pcf_ratio_2 = Column(Numeric(18, 4))
    pcf_ratio_3 = Column(Numeric(18, 4))
    ps_ratio = Column(Numeric(18, 4))
    enterprise_value = Column(Numeric(19, 4))
    enterprise_value_2 = Column(Numeric(19, 4))
