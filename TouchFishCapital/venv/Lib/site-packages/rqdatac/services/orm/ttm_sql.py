# -*- coding: utf-8 -*-
from sqlalchemy import Numeric, Column
from .fundamental_base_sql import FundamentalBase


class CashFlowStatementTTM(FundamentalBase):
    net_finance_cashflowTTM = Column(Numeric(18, 4))
    net_operate_cashflowTTM = Column(Numeric(18, 4))
    net_invest_cashflowTTM = Column(Numeric(18, 4))
    net_cashflowTTM = Column(Numeric(18, 4))
    sale_service_render_cashTTM = Column(Numeric(18, 4))


class IncomeStatementTTM(FundamentalBase):
    np_parent_company_ownersTTM = Column(Numeric(18, 4))
    financial_expenseTTM = Column(Numeric(18, 4))
    administration_expenseTTM = Column(Numeric(18, 4))
    ni_from_value_changeTTM = Column(Numeric(18, 4))
    ni_from_operatingTTM = Column(Numeric(18, 4))
    net_profitTTM = Column(Numeric(18, 4))
    total_profitTTM = Column(Numeric(18, 4))
    gross_profitTTM = Column(Numeric(18, 4))
    ebitTTM = Column(Numeric(18, 4))
    operating_expenseTTM = Column(Numeric(18, 4))
    operating_costTTM = Column(Numeric(18, 4))
    operating_profitTTM = Column(Numeric(18, 4))
    operating_revenueTTM = Column(Numeric(18, 4))
    non_operating_net_incomeTTM = Column(Numeric(18, 4))
    operating_payoutTTM = Column(Numeric(18, 4))
    total_operating_costTTM = Column(Numeric(18, 4))
    total_operating_revenueTTM = Column(Numeric(18, 4))
    asset_impairment_lossTTM = Column(Numeric(18, 4))


class FinancialIndicatorTTM(FundamentalBase):
    eps_belongs_to_parentTTM = Column(Numeric(18, 4))
    epsTTM = Column(Numeric(18, 4))
    financial_expense_rateTTM = Column(Numeric(18, 4))
    invest_associates_inc_to_tpTTM = Column(Numeric(18, 4))
    admin_expense_rateTTM = Column(Numeric(18, 4))
    value_change_ni_to_tpTTM = Column(Numeric(18, 4))
    no_cf_to_operating_niTTM = Column(Numeric(18, 4))
    cash_rate_of_salesTTM = Column(Numeric(18, 4))
    operating_mi_to_tpTTM = Column(Numeric(18, 4))
    net_profit_to_total_operating_revenueTTM = Column(Numeric(18, 4))
    return_on_equityTTM = Column(Numeric(18, 4))
    operating_cashflow_per_shareTTM = Column(Numeric(18, 4))
    cashflow_per_shareTTM = Column(Numeric(18, 4))
    operating_revenue_per_shareTTM = Column(Numeric(18, 4))
    ebit_to_total_revenueTTM = Column(Numeric(18, 4))
    operating_expense_rateTTM = Column(Numeric(18, 4))
    net_profit_ratioTTM = Column(Numeric(18, 4))
    gross_income_ratioTTM = Column(Numeric(18, 4))
    period_costs_rateTTM = Column(Numeric(18, 4))
    sale_service_cash_to_operating_revenueTTM = Column(Numeric(18, 4))
    operating_profit_to_total_revenueTTM = Column(Numeric(18, 4))
    net_non_operating_income_to_revenueTTM = Column(Numeric(18, 4))
    total_operating_cost_to_total_operating_revenueTTM = Column(Numeric(18, 4))
    asset_impair_loss_to_total_operating_revenueTTM = Column(Numeric(18, 4))
    return_on_asset_ebitTTM = Column(Numeric(18, 4))
    return_on_assetTTM = Column(Numeric(18, 4))
