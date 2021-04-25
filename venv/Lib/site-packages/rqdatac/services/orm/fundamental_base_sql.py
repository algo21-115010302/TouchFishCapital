# coding: utf-8
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class FundamentalBase(Base):
    __tablename__ = "fundamental_view"

    stockcode = Column(String(12), primary_key=True, nullable=False)
    tradedate = Column(Integer, primary_key=True, nullable=False)
    end_date = Column(Integer)
    announce_date = Column(Integer)
    rpt_year = Column(Integer)
    rpt_quarter = Column(Integer)
