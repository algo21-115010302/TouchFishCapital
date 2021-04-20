# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from rqdatac.client import get_client
from rqdatac.validators import ensure_string, check_items_in_container, ensure_order_book_id
from rqdatac.utils import to_datetime
from rqdatac.decorators import export_as_api


class Tmall:
    @staticmethod
    def stocks():
        """获取电商天猫数据股票列表

        :return:
            股票列表
        """

        return get_client().execute("tmall.stocks")

    @staticmethod
    def data(order_book_id, start_date=None, end_date=None, frequency="1d", fields=None):
        """获取天猫电商销售额数据

        :param order_book_id: 股票名
        :param start_date: 开始日期，默认为结束日期前一个月,  必须在2016年6月30日之后 (Default value = None)
        :param end_date: 结束日期 (Default value = None)
        :param frequency: 如'1d', '1M' (Default value = "1d")
        :param fields: 如'sales' (Default value = None)
        :returns: 返回DataFrame

        """
        order_book_id = ensure_order_book_id(order_book_id)

        if not end_date:
            end_date = datetime.date.today()

        if not start_date:
            start_date = end_date - datetime.timedelta(days=30)

        end_date = to_datetime(end_date)
        start_date = to_datetime(start_date)

        if start_date < datetime.datetime(2016, 6, 30):
            raise ValueError("start_date cannot be earlier than 2016-06-30")

        if start_date > end_date:
            raise ValueError()

        ensure_string(frequency, "frequency")
        check_items_in_container([frequency], {"1d", "1M"}, "frequency")

        if fields is None:
            fields = "sales"
        else:
            fields = ensure_string(fields, "fields")
            check_items_in_container(fields, ["sales"], "fields")
        data = get_client().execute("tmall.data", order_book_id, start_date, end_date, frequency)
        if not data:
            return
        df = pd.DataFrame(data)
        df = df.set_index("date")
        df.sort_index(inplace=True)
        df.columns = ["sales"]
        return df


@export_as_api
class ecommerce:
    tmall = Tmall
