"""
导入数字货币策略
"""
from datetime import datetime

from touch_fish.event import EventEngine
from touch_fish.utils.constant import Exchange, Interval
from touch_fish.utils.engine import MainEngine
from touch_fish.applications.data_manager.engine import ManagerEngine

event_engine = EventEngine()
main_engine = MainEngine(event_engine)

manager_engine = ManagerEngine(main_engine, event_engine)

manager_engine.import_data_from_csv(file_path="D:/TouchFishCapital/data.csv",
                                    symbol='BTCUSDT',
                                    exchange=Exchange.HUOBI,
                                    interval=Interval.MINUTE,
                                    datetime_head='datetime',
                                    open_head='open',
                                    high_head='high',
                                    low_head='low',
                                    close_head='close',
                                    volume_head='volume',
                                    open_interest_head='open_interest',
                                    datetime_format='%Y-%m-%d %H:%M:%S')

sample_data = manager_engine.load_bar_data(symbol='BTCUSDT',
                             exchange=Exchange.HUOBI,
                             interval=Interval.MINUTE,
                             start=datetime(2018,1,1),
                             end=datetime(2018,1,10))

