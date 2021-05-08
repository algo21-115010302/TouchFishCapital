from pathlib import Path

from touch_fish.utils.app import BaseApp
from touch_fish.utils.constant import Direction
from touch_fish.utils.object import TickData, BarData, TradeData, OrderData
from touch_fish.utils.utility import BarGenerator, ArrayManager

from .engine import RecorderEngine, APP_NAME


class DataRecorderApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "行情记录"
    engine_class = RecorderEngine
    widget_name = "RecorderManager"
    icon_name = "recorder.ico"
