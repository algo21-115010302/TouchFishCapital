
from touch_fish.event import EventEngine

from touch_fish.utils.engine import MainEngine
from touch_fish.utils.ui import MainWindow, create_qapp

from touch_fish.gateway.huobi import HuobiGateway
from touch_fish.gateway.huobif import HuobifGateway
from touch_fish.gateway.huobis import HuobisGateway
from touch_fish.gateway.huobio import HuobioGateway

from touch_fish.applications.cta_strategy import CtaStrategyApp
from touch_fish.applications.cta_backtester import CtaBacktesterApp
from touch_fish.applications.algo_trading import AlgoTradingApp
from touch_fish.applications.data_manager import DataManagerApp
from touch_fish.applications.data_recorder import DataRecorderApp


def main():
    """"""
    qapp = create_qapp()

    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    main_engine.add_gateway(HuobiGateway)
    main_engine.add_gateway(HuobifGateway)
    main_engine.add_gateway(HuobisGateway)
    main_engine.add_gateway(HuobioGateway)

    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(AlgoTradingApp)
    main_engine.add_app(DataManagerApp)
    main_engine.add_app(DataRecorderApp)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
