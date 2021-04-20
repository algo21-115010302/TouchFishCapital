
from vnpy.event import EventEngine

from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy.gateway.binance import BinanceGateway
from vnpy.gateway.binances import BinancesGateway
from vnpy.gateway.huobi import HuobiGateway
from vnpy.gateway.huobif import HuobifGateway
from vnpy.gateway.huobis import HuobisGateway
from vnpy.gateway.huobio import HuobioGateway
from vnpy.gateway.okex import OkexGateway
from vnpy.gateway.okexf import OkexfGateway
from vnpy.gateway.okexs import OkexsGateway
from vnpy.gateway.okexo import OkexoGateway
from vnpy.gateway.bitmex import BitmexGateway
from vnpy.gateway.bybit import BybitGateway
from vnpy.gateway.gateios import GateiosGateway
from vnpy.gateway.deribit import DeribitGateway
from vnpy.gateway.bitfinex import BitfinexGateway
from vnpy.gateway.coinbase import CoinbaseGateway
from vnpy.gateway.bitstamp import BitstampGateway
from vnpy.gateway.onetoken import OnetokenGateway

from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_backtester import CtaBacktesterApp
from vnpy.app.spread_trading import SpreadTradingApp
from vnpy.app.algo_trading import AlgoTradingApp
from vnpy.app.portfolio_strategy import PortfolioStrategyApp
from vnpy.app.script_trader import ScriptTraderApp
from vnpy.app.market_radar import MarketRadarApp
from vnpy.app.chart_wizard import ChartWizardApp
from vnpy.app.excel_rtd import ExcelRtdApp
from vnpy.app.data_manager import DataManagerApp
from vnpy.app.data_recorder import DataRecorderApp
from vnpy.app.risk_manager import RiskManagerApp
from vnpy.app.portfolio_manager import PortfolioManagerApp
from vnpy.app.paper_account import PaperAccountApp


def main():
    """"""
    qapp = create_qapp()

    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    main_engine.add_gateway(BinanceGateway)
    main_engine.add_gateway(BinancesGateway)
    main_engine.add_gateway(HuobiGateway)
    main_engine.add_gateway(HuobifGateway)
    main_engine.add_gateway(HuobisGateway)
    main_engine.add_gateway(HuobioGateway)
    main_engine.add_gateway(OkexGateway)
    main_engine.add_gateway(OkexfGateway)
    main_engine.add_gateway(OkexsGateway)
    main_engine.add_gateway(OkexoGateway)
    main_engine.add_gateway(BitmexGateway)
    main_engine.add_gateway(BybitGateway)
    main_engine.add_gateway(GateiosGateway)
    main_engine.add_gateway(DeribitGateway)
    main_engine.add_gateway(BitfinexGateway)
    main_engine.add_gateway(CoinbaseGateway)
    main_engine.add_gateway(BitstampGateway)
    main_engine.add_gateway(OnetokenGateway)

    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(SpreadTradingApp)
    main_engine.add_app(AlgoTradingApp)
    main_engine.add_app(PortfolioStrategyApp)
    main_engine.add_app(ScriptTraderApp)
    main_engine.add_app(MarketRadarApp)
    main_engine.add_app(ChartWizardApp)
    main_engine.add_app(ExcelRtdApp)
    main_engine.add_app(DataManagerApp)
    main_engine.add_app(DataRecorderApp)
    main_engine.add_app(RiskManagerApp)
    main_engine.add_app(PortfolioManagerApp)
    main_engine.add_app(PaperAccountApp)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
