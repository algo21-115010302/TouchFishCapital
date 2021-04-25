from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine

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
from vnpy.app.data_manager import DataManagerApp
from vnpy.app.data_recorder import DataRecorderApp
from vnpy.app.risk_manager import RiskManagerApp
from vnpy.app.portfolio_manager import PortfolioManagerApp
from vnpy.app.paper_account import PaperAccountApp


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
main_engine.add_app(DataManagerApp)
main_engine.add_app(DataRecorderApp)
main_engine.add_app(RiskManagerApp)
main_engine.add_app(PortfolioManagerApp)
main_engine.add_app(PaperAccountApp)

main_engine.connect(setting={'API Key': 'ffd51861-4c52736a-bgrdawsdsd-74b8b',
                             'Secret Key': '61516638-ed48289c-1d3bc647-56a66',
                             '会话数': 3,
                             '代理地址': '',
                             '代理端口': ''},
                    gateway_name='HUOBI'
                    )

# 查询合约
# 字典,key为币名.交易所，value是contract信息
main_engine.get_engine('oms').contracts

# 查询tick数据
# 第一步: 订阅数据
from vnpy.trader.constant import Exchange
from vnpy.trader.object import SubscribeRequest
req = SubscribeRequest(symbol='massbtc', exchange=Exchange.HUOBI)
main_engine.subscribe(req, gateway_name='HUOBI')
# 第二步：获取tick数据
# vnpy.trader.engine.OmsEngine.process_tick_event是tick数据更新的回调函数,要对这个函数做修改从而保存数据
main_engine.get_engine('oms').ticks

# 查询order（委托）
main_engine.get_engine('oms').orders

# 查询trades（成交）
main_engine.get_engine('oms').trades

# 查询positions（持仓）
main_engine.get_engine('oms').positions

# 查询account（资金）
main_engine.get_engine('oms').accounts




