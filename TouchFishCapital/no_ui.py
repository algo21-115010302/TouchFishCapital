from touch_fish.event import EventEngine
from touch_fish.utils.engine import MainEngine

from touch_fish.gateway.huobi import HuobiGateway
from touch_fish.gateway.huobif import HuobifGateway
from touch_fish.gateway.huobis import HuobisGateway
from touch_fish.gateway.huobio import HuobioGateway

from touch_fish.applications.cta_strategy import CtaStrategyApp
from touch_fish.applications.cta_backtester import CtaBacktesterApp
from touch_fish.applications.algo_trading import AlgoTradingApp
from touch_fish.applications.data_manager import DataManagerApp
from touch_fish.applications.data_recorder import DataRecorderApp


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
from touch_fish.utils.constant import Exchange
from touch_fish.utils.object import SubscribeRequest
req = SubscribeRequest(symbol='massbtc', exchange=Exchange.HUOBI)
main_engine.subscribe(req, gateway_name='HUOBI')
# 第二步：获取tick数据
# touch_fish.utils.engine.OmsEngine.process_tick_event是tick数据更新的回调函数,要对这个函数做修改从而保存数据
main_engine.get_engine('oms').ticks

# 查询order（委托）
main_engine.get_engine('oms').orders

# 查询trades（成交）
main_engine.get_engine('oms').trades

# 查询positions（持仓）
main_engine.get_engine('oms').positions

# 查询account（资金）
main_engine.get_engine('oms').accounts




