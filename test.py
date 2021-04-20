from vnpy.event import EventEngine
from vnpy.gateway.huobi.huobi_gateway import HuobiGateway, HuobiRestApi

event_engine = EventEngine()
huobi_gateway = HuobiGateway(event_engine)
self = HuobiRestApi(huobi_gateway)
self.connect(**{'key': 'ffd51861-4c52736a-bgrdawsdsd-74b8b',
                      'secret': '61516638-ed48289c-1d3bc647-56a66',
                      'session_number': 3,
                      'proxy_host': '',
                      'proxy_port': ''})
