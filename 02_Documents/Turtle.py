'''
60日最高买入，十日最低卖出
'''
#加载库函数
import numpy as np
import pandas as pd                   
import talib as ta                    # 加载技术分析库
from jqdatasdk import *
import datetime
#输入聚宽接口的账号和密码
auth('******','*******')

#初始化回测环境
start = '20200708'                    
end = '20201013'                      

#初始化策略参数:
Max_Position_per = 0.1             # 每只股票购入的最高比例为10%
max_history_window = 250           # 设置最长回测周期
Max_time_range  = 60               # 设置数据回顾周期
limit_unit = 4                     # 限制最多买入的单元数
atrlength = 20                     # 计算真实波幅考虑的周期数
DC_range  = 20                     # 计算DC通道考虑的周期数
trade_percent = 0.01               # 每次交易占总资产比例的基础值
record = pd.DataFrame({'symbol':[],'add_time':[],'last_buy_price':[]} )  #股票及对应的加仓次数和上一次买价 

class Account(object):
    """docstring for Account"""
    capital_base=0
    cash=0
    positions=pd.DataFrame()
    def __init__(self,capital_base):
        self.capital_base=capital_base
        self.cash=capital_base
    def order_to(self,sec,unit,price):
        if unit==0 or len(self.positions)>4:
            return False
        else:
            if unit==-1:
                #清仓 加钱减头寸
                print("清仓:",sec)
                self.cash=self.cash+self.positions['position'][sec]*price
                self.positions=self.positions.drop(index=sec)
            else:
                #减钱 加头寸
                self.cash=self.cash - unit*price
                if sec in self.positions.index:
                    #加仓
                    print("加仓:",sec)
                    punit=self.positions['position'][sec]+unit
                    self.positions=self.positions.drop(index=sec)
                    self.positions=self.positions.append(pd.DataFrame({"position":[punit]},index=[sec]))
                else:
                    #开仓
                    print("开仓:",sec)
                    punit=unit
                    self.positions=self.positions.append(pd.DataFrame({"position":[punit]},index=[sec]))
            return True
    def get_positions(self):
           return self.positions
    def get_money(self,timing_today):
        money=self.cash
        for sec in self.positions.index:
            price=get_price(sec, end_date=timing_today,
                    frequency='daily', fields='close', 
                    skip_paused=False, fq='pre', count=1, panel=True, fill_paused=True) 
            money=money+self.positions['position'][sec]*price['close'][0]
        return money 
    def portfolio_value(self):
        return self.capital_base
def calcUnit(portfolio_value,ATR):
    '''
     计算unit,注意股数为100的整数倍
    '''
    value = portfolio_value * trade_percent         
    return int((value/ATR)/100)*100                             

def timing_turtle(timing_today,account):
    #全局变量声明
    global record                                                                                 #声明record为局变量
    #数据获取（通用部分）：投资者账户，可供投资股票，价格数据，持仓数据，账户金额数据
   
    current_universe=get_index_stocks("000016.XSHG", date=timing_today)
    #history = context.history(current_universe,['closePrice','lowPrice','highPrice'],Max_time_range, rtype='array')   #构建金肯特纳交易系统时考虑过去Max_time_range的历史数据
    
    security_position = account.get_positions()                                                   #字典数据，上一K线结束后的有效证券头寸，即持仓数量大于0的证券及其头寸
    cash = account.cash                                                                           #获取股票账户可用于投资的现金额度
    #按照海龟交易系统循环处理所有证券池中股票
    x=0
    for sec in current_universe:                                                                  #遍历所有可供投资股票；注：如该策略执行了选股策略，该部分应遍历被选中股票     
    #数据获取（专有部分）：最低价，最高价，收盘价
        #print('sec:',sec)
        history=get_bars(sec,Max_time_range, unit='1d',
         fields=['date','high','low','close'],
         include_now=False, end_dt=timing_today, fq_ref_date=None,df=True)
        
        close = history['close']                                                        #获取股票sec过去Max_time_range天的收盘价
        low   = history['low']                                                           #获取股票sec过去Max_time_range天的最低价
        high  = history['high']                                                           #获取股票sec过去Max_time_range天的最高价
        #current_price = context.current_price(sec)
        #count不为1为None的话，会调取所以历史数据 
        current_price=get_price(sec, end_date=timing_today.fromordinal((timing_today.toordinal()-1)),
                    frequency='daily', fields='close', 
                    skip_paused=False, fq=None, count=1, panel=True, fill_paused=True)                                                  #获得当前时刻价格
    #数据处理（策略部分）：计算真实波幅，
        atr = ta.ATR(high, low, close, atrlength)[59]                                               #计算真实波幅   
    #策略部分(入场): 突破DC通道上轨，入场，买入1个单位的股
        if  current_price['close'][0]> high[:-1].max() and sec not in security_position.index:                                  # 收盘价上穿DC上轨，且无持仓（突破通道上轨阻力位）
    
            unit = calcUnit(account.portfolio_value(),atr)                                                               # 计算建仓时应购入的股票数量
            '''
            如果增设buy_price和sell_price可以消除部分滑点
            buy_price=get_price(sec, end_date=timing_today,frequency='daily', fields='open', 
                    skip_paused=False, fq='pre', count=1, panel=True, fill_paused=True)
            ''' 
            if account.order_to(sec,unit,current_price['close'][0]):                                                                                         # 买入unit股的sec股票
                if len(record)!=0:                                                                 
                    record = record[record['symbol']!=sec]                                                                  # 清空record中sec过期的记录                   
                record = record.append(pd.DataFrame({'symbol':[sec],'add_time':[1],'last_buy_price':[current_price['close'][0]]}))     # 记录股票，加仓次数及买入价格
            continue
           
        #策略部分(加仓)：若股价在上一次买入（或加仓）的基础上上涨了0.5N，则加仓1个单位的股票
        elif sec in security_position.index:                                                          # 判断是否已经持仓
            #策略部分(离场：止损或止盈)：当价格相对上个买入价下跌 2ATR时（止损）或当股价跌破10日唐奇安通道（止盈），清仓离场 
 
            if (record[record['symbol'] == sec]['last_buy_price'].empty):
                continue
            else:     
                last_price =float(record[record['symbol'] == sec]['last_buy_price'])             # 上一次的买入价格
            if current_price['close'][0]< low[-int(DC_range/2):-1].min() or current_price['close'][0] <(last_price - 2*atr):
                print("清仓")
                if account.order_to(sec,-1,current_price['close'][0]):                              # 清仓离场
                    record = record[record['symbol']!=sec]
                continue# 将卖出股票的记录清空      
            add_price =last_price + 0.5 * atr                                                # 计算是否加仓的判断价格
            add_unit = float(record[record['symbol'] == sec]['add_time'])                          # 已加仓次数
            if current_price['close'][0] > add_price and add_unit < limit_unit:                                # 价格上涨超过0.5N并且加仓次数小于4次
                print("加仓")
                unit = calcUnit(account.portfolio_value(),atr)                                        # 计算加仓时应购入的股票数量
                if account.order_to(sec,unit,current_price['close'][0]):                                                                     # 买入1unit的股票
                    record.loc[record['symbol']== sec,'add_time']=record[record['symbol']== sec]['add_time']+1   # 加仓次数+1
                    record.loc[record['symbol']== sec,'last_buy_price']=current_price['close'][0]                            # 加仓次数+1         
                

def main():
    timing_day=get_trade_days(start_date=start, end_date=end, count=None)
    account=Account(100000)
    dir="C:\\Users\\Walker\\Documents\\jcode\\"
    positions=pd.DataFrame()
    y=0
    for timing_today in timing_day:
        
        print("时间循环",timing_today)
        #执行海龟法则，把每日资产写入DataFrame
        timing_turtle(timing_today,account)
        positions=positions.append(pd.DataFrame({"capital":[account.get_money(timing_today)]},index=[timing_today.isoformat()]))   
        print(account.get_positions())
    positions.to_csv(dir+"p2.csv",encoding='utf-8')
    logout()

if __name__ == '__main__':
    main()

