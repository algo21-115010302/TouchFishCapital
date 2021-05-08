## Project Content Introduction 

This is a simple digital currency trading system based on the vnpy quantitative framework.   
This framework can be used to implement the following functions：  
* Live Trading Framework
* Backtesting Data Import
* Strategy writing and backtesting
* Algorithm Trading
* Newly added feature: TCA

## Run the framework
Run the run.py file in the TouchFishCapital folder, then the GUI of the framework will appear.





### 问题 **【交给罗丰决定】**
* project具体的要求，需要呈现什么东西，【参看第一节PPT】
* 时间问题，注意可交易时间，交易频率
* 时间问题，策略持续时间
* tick 级数据量，多币种的数据可能会很大，

### 有什么要做的
* 数据库 / crypto-tick-data / **【交给罗丰决定】**
    * Choice1：直接用api，即使运行，在服务器跑，【币安】
    * Choice2：挂载到本地服务器上，本地读取
    * 表头是不是只有 `Open, close, high, low, amount, ptc_change`
    

* 系统架构


* 策略 **【简单就好】**
    * Choice1：单币种【以太坊趋势更新，择时交易】
    * Choice2：多币种【换仓，每个不同币种的仓位】
    * 机器学习？LSTM？简单的择时策略？
    * dictionary/json **输出**
        * 择时策略
        * 每次调仓，每个币种的比例 `{"BTC":0.2, "ENT":0.8, "LGC":-1}`
        * 每个币种的仓位   `{"BTC":20, "ENT":30, "LGC":0}`
        

* 回测：
    * TCA 【交易成本分析】
    * 详见课件 `交易所的费用，佣金，成本`
    * 历史数据
    
    ```
    策略：dict/json    数据库：历史数据 
                \   /
                 回测
    ```

* GUI **能抄就抄**
    * HTML / PyQt
    * 实时持仓表格
    * 收益曲线 
    * 关键的kpi `最大回撤，sharpe ratio，手续分...`
    * 能够实现实盘操作，手操【附加，有时间加进去】
    * 接口 `json`
    * https://zhuanlan.zhihu.com/p/136821953
    
   * GUI模拟动态过程
       * 先生成全部调仓器的交易信息和回测数据，
       * 从生成的数据动态读取

   * 实盘
       * 实时爬取交易所数据？
       * 如何完成手动交易和策略的balance

* readme, slides     
    
 
### 分工
* 不太清楚，罗丰能够提供什么【数据库、整体框架】
* 我们要做的分工
* 回测、GUI【周亚楠，葛方洲、何昭仪】
* 策略【孙鸾、赵庆】

### 下次任务
* 反馈一下自己部分遇到的问题
* 周日【等罗丰回来】
