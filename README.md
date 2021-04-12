### 问题 **【交给罗丰决定】**
* project具体的要求，需要呈现什么东西，【参看第一节PPT】
* 时间问题，注意可交易时间，交易频率
* 时间问题，策略持续时间
* tick 级数据量，多币种的数据可能会很大，


### 有什么要做的
* 数据库 / crypto-tick-data / **【交给罗丰决定】**
    * Choice1：直接用api，即使运行，在服务器跑，【币安、OKEX、火币随意】
    * Choice2：挂载到本地服务器上，本地读取
    * 表头是不是只有 `Open, close, high, low, amount, ptc_change`
    * 历史数据+实时数据
    

* 系统架构
   * 完成各个组件，能有的尽量有（RMS必须有），对接实时数据实现真实（自负盈亏）或模拟交易
   * 下单系统（实现交易算法：TWAP VWAP 算法participation rate）、回测系统
   * 遵循开发、测试、production流程
   * <img src = "https://d1rwhvwstyk9gu.cloudfront.net/2019/11/Emergence-of-protocols.png">


* 策略 **【简单就好】**
    * 可以用现有的
    * 持仓时间小于一天
    * Choice1：单币种【以太坊趋势更新，择时交易】
    * Choice2：多币种【换仓，每个不同币种的仓位】
    * 简单的择时策略？机器学习？LSTM？简单的择时策略？
    * dictionary/json **输出**
        * 择时策略
        * 每次调仓，每个币种的比例 `{"BTC":0.2, "ENT":0.8, "LGC":-1}`
        * 每个币种的仓位   `{"BTC":20, "ENT":30, "LGC":0}`
        

* 回测：
    * TCA 【交易成本分析】
    * 详见课件 `交易所的费用，佣金，成本`
    * https://github.com/cuemacro/tcapy
    * 历史数据
    
    ```
    策略：dict/json    数据库：历史数据 
                \   /
                 回测
    ```

* GUI **能抄就抄**
    * CML / HTML / PyQt
    * 实时持仓表格
    * 收益曲线 
    * 关键的kpi `最大回撤，sharpe ratio，手续分...`
    * 能够实现实盘操作，手操
    * 接口 `json`
    * https://zhuanlan.zhihu.com/p/136821953
    
   * GUI模拟动态过程
       * 先生成全部调仓器的交易信息和回测数据，
       * 从生成的数据动态读取

   * 实盘
       * 实时爬取交易所数据？
       * 如何完成手动交易和策略的balance

* readme
* slides     
    
 
### 分工
* 不太清楚，罗丰能够提供什么【数据库、整体框架】
* 我们要做的分工
* 回测、GUI【周亚楠，葛方洲、何昭仪】
* 策略【孙鸾、赵庆】

### 下次任务
* 反馈一下自己部分遇到的问题
* 周日【等罗丰回来】
