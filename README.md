## Project Content Introduction 

This is a simple digital currency trading system based on the vnpy quantitative framework.   
This framework can be used to implement the following functions：  
* Live Trading Framework
* Backtesting Data Import
* Strategy writing and backtesting
* Algorithm Trading
* Newly added feature: TCA
* Contract Search

## Run the framework
Run the run.py file in the TouchFishCapital folder, then the GUI of the framework will appear.  
Click the System button to connect to your personal account and exchange.  
The main screen is displayed as follows:  

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/framework%201.png)

## Backtesting Data Import
Click the Data Management button in the main interface to enter the upload data interface.   
Select the local csv data file, fill in the parameters and the table header corresponding to each column of data, and click OK.  

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/import%20data.png)
![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/import%20data%20successfully.png)

After refreshing the interface, click View and enter the time period you want to view the data, you can see the uploaded data in the right interface. 

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/view%20data.png)

Make sure the upload data is successful.

## Strategy writing and backtesting

Click the backtest button in the main interface to enter the backtest interface.    
Here you can select the written strategy file, set the backtesting time period, commission, slippage, etc.  

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/backtest.png)

Enter the required parameters for the selected strategy and start backtesting.   

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/backtest%20parameters.png)

After the backtest is finished, the interface will automatically display the backtest results and you can see the money curve, return indicator, etc. for the strategy in the selected time period.   

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/backtest%20result%201.png)
![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/backtest%20result%202.png)

## Algorithm Trading

Click the Algorithmic Trading button on the main screen to enter the interface for setting up algorithmic trading.     

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/algorithm%20trading%201.png)

After setting up the currency, exchange, algorithm and trading rules to be traded, the system will automatically perform live trading according to the settings.  

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/algorithm%20trading%202.png)

Orders submitted for algorithmic trading and successful orders are displayed in the order and trade windows of the main interface, and real-time changes to the account are reflected in the account window.   
If the transaction is unsuccessful, a detailed log will be displayed in the log window.   

## Newly added feature: TCA

After a successful transaction, the order and trade are displayed as shown：   

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/tca%202.png)
![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/tca%203.png)
![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/trading%20result.png)
![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/tca.png)

 For TCA, two columns, Transaction Cost and Slippage, have been added to the interface to monitor the non-fixed costs incurred when each order is filled.   
 
 ## Contract Search
 
In addition, detailed inquiries about the contract can be made on the main screen. You can search for all contracts by clicking on the query in help and leaving the search field empty.  

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/query%201.png)

It is also possible to perform a search for contracts dedicated to a particular exchange： 

![image](https://github.com/algo21-115010302/TouchFishCapital/blob/main/GUI%20pngs/query%202.png)

## Team Members

* Zhou Yanan - GUI and backtesting
* Luo Feng - Live trading and backtesting framework
* He Zhaoyi - Strategy and TCA
* Ge Fangzhou - GUI and backtesting
* Zhao Qing - Strategy and presentation

