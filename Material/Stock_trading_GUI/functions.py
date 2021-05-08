from PyQt5.QtWidgets import QWidget, QApplication, QVBoxLayout, QMainWindow, QSpacerItem, QSizePolicy, QScrollArea
from Treasure import Ui_MainWindow
from PyQt5.QtCore import Qt, pyqtSlot, QTimer, QPointF, QRectF
from PyQt5.QtGui import QCursor, QPainter, QPicture
import pyqtgraph as pg
import tushare as ts
import sys
import kdata_news
import datetime
import traceback
import favorite_part as fp
import sip


class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.setWindowFlags(Qt.FramelessWindowHint)
        self.setStyleSheet(Stylesheet)

        # self.k_plt = pg.PlotWidget()
        # self.kLayout.addWidget(self.k_plt)
        #
        # self.trade = pg.PlotWidget()
        # self.tradeLayout.addWidget(self.trade)
        self.code_g_tu = 'sh000001'
        self.code_g_me = '000001'

        self.choose_my = {}

        self.graphWidget = pg.PlotWidget(enableAutoRange=True)
        self.ui.divideLayout.addWidget(self.graphWidget)
        self.realTime()
        self.realTime_label()
        self.realTime_refresh_lbl()
        self.realTime_button()
        self.t, self.y = kdata_news.spider(self.code_g_me)  # 打开默认上证指数
        self.price_label()
        self.divide_blank()

        pen = pg.mkPen(color='w')
        self.g = self.graphWidget.plot(self.y, pen=pen)

        self.ui.favoriteLayout.setAlignment(Qt.AlignTop)  # 让label置顶
        self.ui.deleteButton.clicked.connect(self.deletelayout)
        self.ui.plusButton.clicked.connect(self.addlayout)

        self.ui.searchButton.clicked.connect(self.get_code)
        self.ui.aStock.clicked.connect(self.display_line)
        self.ui.aStock.click()

    def deletelayout(self):
        try:
            propose = self.choose_my[self.code_g_me]
            self.ui.favoriteLayout.removeWidget(propose)
            sip.delete(propose)  # 接下来写小部件大小和添加删除
            del self.choose_my[self.code_g_me]
        except Exception as a:
            print(a)

    def refresh(self):
        try:
            for i in self.choose_my:
                _, price = kdata_news.spider(i)
                v = self.choose_my[i]
                v.lbl_1.setText(str(price[-1]))
                QApplication.processEvents()
        except Exception as a:
            print(a)

    def addlayout(self):
        try:
            _, price_lbl = kdata_news.spider(self.code_g_me)
            item = fp.Favorite(self.code_g_me, price_lbl[-1])
            self.ui.favoriteLayout.addWidget(item)
            self.choose_my[self.code_g_me] = item
        except Exception as a:
            print(a)

    def check_exist_choose(self):
        if self.code_g_me in self.choose_my:
            self.ui.plusButton.setEnabled(False)
            self.ui.deleteButton.setEnabled(True)
        else:
            self.ui.plusButton.setEnabled(True)
            self.ui.deleteButton.setEnabled(False)

    @pyqtSlot()
    def on_closeButton_clicked(self):
        """
        关闭窗口
        """
        self.close()

    @pyqtSlot()
    def on_minimizeButton_clicked(self):
        """
        最小化窗口
        """
        self.showMinimized()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.m_flag = True
            self.m_Position = event.globalPos() - self.pos()  # 获取鼠标相对窗口的位置
            event.accept()
            # self.setCursor(QCursor(Qt.OpenHandCursor))  # 更改鼠标图标

    def mouseMoveEvent(self, QMouseEvent):
        if Qt.LeftButton and self.m_flag:
            self.move(QMouseEvent.globalPos() - self.m_Position)  # 更改窗口位置
            QMouseEvent.accept()

    def mouseReleaseEvent(self, QMouseEvent):
        self.m_flag = False
        self.setCursor(QCursor(Qt.ArrowCursor))

    def update_plot_data(self):
        try:
            num = len(self.y)
            self.t, self.y = kdata_news.spider(self.code_g_me)
            if len(self.y) > num:
                self.y.append(self.y[:-1])  # Add a new random value.
                self.g.setData(self.y[:-1])  # Update the data.
                print(self.g)
        except Exception as a:
            print(a)

    def get_code(self):
        try:
            self.code_g_tu = self.ui.searchContent.text()
            self.code_g_me = self.code_g_tu
            self.price_label()
            if self.ui.szButton.isChecked():
                self.t, self.y = kdata_news.spider(self.code_g_me, blank='0')
                self.query_slot(self.code_g_tu)
            else:
                self.t, self.y = kdata_news.spider(self.code_g_me)
                self.query_slot(self.code_g_tu)
            self.graphWidget.clear()
            pen = pg.mkPen(color='w')
            self.g = self.graphWidget.plot(self.y, pen=pen)
        except Exception as a:
            print(a)

    def display_line(self):
        try:
            self.query_slot()
            self.code_g_me = '000001'
            self.t, self.y = kdata_news.spider('000001')
            self.graphWidget.clear()
            pen = pg.mkPen(color='w')
            print(self.y)
            self.g = self.graphWidget.plot(self.y, pen=pen)
        except Exception as a:
            print(a)

    def divide_blank(self):
        self.graphWidget.showGrid(x=False, y=True)
        self.graphWidget.setBackground('k')
        self.axis_dict = dict(enumerate(self.t))
        axis_1 = [(i, self.t[::][i]) for i in range(0, len(self.t), 3)]  # 获取日期值
        axis_2 = [(i, self.t[::][i]) for i in range(0, len(self.t), 5)]
        axis_3 = [(i, self.t[::][i]) for i in range(0, len(self.t), 8)]
        axis_4 = [(i, self.t[::][i]) for i in range(0, len(self.t), 10)]
        axis_5 = [(i, self.t[::][i]) for i in range(0, len(self.t), 30)]
        stringaxis = pg.AxisItem(orientation='bottom')  # 创建一个刻度项
        stringaxis.setTicks([axis_5, axis_4, axis_3, axis_2, axis_1, self.axis_dict.items()])  # 设置X轴刻度值
        self.graphWidget.getAxis("bottom").setTicks([axis_5, axis_4, axis_3, axis_2, axis_1, self.axis_dict.items()])

    def realTime(self):
        self.timer = QTimer()
        self.timer.setInterval(11000)
        self.timer.timeout.connect(self.update_plot_data)
        self.timer.start()

    def realTime_label(self):
        self.timer_2 = QTimer()
        self.timer_2.setInterval(5000)
        self.timer_2.timeout.connect(self.price_label)
        self.timer_2.start()

    def realTime_button(self):
        self.timer_3 = QTimer()
        self.timer_3.setInterval(500)
        self.timer_3.timeout.connect(self.check_exist_choose)
        self.timer_3.start()

    def realTime_refresh_lbl(self):
        self.timer_4 = QTimer()
        self.timer_4.setInterval(5000)
        self.timer_4.timeout.connect(self.refresh)
        self.timer_4.start()

    def price_label(self):
        today_day = ts.get_realtime_quotes(self.code_g_tu)
        self.ui.price.setText(list(today_day['price'])[0])
        self.ui.highprice.setText(list(today_day['high'])[0])
        self.ui.lowprice.setText(list(today_day['low'])[0])
        self.ui.openprice.setText(list(today_day['open'])[0])
        self.ui.tradeprice.setText(list(today_day['volume'])[0])
        self.ui.amount.setText(list(today_day['amount'])[0])

    def query_slot(self, s_code='sh000001'):
        try:
            self.move_slot = pg.SignalProxy(self.ui.k_plt.scene().sigMouseMoved, rateLimit=60, slot=self.print_slot)
            start_date = datetime.datetime.today() - datetime.timedelta(days=int('180') + 1)
            start_date_str = datetime.datetime.strftime(start_date, "%Y-%m-%d")
            end_date = datetime.datetime.today() - datetime.timedelta(days=1)
            end_date_str = datetime.datetime.strftime(end_date, "%Y-%m-%d")
            self.plot_k_line(code=s_code, start=start_date_str, end=end_date_str)
        except Exception as e:
            print(traceback.print_exc())

    def plot_k_line(self, code=None, start=None, end=None):
        self.data = ts.get_hist_data(code=code, start=start, end=end).sort_index()
        y_min = self.data['low'].min()
        y_max = self.data['high'].max()
        data_list = []
        data_t_list = []
        d = 0
        for dates, row in self.data.iterrows():
            # 将时间转换为数字
            # date_time = datetime.datetime.strptime(dates, '%Y-%m-%d')
            # t = date2num(date_time)
            open, high, close, low = row[:4]
            ma5 = row['ma5']
            ma10 = row['ma10']
            ma20 = row['ma20']
            trade = row['volume']
            datas = (d, open, close, low, high, ma5, ma10, ma20)
            t_datas = (d, open, close, trade)
            data_list.append(datas)
            data_t_list.append(t_datas)
            d += 1
        self.axis_dict = dict(enumerate(self.data.index))
        axis_1 = [(i, list(self.data.index)[i]) for i in range(0, len(self.data.index), 3)]  # 获取日期值
        axis_2 = [(i, list(self.data.index)[i]) for i in range(0, len(self.data.index), 5)]
        axis_3 = [(i, list(self.data.index)[i]) for i in range(0, len(self.data.index), 8)]
        axis_4 = [(i, list(self.data.index)[i]) for i in range(0, len(self.data.index), 10)]
        axis_5 = [(i, list(self.data.index)[i]) for i in range(0, len(self.data.index), 30)]
        stringaxis = pg.AxisItem(orientation='bottom')  # 创建一个刻度项
        stringaxis.setTicks([axis_5, axis_4, axis_3, axis_2, axis_1, self.axis_dict.items()])  # 设置X轴刻度值
        self.ui.k_plt.getAxis("bottom").setTicks([axis_5, axis_4, axis_3, axis_2, axis_1, self.axis_dict.items()])
        self.ui.k_plt.plotItem.clear()  # 清空绘图部件中的项
        item = CandlestickItem(data_list)  # 生成蜡烛图数据
        item_2 = TradeItem(data_t_list)
        self.ui.k_plt.addItem(item)  # 在绘图部件中添加蜡烛图项目
        self.ui.k_plt.showGrid(x=False, y=True)  # 设置绘图部件显示网格线
        self.ui.k_plt.setYRange(y_min, y_max)
        self.label = pg.TextItem()  # 创建一个文本项
        self.ui.k_plt.addItem(self.label)  # 在图形部件中添加文本项
        self.vLine = pg.InfiniteLine(angle=90, movable=False, )  # 创建一个垂直线条
        self.hLine = pg.InfiniteLine(angle=0, movable=False, )  # 创建一个水平线条
        self.ui.k_plt.addItem(self.vLine, ignoreBounds=True)  # 在图形部件中添加垂直线条
        self.ui.k_plt.addItem(self.hLine, ignoreBounds=True)  # 在图形部件中添加水平线条
        self.ui.trade.getAxis("bottom").setTicks([axis_5, axis_4, axis_3, axis_2, axis_1, self.axis_dict.items()])
        self.ui.trade.plotItem.clear()
        self.ui.trade.addItem(item_2)
        self.ui.trade.showGrid(x=False, y=True)

    def print_slot(self, event=None):
        if event is None:
            print("事件为空")
        else:
            pos = event[0]  # 获取事件的鼠标位置
            try:
                # 如果鼠标位置在绘图部件中
                if self.ui.k_plt.sceneBoundingRect().contains(pos):
                    mousePoint = self.ui.k_plt.plotItem.vb.mapSceneToView(pos)  # 转换鼠标坐标
                    index = int(mousePoint.x())  # 鼠标所处的X轴坐标
                    pos_y = int(mousePoint.y())  # 鼠标所处的Y轴坐标
                    if -1 < index < len(self.data.index):
                        # 在label中写入HTML
                        self.label.setHtml(
                            "<p style='color:white'><strong>日期：{0}</strong></p><p style='color:white'>开盘：{1}</p>"
                            "<p style='color:white'>收盘：{2}</p><p style='color:white'>最高价：<span style='color:red;"
                            "'>{3}</span></p><p style='color:white'>最低价：<span style='color:green;'>{4}</span>"
                            "</p>".format(
                                self.axis_dict[index], self.data['open'][index], self.data['close'][index],
                                self.data['high'][index], self.data['low'][index]))
                        self.label.setPos(mousePoint.x(), mousePoint.y())  # 设置label的位置
                    # 设置垂直线条和水平线条的位置组成十字光标
                    self.vLine.setPos(mousePoint.x())
                    self.hLine.setPos(mousePoint.y())
            except Exception as e:
                print(traceback.print_exc())


class CandlestickItem(pg.GraphicsObject):
    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data  # data里面必须有以下字段: 时间, 开盘价, 收盘价, 最低价, 最高价, ma5
        self.generatePicture()  # 绑定按钮点击信号

    def generatePicture(self):
        prema5 = 0
        prema10 = 0
        prema20 = 0
        self.picture = QPicture()  # 实例化一个绘图设备
        p = QPainter(self.picture)  # 在picture上实例化QPainter用于绘图
        w = (self.data[1][0] - self.data[0][0]) / 3
        for (t, open, close, min, max, ma5, ma10, ma20) in self.data:
            p.setPen(pg.mkPen('w'))  # 设置画笔颜色
            # print(t, open, close, min, max)
            p.drawLine(QPointF(t, min), QPointF(t, max))  # 绘制线条
            if open > close:  # 开盘价大于收盘价
                p.setBrush(pg.mkBrush('g'))  # 设置画刷颜色为绿
            else:
                p.setBrush(pg.mkBrush('r'))  # 设置画刷颜色为红
            p.drawRect(QRectF(t - w, open, w * 2, close - open))  # 绘制箱子
            if prema5 != 0:
                p.setPen(pg.mkPen('r'))
                p.drawLine(QPointF(t-1, prema5), QPointF(t, ma5))
            prema5 = ma5
            if prema10 != 0:
                p.setPen(pg.mkPen('c'))
                p.drawLine(QPointF(t-1, prema10), QPointF(t, ma10))
            prema10 = ma10
            if prema20 != 0:
                p.setPen(pg.mkPen('m'))
                p.drawLine(QPointF(t-1, prema20), QPointF(t, ma20))
            prema20 = ma20
        p.end()

    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        return QRectF(self.picture.boundingRect())


class TradeItem(pg.GraphicsObject):
    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data  # data里面必须有以下字段: 时间, 开盘价, 收盘价, 交易量
        self.generatePicture()  # 绑定按钮点击信号

    def generatePicture(self):
        self.picture = QPicture()  # 实例化一个绘图设备
        p = QPainter(self.picture)  # 在picture上实例化QPainter用于绘图
        w = (self.data[1][0] - self.data[0][0]) / 3
        for (t, open, close, trade) in self.data:
            p.setPen(pg.mkPen('w'))  # 设置画笔颜色
            p.drawLine(QPointF(t, 0), QPointF(t, trade/10000))  # 绘制线条
            if open > close:  # 开盘价大于收盘价
                p.setBrush(pg.mkBrush('g'))  # 设置画刷颜色为绿
            else:
                p.setBrush(pg.mkBrush('r'))  # 设置画刷颜色为红
            p.drawRect(QRectF(t - w, 0, w * 2, trade/10000))  # 绘制箱子
        p.end()

    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)

    def boundingRect(self):
        return QRectF(self.picture.boundingRect())


Stylesheet = """
#MainWindow {
    background: #ffffff;
}
#leftwidget {
    background: #2894ff;
}
#plusButton {
    border-radius:10px;
}
#searchButton {
    border-radius:10px;
}

"""


if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())
