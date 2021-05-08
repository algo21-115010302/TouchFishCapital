from PyQt5 import QtCore, QtGui
import sip
from PyQt5.QtWidgets import (QWidget, QLabel, QHBoxLayout)


class Favorite(QWidget):

    def __init__(self, code, price):
        super(Favorite, self).__init__()
        self.name = code
        self.lbl_1 = QLabel(str(price))
        self.lbl_2 = QLabel(self.name)
        self.lbl_2_t = QLabel('代码：')
        self.lbl_1_t = QLabel('现价：')
        self.__temp = 0
        self.lbl_2.setStyleSheet('font-size:20px;')
        self.lbl_2_t.setStyleSheet('font-size:15px;')
        self.lbl_1_t.setStyleSheet('font-size:15px;')
        if float(price) > self.__temp:
            self.lbl_1.setStyleSheet('font-size:25px;color:#FF0000;')
        else:
            self.lbl_1.setStyleSheet('font-size:25px;color:#00FF00;')
        self.__temp = float(price)

        self.outlayer = QHBoxLayout()
        self.outlayer.addWidget(self.lbl_2_t)
        self.outlayer.addWidget(self.lbl_2)
        self.outlayer.addWidget(self.lbl_1_t)
        self.outlayer.addWidget(self.lbl_1)
        self.setLayout(self.outlayer)
