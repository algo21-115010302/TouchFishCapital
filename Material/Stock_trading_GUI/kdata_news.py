from urllib import request
import json


headers = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'
                      '68.0.3440.15 Safari/537.36'
}


def spider(code, blank='1'):
    price = []
    time = []
    url = r'http://push2.eastmoney.com/api/qt/stock/trends2/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13' \
          r'&fields2=f51,f52,f53,f54,f55,f56,f57,f58&ut=fa5fd1943c7b386f172d6893dbfba10b&ndays=1&iscr=0&secid' \
          r'='+blank+'.'+code

    req = request.Request(url, headers=headers)
    rsp = request.urlopen(req)
    jd = rsp.read().decode('utf-8')
    data = json.loads(jd)
    for i in range(len(data['data']['trends'])):
        temp = data['data']['trends'][i].split(',')
        time.append(temp[0].split(' ')[1])
        price.append(float(temp[2]))
    return time, price