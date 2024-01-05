import json

import requests
class portfolioAPITest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8009"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/stockApi/"
        self.headers = {
                        "Accept-Encoding": "gzip, deflate, br",
                        "Accept-Language": "zh-CN,zh;q=0.9",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36", }

    def login(self, userName, passWord):
        self.loginUrl = self.ctxPath + "/login/"
        params = {'username': userName, 'userpassword': passWord}
        resp = requests.post(self.loginUrl, data=json.dumps(params), headers=self.headers, timeout=(10, 20))
        print("resp:",resp.json())
        if resp.status_code == 200:
            rts =resp.json()
            if (rts['errorFlag']):
                v_cookies = resp.cookies.get_dict()
                print(v_cookies)
                requests_cookies = {}
                for kn, kv in v_cookies.items():
                    requests_cookies[kn] = kv
                session = requests.Session()
                session.cookies.update(requests_cookies)
                self.session = session
            else:
                raise Exception("登录失败[%s]" % rts)
        else:
            raise Exception("登录失败[%s]" % resp.status_code)

    def test_search(self,search,vName,flagAll):
        doMethod="search"
        params = {'doMethod':doMethod, 'search':search,'vName':vName,'flagAll':flagAll,}
        print("self.ctxUrl:",self.ctxUrl)
        print("params:",params)
        #"Content-Type": "application/json",

        # 设置请求头，告诉服务器发送的数据是 JSON 格式
        # qheaders = {'Content-Type': 'application/json'}
        # self.headers.update(qheaders)
        print("self.headers:",self.headers)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        # resp = self.session.post(self.ctxUrl,headers=self.headers,
        #                      json=params, timeout=(10, 20))
        # geturls=self.ctxUrl+"?doMethod=search&search=AA&vName=1&flagAll=ALL"
        # print("geturls:",geturls)
        # resp = self.session.get(geturls,headers=self.headers, timeout=(10, 20))
        print("resp:",resp.json())

    def getSymbolNewMarket(self):
        doMethod="getSymbolNewMarket"
        params = {'doMethod':doMethod, 'symbols':"AMD.O|600519.SH|0700.HK",}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def getStockInfo(self):
        doMethod="getStockInfo"
        params = {'doMethod':doMethod, 'symbol':"AMD.O",}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def getIndexInfo(self):
        doMethod="getIndexInfo"
        params = {'doMethod':doMethod, 'symbol':"AMD.O",}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
if __name__ == '__main__':
    pts = portfolioAPITest()
    # pts.login("robby.xia@baichuaninv.com", 'robbyxia!1')
    pts.login("robby.xia@baichuaninv.com", 'test1234')
    # 查询自选股组合
    # pts.test_search('AA','1','ALL')
    # pts.getSymbolNewMarket()
    # pts.getStockInfo()
    pts.getIndexInfo()



