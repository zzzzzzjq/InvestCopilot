

import requests



class portfolioAPITest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8000"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/portfolioApi/"
        self.headers = {"Accept": "application/json, text/plain, */*",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Accept-Language": "zh-CN,zh;q=0.9",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36", }

    def login(self, userName, passWord):
        self.loginUrl = self.ctxPath + "/login/"
        params = {'username': userName, 'userpassword': passWord}
        resp = requests.post(self.loginUrl, data=params, headers=self.headers, timeout=(10, 20))
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

    def refreshCacheBtn(self):
        resp = self.session.post(self.ctxPath+"/api/refreshCacheBtn/",headers=self.headers,  timeout=(10, 20))
        print("refreshCacheBtn","resp:",resp.json())

    def getPortfolios(self):
        doMethod="getPortfolios"
        # portfolioType="self"
        # params = {'doMethod':doMethod, 'portfolioType':portfolioType,}
        params = {'doMethod':doMethod}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        print(doMethod,"resp:",resp.json())
        return resp.json()
    def createPortfolio(self,portfolioName):
        doMethod="createPortfolio"
        params = {'doMethod':doMethod, 'portfolioName':portfolioName,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        rspdt = resp.json()
        print("resp:",rspdt)
        portfolioId =rspdt['data']['portfolioId']
        print("portfolioId:",portfolioId)
        return portfolioId
    def addPortfolioSymbol(self,symbols,portfolioId):
        doMethod="addPortfolioSymbol"
        params = {'doMethod':doMethod, 'symbols':symbols, 'portfolioId':portfolioId}
        print("params:",params)
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        rspdt = resp.json()
        print("resp:",rspdt)

    def createPortfolioAndAddoSymbol(self,portfolioName,symbols):
        doMethod="createPortfolioAndAddoSymbol"
        params = {'doMethod':doMethod, 'portfolioName':portfolioName,'symbols':symbols}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        rspdt = resp.json()
        print("resp:",rspdt)

    def getPortfolioStocks(self,portfolioId):
        doMethod="getPortfolioStocks"
        params = {'doMethod':doMethod, 'portfolioId':portfolioId,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s"%(doMethod,portfolioId),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def delPortfolioSymbol(self,symbol,portfolioId):
        doMethod="delPortfolioSymbol"
        params = {'doMethod':doMethod, 'portfolioId':portfolioId,}
        print("self.ctxUrl:",self.ctxUrl)
        # resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def delPortfolio(self,portfolioId):
        doMethod="delPortfolio"
        params = {'doMethod':doMethod, 'portfolioId':portfolioId,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def getPortfolioSymbols(self,portfolioId):
        doMethod="getPortfolioSymbols"
        params = {'doMethod':doMethod, 'portfolioId':portfolioId,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def addComments(self,sid,slable,scomments,rid):
        doMethod="addComments"
        params = {'doMethod':doMethod, 'slable':slable, 'scomments':scomments, 'sid':sid, 'rid':6,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def portfolioStocksSort(self):
        doMethod="portfolioStocksSort"
        symbols = ["FAST.O", "X.N","XRX.O", "ZTS.N", "JD.O", "EA.O", "AAOI.O"]
        symbols=",".join(symbols)
        print("symbols:",symbols)
        params = {'doMethod':doMethod, 'portfolioId':"self_35", 'symbols':symbols}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)

    def searchFactors(self,searchKey):
        doMethod="searchFactors"
        params = {'doMethod':doMethod, 'searchKey':searchKey,}
        doMethod="getMenuFactor"
        params = {'doMethod':doMethod, 'searchKey':searchKey,}
        # doMethod="getFactorByMenu"
        params = {'doMethod':doMethod, 'categoryId':"21024"}
        print("self.ctxUrl:",self.ctxUrl)
        doMethod="getFactorMenuCache"
        params = {'doMethod':doMethod, 'categoryId':"21024"}
        print("self.ctxUrl:",self.ctxUrl)
        ctxUrl = self.ctxPath + "/api/factorMenuApi/"
        resp = self.session.post(ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)

if __name__ == '__main__':

    pts = portfolioAPITest()
    # pts.login("robby.xia@baichuaninv.com",'test1234')
    # pts.login("rocky",'test1234')
    pts.login("jsr@163.com",'test1234')
    test_choice = "createandadd"
    if test_choice =="createandadd":
        symbols_list= ['AEE.N','300417.SZ','872869.BJ','VECO.O','000716.SZ','0700.HK']
        symbols = '|'.join(symbols_list)
        pts.createPortfolioAndAddoSymbol("测试apitest_02",symbols)

    #创建自选股组合
    # portfolioId=pts.try_test_createPortfolio("自选股组合10")
    # print("portfolioId:",portfolioId)
    # portfolioId = 'self_34'
    #查询自选股组合
    # pts.refreshCacheBtn()
    # data = pts.getPortfolios()
    # data = pts.getSymbolNewMarket()
    # # #添加自选股代码
    # symbols="600519.SH"
    # symbols="0700.HK"
    # symbols="LVMH.PA"
    # print('xx',data)
    # # print('xx',data['data'])
    # portfolioId=data['data']["portfolioFirst"]['portfolioId']snap
    # print("portfolioId: ",portfolioId)
    # tuserid = '001'
    # pts.addPortfolioSymbol("DASH.O",portfolioId)
    # # #
    # pts.try_test_getPortfolioStocks(portfolioId)
    # pts.test_delPortfolioSymbol('AAPL.O',portfolioId)
    # pts.getPortfolioStocks(portfolioId)
    # pts.test_delPortfolio(portfolioId)
    # pts.getPortfolioSymbols("self_35")
    # pts.portfolioStocksSort()
    # pts.getPortfolioSymbols("self_35")
    # pts.addComments("Audio_8600416cd7fcf64da8c4e40b0132debf",'Ns','portfolioxxId','6')
    # pts.searchFactors("cash")
