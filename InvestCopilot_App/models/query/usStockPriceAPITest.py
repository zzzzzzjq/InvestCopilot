import requests
import json
class usStockPriceAPITest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8000"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/usStockPriceApi/"
        self.headers = {"Accept": "application/json, text/plain, */*",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Accept-Language": "zh-CN,zh;q=0.9",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36", }

    def login(self, userName, passWord):
        self.loginUrl = self.ctxPath + "/login/"
        params = {'username': userName, 'userpassword': passWord}
        resp = requests.post(self.loginUrl,data=params, headers=self.headers, timeout=(10, 20))
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
    def test_getusStockPrice(self,windcode,startTime):
        # 获取组合指标代码
        doMethod="getusStockPrice"
        params = {'doMethod':doMethod, 'windCode':windcode,"startTime":startTime}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)
    def test_getInsiderData(self,windcode,startTime,page,pagesize):
        # 获取组合指标代码
        doMethod="getInsiderData"
        params = {'doMethod':doMethod, 'windCode':windcode,"startTime":startTime,"page":page, "pageSize": pagesize}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)
    def test_getusStockPriceChoice(self,windcode,startTime,col_name):
        # 获取组合指标代码
        doMethod="getusStockPriceChoice"
        params = {'doMethod':doMethod, 'windCode':windcode,"startTime":startTime,"col_name":col_name}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)
    def test_getusStockPriceCol(self,windcode):
        # 获取组合指标代码
        doMethod="getusStockPriceCol"
        params = {'doMethod':doMethod, 'windCode':windcode}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)
    def test_getusStockPrice_2(self,windcode):
        # 获取组合指标代码
        doMethod="getusStockPrice_2"
        params = {'doMethod':doMethod, 'windCode':windcode}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)

if __name__ == '__main__':
    pts = usStockPriceAPITest()
    # pts.login("rocky",'test1234')
    pts.login("18817781975@163.com", 'test12345678')
    pts.test_getInsiderData('METC.O',"20231124",3,30)
    # pts.test_getusStockPrice('AAPL.O',"20100101")
    # pts.test_getusStockPrice('AAPL.O',"")
    # pts.test_getusStockPriceChoice('AAPL.O',"","GGRG|FEF")
    # pts.test_getusStockPriceChoice('AAPL.O', "", "")
    # pts.test_getusStockPriceChoice('AAPL.O',"","s_dq_open|s_dq_close|s_dq_low|s_dq_high|s_dq_volume")
    # pts.test_getusStockPriceCol('AAPL.O')