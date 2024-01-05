import requests
import json
class financialReportAPITest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8000"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/financialReportApi/"
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

    def test_getYearFinancialReport(self,windcode):
        # 获取组合指标代码
        doMethod="getYearFinancialReport"
        params = {'doMethod':doMethod, 'windCode':windcode}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)
    def test_getQuarterFinancialReport(self,windcode):
        # 获取组合指标代码
        doMethod="getQuarterFinancialReport"
        params = {'doMethod':doMethod, 'windCode':windcode}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)
    def test_getQuarterandYearFinancialReport(self,windcode):
        # 获取组合指标代码
        doMethod="getQuarterandYearFinancialReport"
        params = {'doMethod':doMethod, 'windCode':windcode}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(rspdt['data'].keys())
        print(doMethod,"resp:",rspdt)

if __name__ == '__main__':
    pts = financialReportAPITest()
    # pts.login("rocky",'test1234')
    pts.login("jsr@163.com", 'test1234')
    pts.test_getYearFinancialReport('AAPL.O')
    pts.test_getQuarterFinancialReport('AAPL.O')
    pts.test_getQuarterandYearFinancialReport('AAPL.O')