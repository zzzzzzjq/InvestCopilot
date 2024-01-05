import requests
import json
class factorUtilsAPITest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8000"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/factorUtilsApi/"
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
    def test_refreshCacheBtn(self):
        resp = self.session.post(self.ctxPath+"/api/refreshCacheBtn/",headers=self.headers,  timeout=(10, 20))
        print("refreshCacheBtn","resp:",resp.json())
    def test_get_factors_datas(self,templateNo,portfolioId):
        # 获取组合指标代码
        doMethod="get_factors_datas"
        params = {'doMethod':doMethod, 'templateNo':templateNo,"portfolioId":portfolioId}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def test_get_factors_Comparison_among_peers(self,WINDCODE):
        # 获取组合指标代码
        doMethod="get_factors_Comparison_among_peers"
        params = {'doMethod':doMethod, 'windCode':WINDCODE}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def test_get_factors_indexcode(self,INDEXCODE):
        # 获取组合指标代码
        doMethod="get_factors_indexcode"
        params = {'doMethod':doMethod, 'indexCode':INDEXCODE}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def test_get_factors_datas_1115(self,templateNo,stock_list):
        # 获取组合指标代码
        doMethod="get_factors_datas_1115"
        params = {'doMethod':doMethod, 'templateNo':templateNo,"stockList":stock_list}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)

def change_list_tostr():
    stock_seach = ['002735.SZ', 'FGBIP', '8187.HK', 'MNTA', 'THACU', '6663.HK','002461.SZ','1850.HK','HRMS.PA','LVMH.PA']
    # stock_seach = ['002735.SZ', 'FGBIP', '8187.HK', 'MNTA', 'THACU', '6663.HK']  # 测试用例1
    # stock_seach = ['6663.HK'] #测试用例2
    # stock_seach = ['002735.SZ'] #测试用例3
    # stock_seach = []  # 测试用例4
    stock_seach = '|'.join(stock_seach)
    return stock_seach

if __name__ == '__main__':
    pts = factorUtilsAPITest()
    # pts.login("rocky",'test1234')
    pts.login("18817781975@163.com", 'test1234')
    choice_num=5
    if choice_num==1:
        templateNo = '100_65'
        # 1.股票数据较多，有美股和其他股的 'self_23467'
        portfolioId='self_23467'
        # 2.只有中文股的 self_38
        # portfolioId ='self_38'
        # 3.只有美股的
        # portfolioId = 'self_38'
        # 4.没有股票数据的
        # 可以，返回data为空
        # 5.没有指标数据，有股票数据的，可以，是中文名
        # templateNo = '100_84'
        # portfolioId =''
        pts.test_get_factors_datas(templateNo,portfolioId)
        pts.test_get_factors_datas('100_46','self_23563')

    elif choice_num==2:
        templateNo = '100_65'
        stock_seach = change_list_tostr()
        print(stock_seach)
        pts.test_get_factors_datas_1115(templateNo,stock_list=stock_seach)
    elif choice_num==3:
        templateNo = '100_94'
        portfolioId = 'self_53'
        pts.test_get_factors_datas(templateNo, portfolioId)
    elif choice_num==4:
        windcode = 'AEON.A'
        windcode = 'AAPL.O'
        pts.test_get_factors_Comparison_among_peers(windcode)
    elif choice_num==5:
        INDEXCODE='41620'
        pts.test_get_factors_indexcode(INDEXCODE=INDEXCODE)

