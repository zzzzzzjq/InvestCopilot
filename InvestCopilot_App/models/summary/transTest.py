
import requests
#"https://www.daohequant.com/mobileApp/api/informationApi/",
ctxPath="http://127.0.0.1:8009/mobileApp/api/informationApi/"

class summaryTest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8009"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/transApi/"
        self.headers = {"Accept": "application/json, text/plain, */*",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Accept-Language": "zh-CN,zh;q=0.9",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36", }


    def test_refreshCacheBtn(self):
        resp = self.session.post(self.ctxPath+"/api/refreshCacheBtn/",headers=self.headers,  timeout=(10, 20))
        print("refreshCacheBtn","resp:",resp.json())


    def login(self, userName, passWord):
        self.loginUrl = self.ctxPath + "/login/"
        params = {'username': userName, 'userpassword': passWord}
        resp = requests.post(self.loginUrl, data=params, headers=self.headers, timeout=(10, 20))
        print("resp:", resp.json())
        if resp.status_code == 200:
            rts = resp.json()
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

    def uploadTransFile(self):
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "uploadTransFile"
        from requests_toolbelt.multipart.encoder import MultipartEncoder
        data={
                 'doMethod': doMethod,
                 'language': "en",
                 "transTitle":"测试翻译文档3"
                   }
        # 使用 MultipartEncoder 构建请求体
        multipart_data = MultipartEncoder(
            # fields={  'file': ('Intellistock Service Agreement -20231228.docx', open(r'c:/Users/env/Downloads/Intellistock Service Agreement -20231228.docx', 'rb')) , **data}
            # fields={  'file': ('Intellistock Service Agrexxxement -20231228.txt', open(r'c:/Users/env/Downloads/Intellistock Service Agreement -20231228.text', 'rb')) , **data}
            fields={  'file': ('90f31bdc218b225c0a0026b612c0a300-eng.pdf', open(r'c:/Users/env/Downloads/90f31bdc218b225c0a0026b612c0a300-eng.pdf', 'rb')) , **data}
        )
        headers = {'Content-Type': multipart_data.content_type }
        resp = self.session.post(self.ctxUrl,data=multipart_data, headers=headers)
        print(doMethod,resp.json())

    def delTransData(self):
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "delTransData"
        resp = self.session.post(self.ctxUrl,
                             data={
                                 'doMethod': doMethod,
                                 'cid': "Trans_6204da813020b9195d70e8e9bc5c9fcc",
                                   })
        print(doMethod,resp.json())
    def getTransSummary(self):
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "getTransSummary"
        resp = self.session.post(self.ctxUrl,
                             data={
                                 'doMethod': doMethod,
                                 'myTrans': "1",
                                 'translation': "zh",
                                 "pageSize":200,
                                   })
        print(doMethod,resp.json())
if __name__ == '__main__':
    st = summaryTest()
    # test_stockpools()
    # test_getNewsContents_v1()
    # st.login("robby.xia@baichuaninv.com",'robbyxia!1')
    st.login("robby.xia@baichuaninv.com",'test1234')
    # st.login("testrobby2@126.com",'test1234')
    # st.login("dongmin.xue@njjhtz.com",'co000111')
    # st.login("dongmin.xue@njjhtz.com",'co000111')
    # st.login("rocky_zs@pinnacle-cap.cn",'daohequant1202')
    # st.login("liu.z.gavin@gmail.com",'v1s6VXGE') #visitFlag
    # st.login("testrobby3@126.com",'test1234') #visitFlag

    # st.login("robby.xia@pinnacle-cap.cn",'test1234')
    # st.login("testrobby@126.com",'test1234')
    # st.test_refreshCacheBtn()
    rest=st.uploadTransFile()
    # rest=st.delTransData()
    # rest=st.getTransSummary()