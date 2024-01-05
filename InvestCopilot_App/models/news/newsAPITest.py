
import requests
#"https://www.daohequant.com/mobileApp/api/informationApi/",
ctxPath="http://127.0.0.1:8009/mobileApp/api/informationApi/"

class newsApiTest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8009"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/newsApi/"
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

    def getNewsTitleTypes(self):
        doMethod = "getNewsTitleTypes"
        resp = requests.post(self.ctxUrl,
                             data={'doMethod':doMethod,
                                   })

        print(doMethod, resp.json())
    def getNewsClassTypes(self):
        doMethod = "getNewsClassTypes"
        resp = requests.post(self.ctxUrl,
                             data={'doMethod':doMethod,
                                   })

        print(doMethod, resp.json())
    def getNewsDataByTitleTag(self):
        doMethod = "getNewsDataByTitleTag"
        resp = requests.post(self.ctxUrl,
                             data={
                                 'doMethod':doMethod,
                                 # 'searchTitle':"京东集团",
                                 # 'titleTag':"chinese_consumer",
                                 # 'titleTag':"sg_SPV",
                                 'titleTag':"ai_software",
                                 # 'titleTag':"all_title",
                                 # 'news_class':"Earnings",
                                "translation" : "zh",
                                # "ordTag" : "total_title_score",
                                # "news_class" : "Industry trends",
                                # "beginDate" : "2023-12-11",
                                # "endDate" : "2023-12-11",
                                "gtScore" : 0,
                                "ltScore" : 10,
                                 'page':1,
                                 'pageSize':200,
                                   })

        print(doMethod, resp.json())
if __name__ == '__main__':
    st = newsApiTest()
    # test_stockpools()
    # test_getNewsContents_v1()
    # st.login("robby.xia@baichuaninv.com",'robbyxia!1')
    st.login("robby.xia@baichuaninv.com",'test1234')
    # st.login("Ulyssess-liu@outlook.com",'v6Tz0sGv')
    # st.login("rocky",'test1234')  
    #Ulyssess-liu@outlook.com
    #9aTqfBDZn7hjBnMxxPQpqQ==
    #9aTqfBDZn7hjBnMxxPQpqQ==
    #Ulyssess-liu@outlook.com

    # st.login("testrobby@126.com",'test1234')
    # st.test_refreshCacheBtn()
    # st.getNewsTitleTypes()
    st.getNewsDataByTitleTag()
    # st.getNewsClassTypes()