
import requests
#"https://www.daohequant.com/mobileApp/api/informationApi/",
ctxPath="http://127.0.0.1:8009/mobileApp/api/informationApi/"

class summaryTest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8009"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/portfolioApi/"
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

    def test_stockpools(self):
        # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
        resp = requests.post(ctxPath,
                             data={'doMethod':"getStockPool",
                                   # "preDays":-1,
                                   #  "nextDays":0,
                                   "trackFlag" : "1",
                                   "dataType" : "sAts_",
                                   #"symbol":'AAPL.O',#'EXPE.O','MNST.O','BKNG.O'
                                   })
        print( len(resp.json()['fsymbols'] ),resp.json()['fsymbols'] )
        print( len(resp.json()['trackCodes'] ),resp.json()['trackCodes'] )
        print( len(resp.json()['allCodes'] ),resp.json()['allCodes'] )

    def test_getNewsContents_v1(self):
        resp = requests.post(ctxPath,data={'doMethod':"getNewsContents_v1",'id':"AInew_bb1acea9c4f2b5bf683161915836254a"})
        print(resp.json())
    def test_getDocContent(self):
        ctxPath = self.ctxPath +"/api/summaryApi/"
        doMethod = "getDocContent"
        resp = requests.post(ctxPath,
                             data={'doMethod': doMethod,
                                   # 'id':"Audio_9a68601200b69f712e4ad5041af9443a",
                                   # 'id':"Audio_8600416cd7fcf64da8c4e40b0132debf",
                                   'id':"Trans_b386288bf76168cceeba4966e41ab931",
                                   # 'id':"research_jf408555",
                                   # 'id':"company_c8c060d284d26bd1c506370be55f1ab7",
                                   # 'id':"jpmorgan_GPS-4565007-0",
                                   # 'vSourceContents':"0",
                                   # 'secondType':"fanyi",
                                   'translation':"zh"})
        print(doMethod,resp.json())
        return resp
    def test_getViewSummaryTitleByType(self):
        ctxPath = self.ctxPath +"/api/summaryApi/"
        doMethod = "getViewSummaryTitleByType"
        resp = self.session.post(ctxPath,
                             data={'doMethod': doMethod,
                                   "symbols":"BABA.O,AAPL.O,NVDA.O",
                                   # "symbols":"9618.HK",
                                   # "portfolioId":"self_23563",
                                   # "portfolioId":"allHoldings",
                                   # 'dataTypes':"Audio",
                                   'dataTypes':"research",
                                   # 'dataTypes':"transcripts",
                                   # 'dataTypes':"news",
                                   'translation':"zh"})
        print(doMethod,resp.json())
    def getViewSummaryByTag(self):
        ctxPath = self.ctxPath +"/api/summaryApi/"
        doMethod = "getViewSummaryByTag"
        resp = self.session.post(ctxPath,
                             data={'doMethod': doMethod,
                                   # "symbols":"BABA.O,AAPL.O,NVDA.O",
                                   # "symbols":"TSLA.O",
                                   "portfolioId":"self_23563",
                                   # "portfolioId":"allHoldings",
                                   # 'dataTypes':"Audio",
                                   # 'dataTypes':"research",
                                   # 'dataTypes':"transcripts",
                                   "pageSize":5,
                                   'dataTypes':"news",
                                   'translation':"zh"})
        print(doMethod,resp.json())
    def getStrategySumViewByTag(self):
        ctxPath = self.ctxPath +"/api/summaryApi/"
        doMethod = "getStrategySumViewByTag"
        resp = self.session.post(ctxPath,
                             data={'doMethod': doMethod,
                                   "page":1,
                                   "pageSize":5,
                                   'translation':"zh"})
        print(doMethod,resp.json())
    def test_companyApi(self):
        ctxPath = self.ctxPath +"/api/companyApi/"
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "addCompany"
        resp = self.session.post(ctxPath,
                             data={'doMethod': doMethod,
                                   'companyCode': "test",
                                   'companyName': "test",
                                   'shortName': "test", })
        #api/addUser
        ctxPath = self.ctxPath +"/api/addUser/"
        doMethod = "addUser"
        resp = self.session.post(ctxPath,
                             data={'doMethod': doMethod,
                                   'email': "test",
                                   'userRealName': "test",
                                   'userNickName': "test",
                                   'companyId': "test",
                                   'roleId': "test",
                                   'shortName': "test", })
        print(doMethod,resp.json())
    def test_userApi(self):
        ctxPath = self.ctxPath +"/api/userApi/"
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "getAllUserData"
        resp = self.session.post(ctxPath,
                             data={'doMethod': doMethod,  })
        print(doMethod,resp.json())
    def viewInterResearch(self):
        ctxPath = self.ctxPath +"/api/viewInterResearch/"
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "getResearchFile"
        resp = self.session.post(ctxPath,
                             data={'doMethod': doMethod,
                                   "fileId":"cicc_324946_1150531",
                                   "returnType":"stream",
                                   })
        print(doMethod,resp.json())
    def meetingType(self):
        ctxPath = self.ctxPath +"/api/audioApi/"
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "meetingType"
        resp = self.session.post(ctxPath,
                             data={
                                 'doMethod': doMethod,
                                 'translation': "en",
                                   })
        print(resp.text)
        print(doMethod,resp.json())
    def getAudioSummary(self):
        ctxPath = self.ctxPath +"/api/audioApi/"
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "getAudioSummary"
        resp = self.session.post(ctxPath,
                             data={
                                 'doMethod': doMethod,
                                 'translation': "zh",
                                 "pageSize":5,
                                 # "meetingTypes":"1,2,3",
                                 # "myAudio":1,
                                 # "searchTitle":"力",
                                   })
        print(doMethod,resp.json())
    def uploadAudioFile(self):
        ctxPath = self.ctxPath +"/api/audioApi/"
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "uploadAudioFile"
        from requests_toolbelt.multipart.encoder import MultipartEncoder
        data={
                 'doMethod': doMethod,
                 'language': "en",
                 "audioTitle":"1202测试环境测试电话会议",
                 "meetingType":"1",
                   }
        # 使用 MultipartEncoder 构建请求体
        multipart_data = MultipartEncoder(
            fields={  'file': ('New Recording 6.m4a', open(r'c:/Users/env/Downloads/New Recording 6.m4a', 'rb')) , **data}
        )
        headers = {'Content-Type': multipart_data.content_type }
        resp = self.session.post(ctxPath,data=multipart_data, headers=headers)
        print(doMethod,resp.json())
    def delAudioData(self):
        ctxPath = self.ctxPath +"/api/audioApi/"
        # doMethod = "getCompanyData"
        # resp = self.session.post(ctxPath,
        #                      data={'doMethod': doMethod, })
        doMethod = "delAudioData"
        resp = self.session.post(ctxPath,
                             data={
                                 'doMethod': doMethod,
                                 'cid': "Audio_8186e53ce642879efe394c70054342c7",
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
    # st.getViewSummaryByTag()
    # st.getStrategySumViewByTag()
    # st.test_getDocContent()
    # st.meetingType()
    # st.getAudioSummary()
    # st.uploadAudioFile()
    # st.test_getViewSummaryTitleByType()
    # st.delAudioData()

    # st.test_companyApi()
    # st.test_userApi()
    from InvestCopilot_App.models.summary.viewSummaryMode import viewSummaryMode
    # viewSummaryMode().getSummaryViewByTag(vtags=["news"],queryId="self_23462")
    # rest = viewSummaryMode().getSummaryViewByTag(vtags=["research"],queryId="self_23462",userId='051',page=1,pageSize=10)
    # rest = viewSummaryMode().getSummaryViewByTag(vtags=["transcripts","Press Releases"],queryId="self_23462",userId='051',page=1,pageSize=10)
    # rest = viewSummaryMode().getSummaryViewByTag(vtags=["EarningsCallPresentation"],queryId="self_23462",userId='051',page=1,pageSize=10)
    # rest = viewSummaryMode().getSummaryViewByTag(vtags=["innerResearch"],queryId="self_23462",userId='051',page=1,pageSize=10)
    # rest = viewSummaryMode().getSummaryViewByTag(vtags=["research"],queryId="hgcl",userId='051',page=1,pageSize=10)
    # rest = viewSummaryMode().getSummaryViewByTag(vtags=["research"],queryId="hgcl",userId='051',page=1,pageSize=10)
    # rest = viewSummaryMode().getDocContent(['company_bff92a82f5b335b6b15a4625cd17cf04'])
    # rest = viewSummaryMode().getDocContent(['company_bff92a82f5b335b6b15a4625cd17cf04'])

    # rest=st.viewInterResearch()
    rest=st.test_getDocContent()
    # vsummary_dt=rest.data
    # print("vsummary_dt:",vsummary_dt)
    # for x in  vsummary_dt :
    #     print(x['publishOn'],x["source"],x["title"],)
    # pass