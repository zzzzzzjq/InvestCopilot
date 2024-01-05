

import requests



class likeAPITest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8009"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/stockChatApi/"
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


    def stockChatMeta(self):
        doMethod="stockChatMeta"
        params = {'doMethod':doMethod,  }
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        print(doMethod,"resp:",resp.json())
        return resp.json()

    def stockChatQuery(self):
        doMethod="stockChatQuery"
        params = {'doMethod':doMethod,
                  "query_text": "特斯拉这个季度股价表现如何2",
                  "chat_id": "asd478818959fd6578s88d969121",
                  # "result_id": "658ad2ec377b8960cd802dd9",
                  "model_names": "gpt-4,gpt-4-1106-preview",
                  # "auto_filter": 1,
                  "company_ids": "1,2",
                  "datatype_ids": "1,2",
                  "months": "202311,202312"
                  }
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params )
        print(doMethod,"resp:",resp.json())
        return resp.json()

    def getStockChatConv(self):
        doMethod="getStockChatConv"
        params = {'doMethod':doMethod,
                  "conv_id": "asd478818959fd6578s88d9691",
                  }
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params )
        print(doMethod,"resp:",resp.json())
        return resp.json()

    def delStockChatConv(self):
        doMethod="delStockChatConv"
        params = {'doMethod':doMethod,
                  "conv_id": "asd478818959fd6578s88d969121",
                  }
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params )
        print(doMethod,"resp:",resp.json())
        return resp.json()
    def editStockChatConv(self):
        doMethod="editStockChatConv"
        params = {'doMethod':doMethod,
                  "conv_id": "asd478818959fd6578s88d969122",
                  "conv_title":"鞍山市"
                  }
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params )
        print(doMethod,"resp:",resp.json())
        return resp.json()

    def getStockChatConvResult(self):
        doMethod="getStockChatConvResult"
        params = {'doMethod':doMethod,
                  "conv_id": "asd478818959fd6578s88d96912",
                  }
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params )
        print(doMethod,"resp:",resp.json())
        return resp.json()

    def delLike(self):
        doMethod="delLike"
        params = {'doMethod':doMethod, 'sid':"cnbc_1c62b42490857e10b8f37cbddd1bd070", 'slike':"N",}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        print(doMethod,"resp:",resp.json())
        return resp.json()
    def getLikeNum(self):
        doMethod="getLikeNum"
        params = {'doMethod':doMethod, 'sid':"jpmorgan_GPS-4555300-0", 'slike':"N",}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        print(doMethod,"resp:",resp.json())
        return resp.json()
    def getCommentsData(self):
        doMethod="getCommentsData"
        params = {'doMethod':doMethod, 'sid':"mq_b9380273-85d1-419f-8bc2-04656898bb7b", 'slike':"N",'page':2,'pageSize':3}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        print(doMethod,"resp:",resp.json())
        return resp.json()
    def getCommentsNum(self):
        doMethod="getCommentsNum"
        params = {'doMethod':doMethod, 'sid':"jpmorgan_GPS-4555300-0", 'slike':"N",}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        print(doMethod,"resp:",resp.json())
        return resp.json()

if __name__ == '__main__':
    pts = likeAPITest()
    # pts.login("robby.xia@baichuaninv.com",'robbyxia!1')
    pts.login("robby.xia@baichuaninv.com",'test1234')
    # pts.login("yewei.lu@njjhtz.com",'jUgpdKMW')
    # pts.stockChatMeta()
    # pts.stockChatQuery()
    # pts.delStockChatConv()
    pts.getStockChatConv()
    pts.editStockChatConv()
    pts.getStockChatConv()
    # pts.getStockChatConvResult()
    # pts.delLike()
    # pts.getLikeNum()
    # pts.getCommentsData()
    # pts.getCommentsNum()
