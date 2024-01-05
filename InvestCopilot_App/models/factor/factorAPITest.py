import requests
import json
class factorAPITest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8000"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/factorApi/"
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
    def test_getAllFactorTemplates(self):
        doMethod="getAllFactorTemplates"
        # TemplateType ="100"
        # params = {'doMethod':doMethod,'TemplateType':TemplateType,}
        params = {'doMethod':doMethod,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        print(doMethod,"resp:",resp.json())
        return resp.json()
    def test_createFactorCombination(self,templateName):
        doMethod="createFactorCombination"
        params = {'doMethod':doMethod, 'templateName':templateName,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        rspdt = resp.json()
        print("resp:",rspdt)
        if rspdt['errorFlag']:
            if (rspdt['data']!= None):
                templateno =rspdt['data'].get('templateno')
                return templateno
        else: return None
    def test_editFactorTemplate(self,templateno,FactorSymbols,portfolioId,templateName=""):
        doMethod="editFactorTemplate"
        params = {'doMethod':doMethod, 'templateName':templateName,"templateNo":templateno,"FactorSymbols":FactorSymbols,"portfolioId":portfolioId}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        rspdt = resp.json()
        print("resp:",rspdt)
    def test_addFactorSymbol(self,symbols,templateNo):
        doMethod="addFactorSymbol"
        params = {'doMethod':doMethod, 'FactorSymbols':symbols, 'templateNo':templateNo,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,
                             data=params, timeout=(10, 20))
        rspdt = resp.json()
        print("resp:",rspdt)
    def test_createTemplateAndFactors(self,templateName,symbols,portfolioId):
        doMethod = "addFactorTemplate"
        params = {'doMethod': doMethod, 'FactorSymbols': symbols,'templateName':templateName, 'portfolioId':portfolioId}
        print("self.ctxUrl:", self.ctxUrl)
        resp = self.session.post(self.ctxUrl, headers=self.headers,
                                 data=params, timeout=(10, 20))
        rspdt = resp.json()
        print("resp:", rspdt)
        if rspdt['errorFlag']:
            if (rspdt['data']!= None):
                templateno =rspdt['data'].get('templateno')
                return templateno
        else: return None
    def test_getTemplateFactors(self,templateNo):
        # 获取组合指标代码
        doMethod="getTemplateFactors"
        params = {'doMethod':doMethod, 'templateNo':templateNo,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&templateNo=%s"%(doMethod,templateNo),
        #                         headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)

    def test_copyFactorTemplate(self, templateNo,portfolioIdsTo):
        doMethod = "copyFactorTemplate"
        params = {'doMethod': doMethod, 'templateNo': templateNo,"portfolioIdsTo":portfolioIdsTo }
        print("self.ctxUrl:", self.ctxUrl)
        resp = self.session.post(self.ctxUrl, headers=self.headers, data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod, "resp:", rspdt)
    def test_copyFactorTemplatewithNameChange(self, templateNo,portfolioIdsTo,templatename=''):
        doMethod = "copyFactorTemplatewithNameChange"
        params = {'doMethod': doMethod, 'templateNo': templateNo,"portfolioIdsTo":portfolioIdsTo ,"templateName":templatename}
        print("self.ctxUrl:", self.ctxUrl)
        resp = self.session.post(self.ctxUrl, headers=self.headers, data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod, "resp:", rspdt)
    def test_delFactorSymbol(self,symbol,templateno):
        doMethod="delFactorSymbol"
        params = {'doMethod':doMethod, 'templateNo':templateno,'FactorSymbols':symbol,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&templateNo=%s&FactorSymbols=%s"%(doMethod,templateno,symbol),
                                # headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def test_delFactorTemplate(self,templateNo):
        doMethod="delFactorTemplate"
        params = {'doMethod':doMethod, 'templateNo':templateNo,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        # resp = self.session.get(self.ctxUrl+"?doMethod=%s&portfolioId=%s&symbol=%s"%(doMethod,portfolioId,symbol),headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)

    def test_parseFileForSymbols(self,Symbols):
        doMethod="parseFileForSymbols"
        params = {'doMethod':doMethod, 'Symbols':Symbols,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)
    def test_parseFileForSymbols_moreinfo(self,Symbols):
        doMethod="parseFileForSymbols_moreinfo"
        params = {'doMethod':doMethod, 'Symbols':Symbols,}
        print("self.ctxUrl:",self.ctxUrl)
        resp = self.session.post(self.ctxUrl,headers=self.headers,data=params, timeout=(10, 20))
        rspdt = resp.json()
        print(doMethod,"resp:",rspdt)


    def login_check_email(self, userName):
        self.loginUrl = self.ctxPath + "/api/passwordForgetByEmail/"
        params = {'username': userName}
        resp = requests.post(self.loginUrl,data=params, headers=self.headers, timeout=(10, 20))
        print("resp:",resp.json())
        if resp.status_code == 200:
            rts =resp.json()
            if (rts['errorFlag']):
                v_cookies = resp.cookies.get_dict()
                print("v_cookies",v_cookies)
                requests_cookies = {}
                for kn, kv in v_cookies.items():
                    requests_cookies[kn] = kv
                    print(kv,"  ",kn)
                session = requests.Session()
                session.cookies.update(requests_cookies)
                print(session.cookies.items())
                self.session = session
            else:
                raise Exception("登录失败[%s]" % rts)
        else:
            raise Exception("登录失败[%s]" % resp.status_code)

    def change_check_email(self, token,newpassword1,newpassword2):
        self.loginUrl = self.ctxPath + "/api/PasswordChangeByEmail/"
        params = {'token': token,'newpasswd1':newpassword1,'newpasswd2':newpassword2}
        resp = requests.post(self.loginUrl,data=params, headers=self.headers, timeout=(10, 20))
        print("resp:",resp.json())
        if resp.status_code == 200:
            rts =resp.json()
            if (rts['errorFlag']):
                v_cookies = resp.cookies.get_dict()
                print("v_cookies",v_cookies)
                requests_cookies = {}
                for kn, kv in v_cookies.items():
                    requests_cookies[kn] = kv
                    print(kv,"  ",kn)
                session = requests.Session()
                session.cookies.update(requests_cookies)
                print(session.cookies.items())
                self.session = session
            else:
                raise Exception("登录失败[%s]" % rts)
        else:
            raise Exception("登录失败[%s]" % resp.status_code)


if __name__ == '__main__':
    pts = factorAPITest()
    pts.login("18817781975@163.com",'test1234')
    test_choice=10
    if test_choice==1:
        # 1. 创建指标组合
        template_name = "指标组合1113_7"
        templateno=pts.test_createFactorCombination(template_name)
        print("templateno:",templateno)
        # templateno='100_47'
        if templateno is not None:
            # 2. 查询自选股组合
            # pts.test_refreshCacheBtn()
            data = pts.test_getAllFactorTemplates()  # resp.json()
            print('data：',data)
            print('data[data]：',data['data'])
            templateNo = ''
            if data['data'].get("templateFirst") is not None:
                templateNo=data['data']["templateFirst"]['templateNo']
                print(templateNo)
                # 3. 添加自选指标代码
                add_factor_list = ['28699', '28822', '28843', '28788', '28812', '28762', '28769']
                for factor_id in add_factor_list:
                    s = pts.test_addFactorSymbol(factor_id,templateNo)
                # 4. 删除指标模板单指标
                s = pts.test_delFactorSymbol('28822',templateNo)
                # 删除单指标多次
                s = pts.test_delFactorSymbol('28822', templateNo)
                # 5. 查看指标组合的全部指标
                pts.test_getTemplateFactors(templateNo)
                # 6. 修改该指标的指标名
                pts.test_editFactorTemplate("name_change_new",templateNo)
                # 7. 再次查看该用户下所有的指标组合
                data = pts.test_getAllFactorTemplates()
                # 8. 删除指标模板
                # pts.test_delFactorTemplate(templateno)
                # 9. 查看该用户指标模板下所有的指标
                pts.test_getTemplateFactors(templateNo)
        pts.test_getTemplateFactors(templateno)
    elif test_choice==2:
        template_name = "指标组合1115_4"
        # template_name = ''
        portfolioid = 'self_23462'
        # portfolioid = ''
        add_factor_list = ['28699', '28822', '28843', '28788', '28812', '28762', '28769']
        # add_factor_list =[]
        add_factor_str = '|'.join(add_factor_list)
        templateno = pts.test_createTemplateAndFactors(templateName=template_name,symbols=add_factor_str,portfolioId=portfolioid)
        print("templateno:", templateno)
        # templateno='100_44'
        if templateno is not None:
            # 2. 查询自选股组合
            # pts.test_refreshCacheBtn()
            data = pts.test_getAllFactorTemplates()  # resp.json()
            print('data：', data)
            print('data[data]：', data['data'])
            templateNo = ''
            if data['data'].get("templateFirst") is not None:
                templateNo = data['data']["templateFirst"]['templateNo']
                print(templateNo)
                pts.test_getTemplateFactors(templateNo)
    elif test_choice==3:
        templateno='8'
        templateNo=templateno
        portfolioId='self_108'
        # 3. 添加自选指标代码
        # edit_factor_list = ['28699', '28822', '28843', '28788', '28812', '28762', '28769']
        edit_factor_list = ['28699', '28769','28762','49', '28822','90','28844','32',  '28812','28843','53']
        edit_factor_list="|".join(edit_factor_list)
        pts.test_getTemplateFactors(templateNo)
        # 6. 修改该指标的指标名
        pts.test_editFactorTemplate(templateno=templateNo,FactorSymbols=edit_factor_list,portfolioId=portfolioId)
        # 7. 再次查看该用户下所有的指标组合
        pts.test_getTemplateFactors(templateNo)
    elif test_choice==4:
        # userid = "2000"
        templateno = "100_124"
        portfolid = ['self_61', 'self_87', 'self_62', 'self_60', 'self_59', 'self_87']
        portfolid_str = '|'.join(portfolid)
        pts.test_copyFactorTemplate(portfolioIdsTo=portfolid_str,templateNo=templateno)
    elif test_choice==5:
        # userid = "2000"
        templateno = "100_124"
        portfolid = []
        portfolid_str = '|'.join(portfolid)
        pts.test_copyFactorTemplate(portfolioIdsTo=portfolid_str, templateNo=templateno)
    elif test_choice==6:
        pts.test_getTemplateFactors("100_122")
    elif test_choice==7:
        pts.test_parseFileForSymbols("600519|0700.HK|AAPL|NVDA.O|scsc|234fdscs")
        pts.test_parseFileForSymbols_moreinfo("600519|0700.HK|AAPL|NVDA.O|scsc|234fdscs")
    elif test_choice==8:
        templateno = "8"
        portfolid = ['self_108']
        portfolid_str = '|'.join(portfolid)
        pts.test_copyFactorTemplatewithNameChange(portfolioIdsTo=portfolid_str, templateNo=templateno)
    elif test_choice==9:
        pts.login_check_email('18817781975@163.com')
    elif test_choice==10:
        # http://47.106.236.106:1180/#/user/resetPassword?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMTk3OCIsInVzZXJuYW1lIjoiMTg4MTc3ODE5NzVAMTYzLmNvbSIsImV4cCI6MTcwMjIxNjc3Mn0.usCjUL80GcoYmnFbqojNvDA3hSYjPkGEbCMnwb_6G4A
        pts.change_check_email('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiMTk3OCIsInVzZXJuYW1lIjoiMTg4MTc3ODE5NzVAMTYzLmNvbSIsImV4cCI6MTcwMjIxNjc3Mn0.usCjUL80GcoYmnFbqojNvDA3hSYjPkGEbCMnwb_6G4A','test123456','test123456')