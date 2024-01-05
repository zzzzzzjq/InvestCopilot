
import requests
class companyResearchApiTest():

    def __init__(self):
        # self.ctxPath = "http://192.168.2.6:8008"
        self.ctxPath = "http://127.0.0.1:8009"
        # self.ctxPath = "http://47.106.236.106:6543"
        self.ctxUrl = self.ctxPath + "/api/companyResearchApi/"
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
    def addCompanyReport(self):
        doMethod = "addCompanyReport"
        from requests_toolbelt.multipart.encoder import MultipartEncoder
        data={
                 'doMethod': doMethod,
                 'windCode': "BABA.N",
                 "title":"nvd test",
                 "rating":"买入",
                 "priceTarget":"1233",
                 "summaryText":'<p><span style="color:rgb(78, 81, 85); font-family:roboto,sans-serif; font-size:13.3px">报告主题报告主题报告主题</span></p>',
                   }
        # 使用 MultipartEncoder 构建请求体
        multipart_data = MultipartEncoder(
            fields={
                'file1': ('48244714.pdf', open(r'c:/Users/env/Downloads/48244714.pdf', 'rb')) ,
                'file2': ('mega_allfactors.csv', open(r'c:/Users/env/Downloads/mega_allfactors.csv', 'rb')) ,
                      **data}
        )
        headers = {'Content-Type': multipart_data.content_type }
        resp = self.session.post(self.ctxUrl,data=multipart_data, headers=headers)
        print(doMethod,resp.json())
    def editCompanyReport(self):
        doMethod = "editCompanyReport"
        from requests_toolbelt.multipart.encoder import MultipartEncoder
        data={
                 'doMethod': doMethod,
                 'reportId': "company_f685a2fd93ad502469778c3a3ae321d4",
                 # "title":"nvd testxxxxx",
                 "rating":"买入3",
                 # "priceTarget":"123323",
                 # "summaryText":'xxxx<p><span style="color:rgb(78, 81, 85); font-family:roboto,sans-serif; font-size:13.3px">报告主题报告主题报告主题</span></p>',
                   }
        # 使用 MultipartEncoder 构建请求体
        multipart_data = MultipartEncoder(
            fields={
                'file1': ('48244714.pdf', open(r'c:/Users/env/Downloads/48244714.pdf', 'rb')) ,
                'file2': ('mega_allfactors.csv', open(r'c:/Users/env/Downloads/mega_allfactors.csv', 'rb')) ,
                      **data}
        )
        headers = {'Content-Type': multipart_data.content_type }
        # resp = self.session.post(self.ctxUrl,data=multipart_data, headers=headers)
        resp = self.session.post(self.ctxUrl,data=data)
        print(doMethod,resp.json())
    def delCompanyReport(self):
        doMethod = "delCompanyReport"
        data={
                 'doMethod': doMethod,
                 'reportId': "company_af2808ba1cd89e1dcbeffbb986b8be1d",  }
        resp = self.session.post(self.ctxUrl,data=data)
        print(doMethod,resp.json())
    def delCompanyReportAttachment(self):
        doMethod = "delCompanyReportAttachment"
        data={
                 'doMethod': doMethod,
                 'reportId': "company_f685a2fd93ad502469778c3a3ae321d4",
                 'attachmentId': "202312131934062521720",
        }
        resp = self.session.post(self.ctxUrl,data=data)
        print(doMethod,resp.json())
    def getCompanyReport(self):
        doMethod = "getCompanyReport"
        data={
                 'doMethod': doMethod,
                 'myResearch': "1",
                 # 'searchTitle': "茅台",
        }
        resp = self.session.post(self.ctxUrl,data=data)
        print(doMethod,resp.json())
    def viewCompanyResearchAttachment(self):
        doMethod = "viewCompanyResearchAttachment"
        data={
                 'doMethod': doMethod,
                 'reportId': "company_f685a2fd93ad502469778c3a3ae321d4",
                 'attachmentId': "202312131934374489290",
        }
        resp = self.session.post(self.ctxUrl,data=data)
        import os
        # 处理响应
        file_path="d:/work/temp/test_ss.csv"
        if resp.status_code == 200:
            print(resp.headers)
            # 获取文件名和文件扩展名
            file_name = os.path.basename(file_path)
            _, file_extension = os.path.splitext(file_name)

            # 写入文件
            with open(file_path, 'wb') as file:
                file.write(resp.content)

            # 如果是 PDF 文件，你可以在本地打开它
            import subprocess
            subprocess.Popen(["open", file_path])  # 在 macOS 上打开 PDF
        else:
            print(f"Failed to download or preview file. Status code: {resp.status_code}")

        print(doMethod,resp.content)
if __name__ == '__main__':
    st = companyResearchApiTest()
    # st.login("robby.xia@pinnacle-cap.cn",'test1234')
    st.login("robby.xia@baichuaninv.com",'test1234')
    # st.test_refreshCacheBtn()
    # st.addCompanyReport()#添加内部报告
    # st.delCompanyReport()#删除内部报告
    # st.delCompanyReportAttachment()# 删除附件
    # st.editCompanyReport()# 编辑报告
    st.getCompanyReport()# 报告查询
    # st.viewCompanyResearchAttachment()# 查看附件
    #

