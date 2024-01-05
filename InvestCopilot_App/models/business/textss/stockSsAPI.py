# encoding: utf-8
#text-sql 选股 调用一凡API
import datetime
import threading
import time

import requests
from django.http import JsonResponse
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.business.likeMode import likeMode
from InvestCopilot_App.models.business.textss.stockSsMode import stockSelectMode
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.cache.dict import dictCache as dict_cache
Logger = logger_utils.LoggerUtils()

import json
import pandas as pd
import os
import re

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10


class stockSelectApi():
    def __init__(self):
        # self.host="http://47.106.236.106:8503/screener"#外网地址
        self.host="http://172.18.163.217:8503/screener"#内网地址
        self.timeout=(10,20)
    def getTextSelectStocks(self,queryData={}):
        headers = {'Content-Type': 'application/json'}
        # return testdata
        resp = requests.post("%s"%(self.host),json=queryData,headers=headers)
        # print("resp:",resp.json())
        Logger.info("getTextSelectStocks status_code:%s"%resp.status_code)
        print("getTextSelectStocks status_code:%s"%resp.status_code)
        if resp.status_code==200:
            return resp.json()
        else:
            Logger.info("getTextSelectStocks return:%s" % resp.text)
            print("getTextSelectStocks return:%s" % resp.text)
        return {}
@userLoginCheckDeco
def stockSelectApiHandler(request):
    #用户点赞
    rest = ResultData()
    try:
        crest=tools_utils.requestDataFmt(request,fefault=None)
        if not crest.errorFlag:
            return JsonResponse(crest.toDict())
        reqData = crest.data
        doMethod=reqData.get("doMethod")
        user_id = request.session.get("user_id")
        if doMethod=="getTextSelectStocks":
            """ 
            * query：top 10 pe
            * market：us, jp, hk, ch 
            """
            query_text = reqData.get("query_text")
            market = reqData.get("market")
            #参数检查
            if query_text is None or str(query_text).strip()==0:
                rest.errorData(errorMsg="Please enter query_text params.")
            # if market is None or str(market).strip()==0:
            #     rest.errorData(errorMsg="Please enter market params.")
            if market is None or str(market).strip()==0:
                market="us"
            schat = stockSelectApi()
            #默认参数
            qdata={
                    "query": query_text,
                    "market": market,
                }
            Logger.debug("[%s]stockSsAPI Query qdata%s"%(user_id,qdata))
            # c_user_dt = user_utils.getCacheUserInfo(user_id)
            # companyId = c_user_dt['COMPANYID']
            # create_time=datetime.datetime.now()
            #查询次数限制
            # stockSelectMode().addStockChatConv(chat_id,query_text,user_id,create_time)
            # rtnumrs=stockSelectMode().getStockChatRequestNum(user_id,companyId)
            # if not rtnumrs.errorFlag:
            #     return JsonResponse(rtnumrs.toDict())
            meta_data = schat.getTextSelectStocks(qdata)
            Logger.debug("text select stocks return meta_data:%s"% meta_data)
            if 'data' in meta_data:
                indicator_table = pd.read_json(meta_data['data'])
                if len(indicator_table)==0:
                    rest.errorData(errorMsg="抱歉，未找到与您的查询匹配的结果。请尝试使用不同的关键字或调整搜索条件。")
                    return JsonResponse(rest.toDict())
                factorcellDF = cache_db.getFactorCellDF()
                factor_dt = {}
                factorNoList = []
                for r in factorcellDF.itertuples():
                    factorno = str(r.FACTORNO)
                    fView = str(r.FVIEW)
                    factor_dt[factorno] = fView
                stockInfoDict = dict_cache.getStockInfoDT()
                columns = indicator_table.columns.values.tolist()
                Logger.debug("columns:%s"% columns)
                vcolumns = columns.copy()
                vcolumns[0]="windcode"
                vcolumns.insert(1, 'Symbol')
                for _idx, c in enumerate(vcolumns):
                    if c in factor_dt:
                        factorNoList.append(int(c))  # 指标编号  接口提交
                        vcolumns[_idx] = factor_dt[c]
                vcolumns.pop(-1)
                vrows = []
                windCodes = []
                rtWindCodes=[]
                for _idx, row in indicator_table.iterrows():
                    vdt = row.values.tolist()
                    vdt.pop(-1)
                    windCode = vdt[0]
                    rtWindCodes.append(windCode)
                    if windCode in stockInfoDict:
                        windCodes.append(windCode)
                        vinfo = stockInfoDict[windCode]
                        area = vinfo['Area']
                        if area not in ['AM']:
                            vdt.insert(1, vinfo['Stockname'])
                            vrows.append(vdt)
                        else:
                            vdt.insert(1, vinfo['Stockcode'])
                            vrows.append(vdt)
                    else:
                        vdt.insert(1, vdt[0])
                        vrows.append(vdt)
                # print("factorNoList:", factorNoList)
                # print("vrows:", vrows)
                # print("vcolumns:", vcolumns)
                # print("windCodes:", windCodes)
                rest.data={"windCodes":windCodes,"factorNoList":factorNoList,"columns":vcolumns,"data":vrows,"stockcounts":len(vrows)}
            else:
                if "error" in meta_data:
                    rest.errorData(errorMsg="抱歉，发生了一个错误。请尝试重新操作，如果问题仍然存在，请联系支持团队。")
                    return JsonResponse(rest.toDict())
                else:
                    # rest.errorData(errorMsg="抱歉，搜索异常，请稍后再试！")
                    rest.errorData(errorMsg="抱歉，未找到与您的查询匹配的结果。请尝试使用不同的关键字或调整搜索条件。")
                    return JsonResponse(rest.toDict())
                    # rest.data={"windCodes":[],"factorNoList":[],"columns":["windcode","Symbol"],"data":[],"stockcounts":0}
                #保存用户日志数据数据
            # srtd=stockSelectMode().addStockChatConvResult(chat_id,user_id,query_text,update_time,meta_data)
            # result_id="-1"
            # if srtd.errorFlag:
            #     data=srtd.data
            #     result_id=data['result_id']
            # meta_data['result_id']=result_id
            # rest.data =meta_data
            return JsonResponse(rest.toDict())
        elif doMethod=="getStockChatConv":
            pageSize = reqData.get("pageSize")
            if pageSize=="" or pageSize is None:
                pageSize=20
            else:
                pageSize=int(pageSize)
            page = reqData.get("page")
            if page=="" or page is None:
                page=1
            else :page=int(page)
            rest=stockSelectMode().getStockChatConv(user_id,page=page,pageSize=pageSize)
            return JsonResponse(rest.toDict())
        if doMethod=="getStockChatConvResult":
            conv_id=reqData.get("conv_id")
            if conv_id=="" or conv_id is None:
                rest.errorData(errorMsg="Please enter conv_id params.")
                return JsonResponse(rest.toDict())
            pageSize = reqData.get("pageSize")
            if pageSize=="" or pageSize is None:
                pageSize=20
            else:
                pageSize=int(pageSize)
            page = reqData.get("page")
            if page=="" or page is None:
                page=1
            else :page=int(page)
            rest=stockSelectMode().getStockChatConvResult(conv_id,user_id,page=page,pageSize=pageSize)
            return JsonResponse(rest.toDict())

        if doMethod=="delComments":
            cid=reqData.get("cid")
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            if cid=="" or cid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest = likeMode().delComments(cid,sid,user_id)
            return JsonResponse(rest.toDict())

        elif doMethod=="getCommentsNum":
            sid=reqData.get("sid")
            if sid=="" or sid is None:
                rest.errorData(errorMsg="Please enter a params.")
                return JsonResponse(rest.toDict())
            rest = likeMode().getCommentsNum(sid)
            return JsonResponse(rest.toDict())
        else:
            rest.errorData("stockSelectApi not find")
            return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        #UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())

if __name__ == '__main__':
    # %%
    # import requests
    # import pandas as pd
    #
    # columns = ['windCode', '28750', 'tradedate']
    #
    # from InvestCopilot_App.models.toolsutils import dbutils as dbutils
    # import InvestCopilot_App.models.cache.cacheDB  as cache_db
    #
    # con,cur = dbutils.getConnect()
    # factorcellDF = cache_db.getFactorCellDF()
    # factor_dt={}
    # factorNoList=[]
    # for r in factorcellDF.itertuples():
    #     factorno=str(r.FACTORNO)
    #     fView=str(r.FVIEW)
    #     factor_dt[factorno]=fView
    # #股票公司转代码
    # stockInfoDict =dict_cache.getStockInfoDT()
    #
    # url = 'http://47.106.236.106:8503/screener'
    # response = requests.post(url, json={"query": 'top 10 pe and show marktvalue', 'market': 'hk'})  # market: us, jp, hk, ch
    #
    # result = response.json()
    # # result['sql']: 获取sql
    # # result['data']: 获取序列化的dataframe，需用pd.read_json转回dataframe使用
    # print("result:",result)
    #
    # if 'data' in result:
    #     indicator_table = pd.read_json(result['data'])
    #     columns=indicator_table.columns.values.tolist()
    #     print("indicator_table:",indicator_table)
    #     print("columns:",columns)
    #     vcolumns=columns.copy()
    #     vcolumns.insert(1,'Symbol')
    #     tcolumns=columns
    #
    #     for _idx, c in enumerate(columns):
    #         if c in factor_dt:
    #             factorNoList.append(int(c))  # 指标编号  接口提交
    #             vcolumns[_idx]=factor_dt[c]
    #     vcolumns.pop(-1)
    #     indicator_table = pd.read_json(result['data'])
    #     vrows = []
    #     windCodes=[]
    #     for _idx, row in indicator_table.iterrows():
    #         vdt = row.values.tolist()
    #         vdt.pop(-1)
    #         windCode = vdt[0]
    #         print("windCode:",windCode)
    #         if windCode in stockInfoDict:
    #             windCodes.append(windCode)
    #             vinfo = stockInfoDict[windCode]
    #             print("vinfo:",vinfo)
    #             area = vinfo['Area']
    #             if area not in ['AM']:
    #                 vdt.insert(1,vinfo['Stockname'])
    #                 vrows.append(vdt)
    #             else:
    #                 vdt.insert(1, vinfo['Stockcode'])
    #                 vrows.append(vdt)
    #         else:
    #             vdt.insert(1, vdt[0])
    #             vrows.append(vdt)
    #
    #     print("factorNoList:",factorNoList)
    #     print("vrows:",vrows)
    #     print("vcolumns:",vcolumns)
    #     print("windCodes:",windCodes)
    #
    # else:
    #     print('error', result)

    pass


