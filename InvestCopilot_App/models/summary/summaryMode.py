# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings("ignore")
import json
import math
import time
import traceback
import datetime
import re
import socket
import os
import sys
import requests
import pandas as pd
import math
import threading
from pymongo import UpdateOne,UpdateMany,ReplaceOne

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from bson.son import SON

sys.path.append("../..")
# import login.stock.stockutils as stock_utils
# import login.toolsutils.ToolsUtils as tool_utils
# import numpy as np
# import pandas as pd
# import decimal
# import login.cache.cacheDB as cache_db
# import login.cache.dataSynTools.cacheTools as redis_tools
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
# from login.toolsutils.ResultData import ResultData
# from login.toolsutils import dbutils as dbutils
# from login.toolsutils import ToolsUtils as tools_utils
# from login.user import UserInfoUtil as user_utils
#
# import login.toolsutils.sendmsg as wx_send
# from login.toolsutils import SysEnums as sys_enums
# import login.toolsutils.LoggerUtils as Logger_utils
# from login.methods.manager import ManagerUserMode as manager_user_mode
# from login.news import  userMode
# import login.sendemail as sendemail

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg

import logging
from InvestCopilot_App.models.toolsutils import dbmongo

Logger = logger_utils.LoggerUtils()

_lock = threading.RLock()
class summaryMode():

    def __init__(self):
        pass
    def getCallData(self, id=None, symbol=None, page=0, pageSize=0, database='news', dbName="earnings_call"):
        # 计算跳过的文档数量
        skip_count = (page - 1) * pageSize
        mgdbdatas = []
        with dbmongo.Mongo(database) as md:
            mydb = md.db
            Hotpostset = mydb[dbName]
            if id is not None:
                calldata = Hotpostset.find_one({'id': id})
                mgdbdatas.append(calldata)
                return mgdbdatas
            if symbol is None:
                calldatas = Hotpostset.find({}, {'_id': 0, 'pdfPath': 0, 'pdfName': 0}).sort([('quarter', -1)]).skip(
                    skip_count).limit(pageSize)
            else:
                calldatas = Hotpostset.find({'symbol': symbol}, {'_id': 0, 'pdfPath': 0, 'pdfName': 0}).sort(
                    [('quarter', -1)]).skip(skip_count).limit(pageSize)
            for c in calldatas:
                #     print(c)
                mgdbdatas.append(c)
        return mgdbdatas

    def getCallSummary(self, id, database='news', dbName="earnings_call"):
        calldata = {}
        with dbmongo.Mongo(database) as md:
            mydb = md.db
            Hotpostset = mydb[dbName]
            calldata = Hotpostset.find_one({'id': id}, {'_id': 0, 'pdfPath': 0, 'pdfName': 0})
        return calldata

    def getNewsIds(self, database, dbName, beginTime, endTime, windCode=None, sumaryFlag="0", allCols=False,
                   addQuerys={}, rtColumns={}, isPool="1"):
        """
        获取新闻编号
        :param database:
        :param dbName:
        :param beginTime:
        :param endTime:
        :param windCode:
        :param sumaryFlag: 是否已经生成好summary
        :param allCols:
        :return:
        """
        rest = ResultData()
        try:
            rstStocks = self.getStockPool()
            stockpool = rstStocks.allCodes
            nstockpool = []
            for code in stockpool:
                nstockpool.append(code)
            with dbmongo.Mongo(database) as md:
                mydb = md.db
                contentsSet = mydb[dbName]
                # 研报发布时间
                querys = {"publishOn": {'$gte': beginTime, '$lte': endTime}}
                if windCode is not None:
                    querys['windCode'] = windCode
                else:
                    if isPool:
                        querys["windCode"] = {"$in": nstockpool}
                if str(sumaryFlag) == "1":
                    querys["updateTime"] = {"$exists": True}
                else:
                    querys["updateTime"] = {"$exists": False}
                if len(querys) > 0:
                    querys = addQuerys
                rtCols = {"_id": 0}
                if not allCols:
                    rtCols = {"_id": 0, "id": 1, }
                if len(rtColumns) > 0:
                    allCols = True
                    rtCols.update(rtColumns)
                #print("getNewsIds querys:%s rtColumns:%s rtCols:%s" % (querys, rtColumns,rtCols))
                # Logger.info("getNewsIds querys:%s" % querys)
                contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                if not allCols:
                    nids = [x['id'] for x in contentsSets]
                else:
                    nids = [x for x in contentsSets]
            # print("nids:",len(nids),nids)
            rest.data = nids
        except:
            Logger.errLineNo(msg="getNewsIds error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def getSetData(self, database, dbName, addQuerys={}, rtColumns={}):
        """
        获取新闻编号
        :param database:
        :param dbName:
        :param allCols:
        :return:
        """
        rest = ResultData()
        try:
            with dbmongo.Mongo(database) as md:
                mydb = md.db
                contentsSet = mydb[dbName]
                querys = {}
                if len(addQuerys) > 0:
                    if dbName in ['summaryIds']:
                        ctdt = addQuerys["createTime"]
                        if "$gte" in ctdt:
                            gte = ctdt["$gte"]
                            ctdt["$gte"]=datetime.datetime.strptime(gte,"%Y-%m-%d %H:%M:%S")
                        if "$lte" in ctdt:
                            lte = ctdt["$lte"]
                            ctdt["$lte"]=datetime.datetime.strptime(lte,"%Y-%m-%d %H:%M:%S")
                        addQuerys["createTime"]=ctdt
                    querys.update(addQuerys)
                rtCols = {"_id": 0}
                if len(rtColumns) > 0:
                    rtCols.update(rtColumns)
                else:
                    rtCols = {"_id": 0, }
                #print("getSetData db:%s,set:%s querys:%s rtColumns:%s" % (database,dbName, querys, rtColumns))
                Logger.info("getSetData querys:%s" % querys)
                contentsSets = contentsSet.find(querys, rtCols)  # 0不显示，1：显示； 查询数据
                nids = [x for x in contentsSets]
            # print("nids:",len(nids),nids)
            rest.data = nids
        except:
            Logger.errLineNo(msg="getSetData error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def addNewReport(self, windCode,title,userId,companyId, content):
        """
        获取新闻编号
        :param database:
        :param dbName:
        :param allCols:
        :return:
        """
        rest = ResultData()
        try:
            publishOn=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            title= "(%s) "%windCode + title
            rid = "company_"+tools_utils.md5("%s%s"%(windCode,title))
            _newdata = {
            "id":rid,
            "title":title,
            "symbols": windCode,
            "windCode": windCode,
            "productName": windCode,
            "tickers": [
                windCode
            ],
            "summaryText": "",
            "source": "company",
            "dataType": "innerResearch",
            "publishOn": publishOn,
            "insertTime":publishOn,
            "summary": content,
            "cuserId":userId,
            "companyId":companyId,
            "title_zh": title,
            "updateTime": publishOn
        }
            with dbmongo.Mongo("website") as md:
                mydb = md.db
                contentsSet = mydb["company"]
                existsData = contentsSet.find_one({'id': rid})
                if existsData is None:
                    rts = contentsSet.insert_one(_newdata)
                else:
                    rest.errorData(errorMsg="与主题相关的研报内容已经存在，请重新修改")
                    return rest
        except:
            Logger.errLineNo(msg="addNewReport error")
            rest.errorData(errorMsg="抱歉，报告数据失败，请稍后重试")
        return rest

    def getInfomationData(self, beginTime, endTime, windCodes=[], userIdsPool=[], sumaryFlag="0", allCols=False,
                          rtcolumns={}):
        """
        获取新闻资讯
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        try:
            rstStocks = self.getStockPool(userIds=userIdsPool)
            stockpool = rstStocks.allCodes
            nstockpool = []
            rtnids = []
            for code in stockpool:
                nstockpool.append(code)
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag=sumaryFlag)
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            for idb in infomationDBs:
                database = idb['website']
                dbset = idb['dbset']
                # print("query:website:%s,dbset:%s" % (database, dbset))
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    # 研报发布时间
                    querys = {"publishOn": {'$gte': beginTime, '$lte': endTime}}
                    if len(windCodes) > 0:
                        querys["windCode"] = {"$in": windCodes}
                    else:
                        querys["windCode"] = {"$in": nstockpool}
                    querys["updateTime"] = {"$exists": False}
                    rtCols = {"_id": 0}
                    if not allCols:
                        rtCols = {"_id": 0, "id": 1, }
                    if len(rtcolumns) > 0:
                        rtCols = rtcolumns
                        rtCols.update({'_id': 0})
                    # print("getInformationData querys:%s,rtCols:%s"%(querys,rtCols))
                    Logger.info("getInformationData querys:%s" % querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                    nids = [x for x in contentsSets]
                    rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            rest.data = rtnids
        except:
            Logger.errLineNo(msg="getInformationData error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest


    def getNewsIds_v1(self, beginTime, endTime, windCodes=[], userIdsPool=[], sumaryFlag="0", allCols=False,
                      rtcolumns={}):
        """
        获取新闻资讯编号
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        try:

            #宏观策略
            authorDT=self.getAuthorIds()
            rstStocks = self.getStockPool(userIds=userIdsPool,preDays=-4)
            stockpool = rstStocks.allCodes
            nstockpool = []
            rtnids = []
            for code in stockpool:
                nstockpool.append(code)
            nstockpool.append("Audio")
            # nstockpool.extend(monitorCodes)
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            for idb in infomationDBs:
                database = idb['website']
                dbset = idb['dbset']
                fnstockpool=nstockpool
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    # 研报发布时间
                    querys = {"publishOn": {'$gte': beginTime, '$lte': endTime},"summaryText":{"$ne":""}}
                    if len(windCodes) > 0:
                        fnstockpool= windCodes
                    else:
                        if dbset in authorDT:
                            # print("authorDT[dbset]['authorIds']:",authorDT[dbset]['authorIds'])
                            fnstockpool.extend(authorDT[dbset]['authorIds'])  # 研究员编号
                    if sumaryFlag=="1":
                        querys["updateTime"] = {"$exists": False}
                    rtCols = {"_id": 0}
                    if not allCols:
                        rtCols = {"_id": 0, "id": 1,"tickers":1}
                    if len(rtcolumns) > 0:
                        rtCols = rtcolumns
                        rtCols.update({'_id': 0})
                    # print("getInfomationIds dbset:%s, querys:%s"%(dbset,querys))
                    contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                    nids = []
                    for ec in contentsSets:
                        if 'tickers' not in ec:
                            continue
                        ctickers = ec['tickers']  # 关联代码匹配
                        if len(set(ctickers) & set(fnstockpool)) > 0:
                            nids.append(ec['id'])  #
                    # nids = [x['id'] for x in contentsSets if x['windCode'] in nstockpool]
                    rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            rest.data = rtnids
            rest.count = len(rtnids)
        except:
            Logger.errLineNo(msg="getInfomationIds error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def getNewsIds_v2(self, beginTime, endTime, windCodes=[], userIdsPool=[], sumaryFlag="0", allCols=False,
                      rtcolumns={}):
        """
        获取新闻资讯编号
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        try:
            rstStocks = self.getStockPool(userIds=userIdsPool)
            stockpool = rstStocks.allCodes
            nstockpool = []
            rtnids = []
            for code in stockpool:
                nstockpool.append(code)
            if len(windCodes) > 0:
                nstockpool = windCodes  # querys["windCode"] = {"$in": windCodes}
            else:
                # querys["windCode"] = {"$in": nstockpool}
                pass
            nstockpool.append("Audio")
            querys = {"publishOn": {'$gte': beginTime, '$lte': endTime},"summaryText":{"$ne":""}}
            if sumaryFlag == "1":
                querys["updateTime"] = {"$exists": False}
            rtCols = {"_id": 0, 'windCode': 1}
            if not allCols:
                rtCols = {"_id": 0, "id": 1, 'windCode': 1}
            if len(rtcolumns) > 0:
                rtCols = rtcolumns
                rtCols.update({'_id': 0})
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            for idb in infomationDBs:
                database = idb['website']
                dbset = idb['dbset']
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    # 研报发布时间
                    # Logger.info("getInfomationIds querys:%s"%querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                    nids=[]
                    for ec in contentsSets:
                        if 'windCode'not in ec:
                            continue
                        if ec['windCode'] in nstockpool:
                            nids.append(ec["id"])
                    # nids = [x['id'] for x in contentsSets if x['windCode'] in nstockpool]
                    rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            rest.data = rtnids
            rest.count = len(rtnids)
        except:
            Logger.errLineNo(msg="getNewsIds_v2 error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def getNewsIds_v3(self, beginTime, endTime, windCodes=[], userIdsPool=[], sumaryFlag="0", allCols=False,
                      rtcolumns={},status=-1,gptid=None):
        """
        获取新闻资讯编号
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        try:
            _lock.acquire()
            if gptid is None:
                gptid="1"
            data_dict = cache_dict.getSysDict('1783')
            audioFlag = True  # 是否需要计算音频摘要
            if "audioFlag" in data_dict:
                audioFlagStr = data_dict['audioFlag']
                if str(audioFlagStr) == "1":
                    audioFlag = True
                else:
                    audioFlag = False
            transcriptsFlag = True  # 是否需要计算电话业绩会议
            if "transcriptsFlag" in data_dict:
                transcriptsFlagStr = data_dict['transcriptsFlag']
                if str(transcriptsFlagStr) == "1":
                    transcriptsFlag = True
                else:
                    transcriptsFlag = False

            pressReleasesFlag = True  # 是否需要计算业绩报告
            if "pressReleasesFlag" in data_dict:
                pressReleasesFlagStr = data_dict['pressReleasesFlag']
                if str(pressReleasesFlagStr) == "1":
                    pressReleasesFlag = True
                else:
                    pressReleasesFlag = False

            newsFlag = True  # 是否需要计算新闻摘要
            if "newsFlag" in data_dict:
                newsFlagStr = data_dict['newsFlag']
                if str(newsFlagStr) == "1":
                    newsFlag = True
                else:
                    newsFlag = False
            newsFlag = True  # 是否需要计算新闻摘要
            if "newsFlag" in data_dict:
                newsFlagStr = data_dict['newsFlag']
                if str(newsFlagStr) == "1":
                    newsFlag = True
                else:
                    newsFlag = False
            researchFlag = True  # 是否需要计算研报摘要
            if "researchFlag" in data_dict:
                researchFlagStr = data_dict['researchFlag']
                if str(researchFlagStr) == "1":
                    researchFlag = True
                else:
                    researchFlag = False
            orderFlag = 1  # 检索数据排序
            if "orderFlag" in data_dict:
                orderFlagStr = data_dict['orderFlag']
                if str(orderFlagStr) == "1":
                    orderFlag = 1
                else:
                    orderFlag = -1
            topXstocksByMKT =1000000
            if "topXstocksByMKT" in data_dict:
                TopXstocksByMKTStr = data_dict['topXstocksByMKT']
                topXstocksByMKT=int(TopXstocksByMKTStr)
            #股票池，只算这里面的公司
            summaryStockPools = []
            if "summaryStockPool" in data_dict:
                summaryStockPoolStr = data_dict['summaryStockPool']
                summaryStockPoolStrs = str(summaryStockPoolStr).split(",")
                for sp in summaryStockPoolStrs:
                    if str(sp).strip() != "":
                        summaryStockPools.append(str(sp).strip())
            #只算池中出业绩公司
            onlyTranscriptsStocks = []
            if "onlyTranscriptsStocks" in data_dict:
                transcriptsStocksStr = data_dict['onlyTranscriptsStocks']
                transcriptsStocksStrs = str(transcriptsStocksStr).split(",")
                for sp in transcriptsStocksStrs:
                    if str(sp).strip() != "":
                        onlyTranscriptsStocks.append(str(sp).strip())
            #追加池中出业绩公司
            appendTranscriptsStocks = []
            if "appendTranscriptsStocks" in data_dict:
                transcriptsStocksStr = data_dict['appendTranscriptsStocks']
                transcriptsStocksStrs = str(transcriptsStocksStr).split(",")
                for sp in transcriptsStocksStrs:
                    if str(sp).strip() != "":
                        appendTranscriptsStocks.append(str(sp).strip())
            # print("newsFlag", newsFlag)
            # print("orderFlag", orderFlag)
            # print("researchFlag", researchFlag)
            # print("topXstocksByMKT", topXstocksByMKT)
            # print("summaryStockPool", summaryStockPools)
            # print("onlyTranscriptsStocks", onlyTranscriptsStocks)
            # print("appendTranscriptsStocks", appendTranscriptsStocks)
            #宏观策略
            authorDT=self.getAuthorIds()
            rstStocks = self.getStockPool(userIds=userIdsPool)#跟踪业绩期排序

            trackCodes = rstStocks.trackCodes#会出业绩公司排序 ,dataType="sAts_"
            fsymbols = rstStocks.fsymbols#排序 剔除业绩公司
            stockpool = rstStocks.allCodes#所有股票池+出业绩公司 无序
            #只算设定股票中的
            if len(summaryStockPools)>0:
                trackCodes=summaryStockPools
                fsymbols=fsymbols
                stockpool=stockpool

            #只算池中的业绩公司
            if len(onlyTranscriptsStocks)>0:
                trackCodes=onlyTranscriptsStocks

            #追击算业绩的公司
            if len(appendTranscriptsStocks)>0:
                trackCodes.extend(appendTranscriptsStocks)

            fnewsCodes = fsymbols[0:topXstocksByMKT]#新闻前20
            nstockpool = []
            rtnids = []
            for code in stockpool:
                nstockpool.append(code)
            if len(windCodes) > 0:
                nstockpool = windCodes  # querys["windCode"] = {"$in": windCodes}
            else:
                # querys["windCode"] = {"$in": nstockpool}
                pass
            #AUDIO
            nstockpool.append("Audio")
            #公司编号 对应所有的公司代码
            companyInfoPdData = manager_user_mode.getAllCompanyInfo()
            companyInfoPdData = companyInfoPdData.rename(columns={'COMPANYID': 'id',
                                                                  'COMPANYNAME': 'name'})
            for cpd in companyInfoPdData.itertuples():
                if cpd.id in ['002','010','000','003']:
                    continue
                nstockpool.append(cpd.id)
            #industry
            webSiteDT,idustryCodes = self.getIdustryPlateRange()
            idustryIds=[]
            for idusk, idusdt in idustryCodes.items():
                idustryIds.extend(idusdt['windCodes'])
            idustryIds=list(set(idustryIds))
            nstockpool.extend(idustryIds)
            querys = {"publishOn": {'$gte': beginTime, '$lte': endTime},"summaryText":{"$ne":""}}
            if sumaryFlag == "1":
                querys["updateTime"] = {"$exists": False}
            rtCols = {"_id": 0, 'windCode': 1}
            if not allCols:
                rtCols = {"_id": 0, "id": 1, 'windCode': 1,'tickers': 1, 'source': 1, 'dataType': 1, 'insertTime': 1}
            if len(rtcolumns) > 0:
                rtCols = rtcolumns
                rtCols.update({'_id': 0})
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            for idb in infomationDBs:
                database = idb['website']
                dbset = idb['dbset']
                fnstockpool=nstockpool
                #新闻 前20市值
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    if dbset in  authorDT:
                        # print("authorDT[dbset]['authorIds']:",authorDT[dbset]['authorIds'])
                        fnstockpool.extend(authorDT[dbset]['authorIds'])#研究员编号
                    # 研报发布时间
                    # Logger.info("getInfomationIds querys:%s"%querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', orderFlag)])  # 0不显示，1：显示； 查询数据
                    nids=[]
                    for ec in contentsSets:
                        if 'tickers'not in ec:
                            continue
                        ctickers=ec['tickers']#关联代码匹配
                        if len(set(ctickers) & set(fnstockpool)) > 0:
                        # if ec['windCode'] in fnstockpool:
                            nids.append(ec)#
                    # nids = [x['id'] for x in contentsSets if x['windCode'] in fnstockpool]
                    #将id进行分类
                    rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            dbError={}
            dbDoing={}
            dbPull={}
            dbNone= {}
            dbEnd= {}
            existsId=[]
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                summarySet = mydb["summaryIds"]
                sdata = summarySet.find({},{'_id':0})#{'status':{"$in":[0,2,9]}}#0:未处理，2：pull,3:处理中 ；9：异常 1：完成
                for sd in sdata:
                    sstatus=sd['status']
                    sid=sd['id']
                    existsId.append(sid)
                    if sstatus ==0:
                        dbNone[sid]=sd
                    elif sstatus ==1:
                        dbEnd[sid]=sd
                    elif sstatus ==2:
                        dbDoing[sid]=sd
                    elif sstatus ==3:
                        dbPull[sid]=sd
                    elif sstatus ==9:
                        dbError[sid]=sd
                    else:
                        continue
            if status==9:
                edatas=[]
                for eid,ed in dbError.items():
                    edatas.append(ed)
                rest.data = edatas
                rest.count = len(edatas)
                return rest
            addRows=[]
            #exeorder -1 优先级最高，对已经出业绩的公司优先处理
            levels={
                    "Audio": {"level": 0, "gptv": ['gpt4']},
                    "Press Releases":{"level":1,"gptv":['gpt3.5']},
                    "transcripts": {"level": 2, "gptv": ['gpt4']},
                    "research":{"level":3,"gptv":['gpt4']},
                    "Economics & Strategy":{"level":4,"gptv":['gpt4']},
                    "industry":{"level":4,"gptv":['gpt4']},
                    "news":{"level":5,"gptv":['gpt3.5']},
                    }
            # print("fnewsCodes:",fnewsCodes)
            for nd in rtnids:
                if nd["id"] in existsId:
                    continue
                vdataType = nd['dataType']
                tickers = nd['tickers']#股票列表
                # print("vdataType:", vdataType,nd['id'])
                # print("tickers:", tickers)
                # print("ckCodes+fnewsCodes:", ckCodes + fnewsCodes)
                if vdataType in ['news']:
                    if not newsFlag:
                        continue
                    isNeed=False #在股票池中才算新闻
                    for exeorder, rstcode in enumerate(fnewsCodes):
                        if rstcode in tickers:
                            isNeed=True
                            break
                    if not isNeed:
                        continue
                #出业绩公司
                elif vdataType in ['transcripts'] :
                    if not transcriptsFlag:
                        continue
                    isNeed=False
                    for exeorder, rstcode in enumerate(trackCodes):
                        if rstcode in tickers:
                            isNeed=True
                            break
                    if not isNeed:
                        continue
                elif vdataType in ['research']:
                    if not researchFlag:#研报开关
                        continue
                elif vdataType in ['Audio']:
                    if not audioFlag:#电话会议
                        continue
                elif vdataType in ['Press Releases']:
                    if not pressReleasesFlag:#业绩报告
                        continue
                if vdataType in levels:
                   vlevels = levels[vdataType]
                else:
                    continue
                nt=datetime.datetime.now()
                insertTime=nd['insertTime']
                insertTime=datetime.datetime.strptime(insertTime,"%Y-%m-%d %H:%M:%S")
                rexeorder = 10000
                if vdataType in ["Audio"]:
                    exeorder=vlevels #优先级最高
                else:
                    #按优先级排序
                    for exeorder, rstcode in enumerate(trackCodes):
                        if rstcode in tickers:
                            rexeorder=exeorder
                            break
                    if rexeorder==10000:
                        #按市值排序
                        for exeorder, rstcode in enumerate(stockpool):
                            if rstcode in  tickers:
                                rexeorder=exeorder
                                break
                nrow={"gptid": gptid,"exeorder": rexeorder,"id": nd["id"],"status": 0,"dataType":vdataType,"createTime": insertTime,"pullTime": nt,"pushTime":nt,"level": vlevels['level'],"gptv": vlevels['gptv'],}
                #按优先级排序
                addRows.append(nrow)
            #dbDoing 触发时间检查，如果pullTime 超过10分钟，重置
            nt = datetime.datetime.now()
            noDoing=[]
            for rid, da in dbDoing.items():
                pushTime=da['pullTime']
                difMinutes = (nt - pushTime).total_seconds() // 60
                # print(rid,"difMinutes:",difMinutes,pushTime,nt)
                if difMinutes>50:#超时20分钟重置
                    noDoing.append(UpdateMany({"id": rid}, {"$set": {"status": 0}}))
            #是否有执行中的gpt4 一次只能有一个在运行
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                summarySet = mydb["summaryIds"]
                #新的列表
                if len(addRows)>0:
                    # Prepare the bulk write operations for upsert
                    # bulk_operations = [
                    #     ReplaceOne({"id": doc["id"]}, doc, upsert=True)
                    #     for doc in addRows
                    # ]
                    # Execute bulk write
                    # summarySet.bulk_write(bulk_operations)
                    # summarySet.insert_many(addRows)
                    for ard in addRows:
                        fsone = summarySet.find_one({"id":ard['id']},{"_id":0,"id":1})
                        if fsone is None:
                            summarySet.insert_one(ard)
                #超时重置状态0.
                if len(noDoing)>0:
                    result = summarySet.bulk_write(noDoing)
                    # print(f"Modified {result.modified_count} documents")
                if len(windCodes)>0:
                    #计算个股摘要程序触发，不下发计算编号。
                    rest.data = []
                    rest.count = 0
                    return rest
                #检查是否有在处理的gpt4请求
                hasData = summarySet.find({"status":{"$in":[2]},"gptv":{"$in":["gpt4"]},"gptid":gptid},{'_id':0})
                hasGtp4 = False
                for gd in hasData:
                    if "gpt4" in gd['gptv']:
                        hasGtp4 = True
                        break
                #获取未处理状态。9：异常状态暂未启用
                pulldatas = summarySet.find({"status":{"$in":[0]}},{'_id':0}).sort([('level',1),('exeorder',1),('createTime',orderFlag)])
                for pds in pulldatas:
                    #已经有在处理的gpt4 就不下发。
                    if "gpt4" in  pds['gptv'] and hasGtp4:
                        continue
                    spullTime=datetime.datetime.now()
                    pds['pullTime']=spullTime
                    pds['status']=2
                    rtnids=[pds]
                    summarySet.update_many({"id": pds["id"]}, {"$set":{'status': 2,'pullTime': spullTime,"gptid":gptid}})
                    break
                else:
                    rtnids=[]
                # summarySet.find_one({"status":{"$in":[0,9]}}).sort([('level',1),('createTime',1)])
            rest.data = rtnids
            rest.count = len(rtnids)
        except:
            Logger.errLineNo(msg="getNewsIds_v3 error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        finally:
            try:
                _lock.release()
            except:pass
        return rest

    def callBackNewStatus(self, id,status,message=""):
        """
        新闻状态回写
        :param id:
        :param status:
        :return:
        """
        rest = ResultData()
        try:
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                summarySet = mydb["summaryIds"]
                sdata = summarySet.find_one({'id':id})#{'status':{"$in":[0,2,9]}}#0:未处理，2：pull,3:处理中 ；9：异常 1：完成
                resetStatus=[]
                if sdata is not None:
                    sid=sdata['id']
                    dstatus=sdata['status']
                    if dstatus ==1:
                        return
                    nerrorCount=1
                    if "errorCount" in sdata:
                        nerrorCount=sdata["errorCount"]+1
                    resetStatus.append(UpdateMany({"id": sid}, {"$set": {"status": status,"errorCount":nerrorCount,"message":message}}))
                if len(resetStatus)>0:
                    result = summarySet.bulk_write(resetStatus)
                    rest.modified_count = result.modified_count
        except:
            Logger.errLineNo(msg="callBackNewStatus error")
            rest.errorData(errorMsg="抱歉，获取修改数据失败，请稍后重试")
        return rest

    def getHisSummaryCode(self,qid="",status="0"):
        """
        :param id:
        :param status: 0:新建；5:开始抓取；6:研报抓取完成；11:开始计算摘要；12:摘要计算完成；
        :return:
        """
        rest = ResultData()
        rest.data=[]
        rest.count=0
        try:
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                contentsSet = mydb["fetchstocks"]
                # 研报发布时间
                querys={"status":status}
                if qid!="" and qid is not None:
                    querys['id']=qid
                # print("getInfomationIds querys:%s"%querys)
                contentsSets = contentsSet.find(querys, {"_id":0}).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                nids = [x for x in contentsSets]
            # print("nids:",len(nids),nids)
            rest.data = nids
            rest.count = len(nids)
        except:
            Logger.errLineNo(msg="getHisSummaryCode error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest



    def getMonitorIds(self,windCodes=[], userIdsPool=[], sumaryFlag="0", allCols=False,
                      rtcolumns={}):
        """
        获取及时新闻资讯编号
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        try:
            rstStocks = self.getStockPool(userIds=userIdsPool)
            stockpool = rstStocks.allCodes
            nstockpool = []
            rtnids = []
            for code in stockpool:
                nstockpool.append(code)
            if len(windCodes) > 0:
                nstockpool = windCodes  # querys["windCode"] = {"$in": windCodes}
            else:
                # querys["windCode"] = {"$in": nstockpool}
                pass
            querys = {}
            if sumaryFlag == "1":
                querys["updateTime"] = {"$exists": False}

            rtCols = {"_id": 0, 'windCode': 1}
            if not allCols:
                rtCols = {"_id": 0, "id": 1, 'windCode': 1}
            if len(rtcolumns) > 0:
                rtCols = rtcolumns
                rtCols.update({'_id': 0})
            # 数据源
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                contentsSet = mydb["monitorIds"]
                # 研报发布时间
                # Logger.info("monitorIds querys:%s"%querys)
                contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                nids=[]
                for ec in contentsSets:
                    if 'windCode'not in ec:
                        continue
                    if ec['windCode'] in nstockpool:
                        nids.append(ec["id"])
                # nids = [x['id'] for x in contentsSets if x['windCode'] in nstockpool]
                rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            rest.data = rtnids
            rest.count = len(rtnids)
        except:
            Logger.errLineNo(msg="getMonitorIds error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest


    def getNewsContents(self, database, dbName, ids, allCols=True):
        """
        通过编号获取分析内容
        :param database:
        :param dbName:
        :param ids:
        :param allCols:
        :return:
        """
        rest = ResultData()
        try:
            with dbmongo.Mongo(database) as md:
                mydb = md.db
                contentsSet = mydb[dbName]
                querys = {"id": {"$in": ids}}
                rtCols = {"_id": 0}
                if not allCols:
                    rtCols = {"_id": 0, "id": 1, }
                contentsSets = contentsSet.find(querys, rtCols)  # 0不显示，1：显示； 查询数据
                if not allCols:
                    nids = [x['id'] for x in contentsSets]
                else:
                    nids = []
                    for s in contentsSets:
                        if "content" in s:
                            sv = s['content']
                            if isinstance(sv, list):
                                s['content'] = "\n".join(sv)
                        nids.append(s)
            rest.data = nids
            rest.count = len(nids)
        except:
            Logger.errLineNo(msg="getNewsContents error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def getInformationData(self, beginTime, endTime, windCodes=[], userIdsPool=[], sumaryFlag="0", allCols=False,
                           rtcolumns={}):
        """
        获取新闻资讯
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        try:
            rstStocks = self.getStockPool(userIds=userIdsPool)
            stockpool = rstStocks.allCodes
            nstockpool = []
            rtnids = []
            for code in stockpool:
                nstockpool.append(code)
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag=sumaryFlag)
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            for idb in infomationDBs:
                database = idb['website']
                dbset = idb['dbset']
                # print("query:website:%s,dbset:%s"%(database,dbset))
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    # 研报发布时间
                    querys = {"publishOn": {'$gte': beginTime, '$lte': endTime}}
                    if len(windCodes) > 0:
                        querys["windCode"] = {"$in": windCodes}
                    else:
                        querys["windCode"] = {"$in": nstockpool}
                    if str(sumaryFlag) == "1":
                        querys["updateTime"] = {"$exists": True}
                    elif str(sumaryFlag) == "-1":
                        pass
                    else:
                        querys["updateTime"] = {"$exists": False}
                    rtCols = {"_id": 0}
                    if not allCols:
                        rtCols = {"_id": 0, "id": 1, }
                    if len(rtcolumns) > 0:
                        rtCols = rtcolumns
                        rtCols.update({'_id': 0})
                    # print("getInformationData querys:%s,rtCols:%s"%(querys,rtCols))
                    # Logger.info("getInformationData querys:%s"%querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                    nids = [x for x in contentsSets]
                    rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            rest.data = rtnids
        except:
            Logger.errLineNo(msg="getInformationData error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def getNewsContents_v1(self, ids=[], sumaryFlag="-1", allCols=True, rtcolumns={}):
        """
        获取资讯内容
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        rest.data = []
        try:
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag=sumaryFlag)
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            qdbs = {}
            for idb in infomationDBs:
                idprefix = idb['idprefix']
                qdbs[idprefix] = idb

            rtnids = []
            for qid in ids:
                qprefix = str(qid).split("_")[0] + "_"
                if qprefix in qdbs:
                    qdbset = qdbs[qprefix]
                    database = qdbset['website']
                    dbset = qdbset['dbset']
                    with dbmongo.Mongo(database) as md:
                        mydb = md.db
                        contentsSet = mydb[dbset]
                        # 研报发布时间
                        querys = {"id": qid}
                        rtCols = {"_id": 0}
                        if not allCols:
                            rtCols = {"_id": 0, "id": 1, }
                        else:
                            rtCols = {"_id": 0}
                        if len(rtcolumns) > 0:
                            rtCols = rtcolumns
                            rtCols.update({'_id': 0})
                        # print("getInformationData querys:%s,rtCols:%s"%(querys,rtCols))
                        # Logger.info("getInformationData querys:%s"%querys)
                        contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                        nids = [x for x in contentsSets]
                        rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            rest.data = rtnids
            rest.count = len(rtnids)
        except:
            Logger.errLineNo(msg="getInfomationIds error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def fillSummary(self, database, dbName, id, summary):
        """
        回填summary
        :param database:
        :param dbName:
        :param id:
        :param summary:
        :return:
        """
        rest = ResultData()
        try:
            with dbmongo.Mongo(database) as md:
                mydb = md.db
                contentsSet = mydb[dbName]
                querys = {"id": id}
                updateCell = {"summary": summary, "updateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                contentsSets = contentsSet.update_one(querys, {"$set": updateCell})  # 0不显示，1：显示； 查询数据
                modified_count = contentsSets.modified_count
            rest.data = [{"id": id, "status": modified_count}]
        except:
            Logger.errLineNo(msg="fillSummary error")
            rest.errorData(errorMsg="抱歉，保存数据失败，请稍后重试")
        return rest

    def fillAudioText(self,audioId, audioText=None):
        """
        回填audio 音频解析文本
        :param id:
        :param summary:
        :return:
        """
        rest = ResultData()
        try:
            if (audioText == "" or audioText is None) :
                rest.errorData("音频内容不能为空!")
                return rest
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="-1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            qdbs = {}
            for idb in infomationDBs:
                idprefix = idb['idprefix']
                qdbs[idprefix] = idb
            qprefix = str(audioId).split("_")[0] + "_"
            if qprefix in qdbs:
                qdbset = qdbs[qprefix]
                database = qdbset['website']
                dbset = qdbset['dbset']
                updateCell = {}
                if audioText is not None:
                    updateCell['audioText']=str(audioText).strip()
                    updateCell['audioTextUpdateTime']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    querys = {"id": audioId}
                    contentsSets = contentsSet.update_one(querys, {"$set": updateCell})
                    modified_count = contentsSets.modified_count
                    rest.data = [{"id": audioId, "status": modified_count}]
            else:
                rest.data = []
        except:
            Logger.errLineNo(msg="fillAudioText error")
            rest.errorData(errorMsg="抱歉，保存数据失败，请稍后重试")
            Logger.error("audioId:%s,audioText:%s"%(audioId,audioText))
            wx_send.send_wx_msg_operation("audioId[%s]audioText save error!"%audioId)
        return rest

    def fillSummary_v1(self, id, summary_zh=None, summary_en=None, uTitleZh=None, uTitleEn=None, importance_score=None,
                       symbols_score=[], summarys=[], summarys2=[], publicSentiment=None, reason=None,relationCompanies={},
                       influencedCompanies={},summaryFormat=None,trans_newcontents=None):
        #回填summary
        rest = ResultData()
        try:
            if (summary_zh == "" or summary_zh is None) and (summary_en == "" or summary_en is None):
                rest.errorData("中文、英文摘要不能同时为空!")
                return rest
            if importance_score is not None:
                if not tool_utils.isNumber(importance_score):
                    rest.errorData("[importance_score]必须为数字类型!")
                    return rest
                importance_score = float(importance_score)
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="-1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            qdbs = {}
            for idb in infomationDBs:
                idprefix = idb['idprefix']
                qdbs[idprefix] = idb
            qprefix = str(id).split("_")[0] + "_"
            if qprefix in qdbs:
                qdbset = qdbs[qprefix]
                database = qdbset['website']
                dbset = qdbset['dbset']
                updateCell = {}
                if summary_zh is not None:
                    updateCell['summary'] = str(summary_zh).strip()
                if summary_en is not None:
                    updateCell['summary_en'] = str(summary_en).strip()
                if uTitleZh is not None:
                    updateCell['title_zh'] = uTitleZh
                if uTitleEn is not None:
                    updateCell['title_en'] = uTitleEn
                if importance_score is not None:
                    updateCell['importance_score'] = importance_score
                if len(symbols_score) > 0:
                    updateCell['symbols_score'] = symbols_score
                if len(summarys) > 0:
                    updateCell['summarys'] = summarys
                if len(summarys2) > 0:
                    updateCell['summarys2'] = summarys2
                if publicSentiment is not None:
                    updateCell['publicSentiment'] = str(publicSentiment).lower()
                if reason is not None:
                    updateCell['reason'] = reason
                if trans_newcontents is not None:
                    updateCell['transSummaryText'] = trans_newcontents
                if len(relationCompanies) > 0:
                    updateCell['relationCompanies'] = relationCompanies
                if len(influencedCompanies) > 0:
                    updateCell['influencedCompanies'] = influencedCompanies
                updateCell['updateTime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                qwindCode = ""
                qtitle = ""
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    querys = {"id": id}
                    contentOne = contentsSet.find_one(querys)
                    if contentOne is not None:
                        dataType = contentOne['dataType']
                        if "textLanguage" in contentOne:
                            if contentOne['textLanguage'] in ['zh-cn', 'zh-tw', 'ja']:
                                # 中英文标题互换
                                updateCell['title_zh'] = contentOne['title']  # 中文
                                updateCell['old_title'] = contentOne['title']  # 原始标题
                                updateCell['title'] = uTitleZh  # 英文
                        if dataType in ['Press Releases']:
                            reportData = contentOne['reportData']
                            rdtime = datetime.datetime.strptime(reportData, '%Y-%m-%d')
                            updateCell['title_zh'] = "%s年%s月%s日业绩公告" % (rdtime.year, rdtime.month, rdtime.day)

                    contentsSets = contentsSet.update_one(querys, {"$set": updateCell})
                    modified_count = contentsSets.modified_count
                # with dbmongo.Mongo("common") as md:
                #     mydb = md.db
                #     mmSet = mydb["monitorIds"]
                #     mmquerys = {"id": id}
                #     mmOne = mmSet.find_one(querys)
                #     if mmOne is not None:
                #         mmCell={}
                #         mmCell['updateTime']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                #         mmCell['summaryFlag']="1"
                #         mmSet.update_one(mmquerys, {"$set": mmCell})
                #         #微信消息通知
                #         wx_send.send_wx_msg_operation("%s %s Summary processing completed!"%(qwindCode,qtitle))
                if summaryFormat is None:
                    with dbmongo.Mongo("common") as md:
                        mydb = md.db
                        mmSet = mydb["summaryIds"]
                        mmSet.update_many({"id": id}, {"$set": {'status': 1, 'pushTime': datetime.datetime.now()}})
                rest.data = [{"id": id, "status": modified_count}]
                # 更新券商上传的会议状态。
                #会议纪要summary计算完成通知。
                self.summaryFinishSend(id)
            else:
                rest.data = []
        except:
            Logger.errLineNo(msg="fillSummary error")
            rest.errorData(errorMsg="抱歉，保存数据失败，请稍后重试")
            Logger.error(
                "id:%s,summary_zh:%s,summary_en:%s,uTitleZh:%s,importance_score:%s,symbols_score:%s,summarys:%s" % (
                id, summary_zh, summary_en, uTitleZh, importance_score, symbols_score, summarys))
            wx_send.send_wx_msg_operation("id[%s]summary save error!" % id)
        return rest

    def updateSourceContent(self, rowid, transSummaryText):
        """
        回填 修正后的原文内容updateSourceContent
        :param id:
        :param transSummaryText:
        :return:
        """
        rest = ResultData()
        try:
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="-1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            qdbs = {}
            for idb in infomationDBs:
                idprefix = idb['idprefix']
                qdbs[idprefix] = idb
            qprefix = str(rowid).split("_")[0] + "_"
            if qprefix in qdbs:
                qdbset = qdbs[qprefix]
                database = qdbset['website']
                dbset = qdbset['dbset']
                updateCell = {}
                if transSummaryText is not None:
                    updateCell['transSummaryText'] = str(transSummaryText).strip()
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    querys = {"id": rowid}
                    contentsSets = contentsSet.update_one(querys, {"$set": updateCell})
                    modified_count = contentsSets.modified_count
                    rest.data = [{"id": rowid, "status": modified_count}]
            else:
                rest.data = []
        except:
            Logger.errLineNo(msg="updateSourceContent error")
            rest.errorData(errorMsg="抱歉，保存数据失败，请稍后重试")
        return rest

    def summaryFinishSend(self,audioId):
        try:
            rest=ResultData()
            if not str(audioId).startswith("Audio_"):
                rest.errorData(errorMsg='非会议纪要摘要不推送消息。')
                return rest
            rest = self.getNewsContents_v1([audioId])
            if not rest.errorFlag:
                return rest
            else:
                rtdt = rest.data
                if len(rtdt) > 0:
                    u_util = userMode.cuserMode()
                    ucDF = u_util.getUserCompanyRF()
                    usser_dt = {}
                    for uc in ucDF.itertuples():
                        userid = str(uc.USERID)
                        usser_dt[userid] = {'userName': uc.USERREALNAME, "companyName": uc.SHORTCOMPANYNAME,
                                            "emailName": uc.USERNAME}
                    qdt = rtdt[0]
                    if "cuserId" in qdt:
                        cuserId = qdt['cuserId']
                        title = qdt['title']
                        if cuserId in usser_dt:
                            udt = usser_dt[cuserId]
                            usernickname = udt['userName']
                            to_addr = udt['emailName']
                            # print("to_addr:", to_addr)
                            # if not (to_addr == "robbyxxh@126.com"):
                            #     rest = ResultData(errorMsg="邮件消息发送成功")
                            #     return rest
                            scontents = """尊敬的用户 您好，%s 摘要内容计算完成，敬请<a href='https://www.daohequant.com/copilotportfolio.html' target='_blank'>查阅</a>。""" % title
                            title = "%s 摘要计算完成通知" % (title)
                            # print(to_addr, usernickname, title)
                            Logger.info("email send audioId:%s" % audioId)
                            Logger.info("email send to_addr:%s" % to_addr)
                            Logger.info("email send usernickname:%s" % usernickname)
                            Logger.info("email send title:%s" % title)
                            Logger.info("email send scontents:%s" % scontents)
                            rtstr = sendemail.sendemailPinnacle(to_addr, usernickname, title, scontents)
                            Logger.info("email send result:%s" % rtstr)
                            if int(str(rtstr).split("|")[0]) > 0:
                                rest = ResultData(errorMsg="邮件消息发送成功")
                            else:
                                rest = rest.errorData(errorMsg=rtstr)
                        else:
                            rest = ResultData(errorMsg="未关联用户")
                        Logger.info("email send result:%s" % rest.toDict())
                        return rest
                else:
                    rest.errorData(errorMsg="数据不存在")
                    return rest
        except:
            Logger.errLineNo(msg="summaryFinishSend error")
            rest.errorData(errorMsg="邮件消息推送异常")
            return rest
    def getMergeTickers(self):
        #股票代码合并
        mergerTickers={}
        with dbmongo.Mongo("StockPool") as md:
            mydb = md.db
            # 需要获取最新的
            Reviewset = mydb["stocks"]
            ReviewDBIds = Reviewset.find_one({"stockTypes":"mergeCodes"}, {"_id": 0})  # 0不显示，1：显示； 查询数据
            if ReviewDBIds is not None:
                for rc in ReviewDBIds['stocks']:
                    mergerTickers.update(rc)
        return mergerTickers

    def fmtEndCode(self,windCode):
        mergeTicks=self.getMergeTickers()
        fillWindCode = mergeTicks[windCode] if windCode in mergeTicks else windCode
        return fillWindCode


    def addNewContent(self, database, dbName, datas=[]):
        """
        :param database:
        :param dbName:
        :param datas:
        :return:
        """
        rest = ResultData()
        try:
            inserted_ids = []
            with dbmongo.Mongo(database) as md:
                mydb = md.db
                contentsSet = mydb[dbName]
                for data in datas:
                    id = data["id"]
                    if "windCode" in data:
                        qWindCode = data["windCode"]
                        data["windCode"] = self.fmtEndCode(qWindCode)
                        data["symbols"] = str(data["windCode"]).split(".")[0]
                    querys = {"id": id}
                    # print("data:",data)
                    # continue
                    if contentsSet.find_one(querys) is None:
                        contentsSet.insert_one(data)
                        inserted_ids.append({"id": id, 'status': "1"})
                    else:
                        inserted_ids.append({"id": id, 'status': "0"})  # 已经存在
            rest.data = inserted_ids
        except:
            Logger.errLineNo(msg="addNewContent error")
            rest.errorData(errorMsg="抱歉，添加数据失败，请稍后重试")
        return rest

    def delNewContent(self, database, dbName, ids=[]):
        """
        删除内容
        :param database:
        :param dbName:
        :param datas:
        :return:
        """
        rest = ResultData()
        try:
            deleted_count = []
            with dbmongo.Mongo(database) as md:
                mydb = md.db
                contentsSet = mydb[dbName]
                for id in ids:
                    querys = {"id": id}
                    rsd = contentsSet.delete_one(querys)
                    deleted_count = rsd.deleted_count
                    deleted_count.append({"id": id, 'status': str(deleted_count)})
            rest.data = deleted_count
        except:
            Logger.errLineNo(msg="delNewContent error")
            rest.errorData(errorCode="")
        return rest


    def contentLable(self,id,label=None,labelText=None):
        """
        新闻摘要标注：好，不好
        :param database:
        :param dbName:
        :param datas:
        :return:
        """
        rest = ResultData()
        try:
            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            qdbs = {}
            for idb in infomationDBs:
                idprefix = idb['idprefix']
                qdbs[idprefix] = idb
            qprefix = str(id).split("_")[0] + "_"
            modified_count=-1
            if qprefix in qdbs:
                qdbset = qdbs[qprefix]
                database = qdbset['website']
                dbset = qdbset['dbset']
                querys = {"id": id}
                if label is None:
                    label=""
                if labelText is None:
                    labelText=""
                updateCell = {"label": label,"labelText":labelText}
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    fone = contentsSet.find_one({"id":id})  # 0不显示，1：显示； 查询数据
                    if fone is not None:
                        if 'label' in fone:
                            if fone['label']==label:
                                updateCell['label']=""
                        contentsSets = contentsSet.update_one(querys, {"$set": updateCell})  # 0不显示，1：显示； 查询数据
                        fone = contentsSet.find_one({"id":id})  # 0不显示，1：显示； 查询数据
                        label=fone['label']
                modified_count = contentsSets.modified_count
            rest.data = {"id": id, "status": modified_count,'label':label}
        except:
            Logger.errLineNo(msg="contentLable error")
            rest.errorData(errorMsg="抱歉，标注信息异常，请稍后重试")
        return rest

    def getEarningsForecastData(self,preDays=-1,nextDays=7):
        #展示一周出业绩的公司
        rst=ResultData()
        try:
            efWindCodes = self.getPressReleases(preDays=preDays,nextDays=nextDays)
            quarterlyReports =self.getCodeQuarterDay(efWindCodes)
            fdatas=[]
            qReports=[]
            for r,v in quarterlyReports.items():
                # print("r:",r,v)
                v['bbgName']=str(v['bbgName']).title()
                wdname = str(v['windName'])
                if wdname.isalpha():
                    v['windName'] =wdname
                else:
                    cks=str(v['windName'])
                    # cks = cks[0:cks.find("(")]
                    v['windName']=cks.title()
                qReports.append(v['qReport'])
                fdatas.append(v)
            fdatas=sorted(fdatas,key=lambda x: x['qReport'])
            qReports=sorted(set(qReports))
            redatas={}
            for rd in qReports:
                rds=[]
                for fd in fdatas:
                    if rd ==fd['qReport']:
                        rds.append(fd)
                redatas[rd]=rds
            # for r,v in redatas.items():
            #     print(r,v)
            #按日期排序分组展示
            rst.qReports=qReports
            rst.data=redatas
        except:
            errorMsg = "抱歉，获取数据异常，请稍后处理！"
            Logger.errLineNo(msg=errorMsg)
            rst.errorData(errorMsg=errorMsg)
        return rst

    def getQuarterlyReport(self,preDays=-3,nextDays=1):
        nt = datetime.datetime.now()
        preDay = (nt + datetime.timedelta(days=preDays)).strftime("%Y-%m-%d")
        nextDay = (nt + datetime.timedelta(days=nextDays)).strftime("%Y-%m-%d")
        qdrts = []
        with dbmongo.Mongo("common") as md:
            mydb = md.db
            # 需要获取最新的
            qReportSet = mydb["quarterlyReport"]
            SPXStocks = qReportSet.find({"qReport": {"$gte": preDay, "$lte": nextDay}}, {"_id": 0})
            for u in SPXStocks:
                qdrts.append(u)
        return qdrts

    def getPressReleases(self,preDays=-3,nextDays=0):
        # 跟踪标普500出业绩的公司
        rest=self.getStockPool(trackFlag="1",preDays=preDays,nextDays=nextDays)
        needCodes=rest.allCodes
        return needCodes

    def getCodeQuarterDay(self,windCodes):
        qdrts={}
        with dbmongo.Mongo("common") as md:
            mydb = md.db
            # 需要获取最新的
            qReportSet = mydb["quarterlyReport"]
            SPXStocks = qReportSet.find({"windCode": {"$in": windCodes}}, {"_id": 0})
            for u in SPXStocks:
                qdrts[u['windCode']] =u  # 用户订阅的股票池列表
        return qdrts

    def getMaxDataGroupWindCode(self,website,dbSet):
        codeTranscriptsMaxDay_dt={}
        try:
            if website is None or dbSet is None:
                return codeTranscriptsMaxDay_dt
            if website =="" or dbSet =="":
                return codeTranscriptsMaxDay_dt
            with dbmongo.Mongo(website) as mdb:
                mydb=mdb.db
                collection=mydb[dbSet]
                pipeline = [
                    {
                        "$group": {
                            "_id": "$windCode",
                            "maxPublishOn": {"$max": "$publishOn"}
                        }
                    }
                ]
                result = list(collection.aggregate(pipeline))
                for entry in result:
                    wind_code = entry["_id"]
                    max_publish_on = entry["maxPublishOn"]
                    codeTranscriptsMaxDay_dt[wind_code]={'windCode': wind_code, 'publishOn':max_publish_on,}
        except PyMongoError as e:
            Logger.errLineNo(msg="getMaxDataGroupWindCode error")

        return codeTranscriptsMaxDay_dt

    def getStockPlateList(self):
        stockPlateList=[]
        # 板块分类股票
        with dbmongo.Mongo("StockPool") as md:
            mydb = md.db
            # 需要获取最新的
            plateCodesSet = mydb["plateCodes"]
            plateCodesStocks = plateCodesSet.find({"disabled": {"$exists": False}}, {"_id": 0}).sort([('ord', 1)])
            for u in plateCodesStocks:
                stockPlateList.append(u)
        return  stockPlateList

    def getIdustryPlateList(self,industryId=None):
        industryPlateList=[]
        # 行业板块分类股票
        with dbmongo.Mongo("common") as md:
            mydb = md.db
            # 需要获取最新的
            plateCodesSet = mydb["industryItems"]
            if industryId is None or  industryId in ['Industry']:
                plateCodesStocks = plateCodesSet.find({"disabled": {"$exists": False}}, {"_id": 0}).sort([('ord', 1)])
            else:
                plateCodesStocks = plateCodesSet.find({"id":industryId, "disabled": {"$exists": False}}, {"_id": 0}).sort([('ord', 1)])
            for u in plateCodesStocks:
                industryPlateList.append(u)
        return  industryPlateList

    def getIdustryPlateRange(self,industryId=None):
        #将报告编号或公司编号按集合进行分类
        #{'website@yipit': {'website': 'website', 'dbset': 'yipit', 'windCodes': ['6af0997d-']}
        idustryPlates=self.getIdustryPlateList(industryId=industryId)
        webSiteCodes = {}
        idustryCodes = {}
        for idusd in idustryPlates:
            sources = idusd['sources']
            allCodes=[]
            for scodes in sources:
                website = scodes['website']
                dbset = scodes['dbset']
                windCodes = scodes['windCodes']
                idks = website + "@" + dbset
                if idks in webSiteCodes:
                    fwindCode = webSiteCodes[idks]['windCodes']
                    fwindCode.extend(windCodes)
                    webSiteCodes[idks]['windCodes'] = fwindCode
                else:
                    webSiteCodes[idks] = {'website': website, "dbset": dbset, "windCodes": windCodes}
                allCodes.extend(windCodes)
            idustryCodes[idusd['id']]={"id":idusd['id'],"name":idusd['name'],"windCodes":allCodes}
        return  webSiteCodes,idustryCodes

    def getIdustryPlateNameList(self):
        industryPlateList=[]
        # 行业板块分类股票
        with dbmongo.Mongo("common") as md:
            mydb = md.db
            # 需要获取最新的
            plateCodesSet = mydb["industryItems"]
            plateCodesStocks = plateCodesSet.find({"disabled": {"$exists": False}}, {"_id": 0,"id":1,"name":1,"ord":1}).sort([('ord', 1)])
            for u in plateCodesStocks:
                industryPlateList.append(u)
        return  industryPlateList

    def getStockPlateList2(self):
        stockPlateList=[]
        dbstockPlateList=[]
        # 板块分类股票
        with dbmongo.Mongo("StockPool") as md:
            mydb = md.db
            # 需要获取最新的
            plateCodesSet = mydb["plateCodes"]
            plateCodesStocks = plateCodesSet.find({"disabled": {"$exists": False}}, {"_id": 0}).sort([('ord', 1)])
            areas=set()
            for u in plateCodesStocks:
                areas.add(u['area'])
                dbstockPlateList.append(u)
            areas=['US','HK','CH','JP']
            areas_codes_US=[]
            areas_codes_HK=[]
            areas_codes_CH=[]
            areas_codes_JP=[]
            for sp in dbstockPlateList:
                if  sp['area'] == 'US':
                    areas_codes_US.append({"id":sp['id'],"name":sp['name']})
                elif  sp['area'] == 'HK':
                    areas_codes_HK.append({"id":sp['id'],"name":sp['name']})
                elif  sp['area'] == 'CH':
                    areas_codes_CH.append({"id":sp['id'],"name":sp['name']})
                elif  sp['area'] == 'JP':
                    areas_codes_JP.append({"id":sp['id'],"name":sp['name']})
            stockPlateList.append({'area':'US','name':'美国','data':areas_codes_US})
            stockPlateList.append({'area':'HK','name':'香港','data':areas_codes_HK})
            stockPlateList.append({'area':'CH','name':'中国','data':areas_codes_CH})
            stockPlateList.append({'area':'JP','name':'中国','data':areas_codes_JP})

            #
            # stockPlate_dt["US"] ={'area':'US','name':'美国','data':areas_codes_US}
            # stockPlate_dt["HK"] = {'area':'HK','name':'香港','data':areas_codes_HK}
            # stockPlate_dt["HK"] = {'area':'CH','name':'中国','data':areas_codes_CH}
            # stockPlate_dt["HK"] = {'area':'JP','name':'中国','data':areas_codes_JP}

        return  stockPlateList

    def getStockPlateCode(self,plateId):
        stockPlateList=[]
        # 板块分类股票
        with dbmongo.Mongo("StockPool") as md:
            mydb = md.db
            # 需要获取最新的
            plateCodesSet = mydb["plateCodes"]
            if plateId=="0" or plateId is None:
                plateCodesStocks = plateCodesSet.find({"disabled": {"$exists": False}}, {"_id": 0})
                for s in plateCodesStocks:
                    stockPlateList.extend(s['windCodes'])
            elif plateId in ['US','HK','CH','JP']:
                plateCodesStocks = plateCodesSet.find({"area":plateId ,"disabled": {"$exists": False}}, {"_id": 0})
                for s in plateCodesStocks:
                    stockPlateList.extend(s['windCodes'])
            else:
                plateCodesStocks = plateCodesSet.find_one({"disabled": {"$exists": False},"id":plateId}, {"_id": 0})
                if plateCodesStocks is not None:
                    stockPlateList=plateCodesStocks['windCodes']
        return  stockPlateList


    def getCodeInfo(self,windCode):
        with dbmongo.Mongo("StockPool") as md:
            mydb = md.db
            # 需要获取最新的
            if str(windCode)[-2:] in [".O", ".N", '.A']:
                stockType = 'USA'
                Reviewset = mydb["stocklist"]
            elif str(windCode)[-3:] in stock_utils.MARKET_CH_LIST:
                stockType = "CH"
                Reviewset = mydb["chStocks"]
            elif str(windCode)[-3:] in ['.HK']:
                stockType = "HK"
                Reviewset = mydb["hkStocks"]
            elif str(windCode)[-3:] in ['.KS']:
                stockType = "KS"
                Reviewset = mydb["ksStocks"]
            elif str(windCode).endswith(".T"):
                stockType = "JP"
                Reviewset = mydb["jpStocks"]
            elif str(windCode).split(".")[-1] in ['TW', 'TWO']:
                stockType = "TT"
                Reviewset = mydb["twStocks"]
            else:
                return {}
            tickerInfo = Reviewset.find_one({'windCode': {"$in":[windCode]}}, {"_id": 0})  # 0不显示，1：显示； 查询数据
            if tickerInfo is None:
                return {}
        return tickerInfo

    def getSycStockPool(self, userIds=[],filterUsers=[]):
        rest = ResultData()
        rest.fsymbols = []
        try:
            fsymbols = []
            if len(userIds) == 0:
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    userstocksSet = mydb["userstocks"]
                    userStocks = userstocksSet.find({"disabled": {"$exists": False}}, {"_id": 0})
                    for u in userStocks:
                        if u['userId'] in filterUsers:
                            continue
                        symbols = u['stocks']
                        fsymbols.extend(symbols)  # 用户订阅的股票池列表
                # 系统默认设定的股票
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    stocksSet = mydb["stocks"]
                    defaultStocks = stocksSet.find({"stockTypes": "Default"}, {"_id": 0})
                    for u in defaultStocks:
                        symbols = u['stocks']
                        fsymbols.extend(symbols)  # 用户订阅的股票池列表
                # 板块分类股票
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    plateCodesSet = mydb["plateCodes"]
                    plateCodesStocks = plateCodesSet.find({"disabled": {"$exists": False}}, {"_id": 0})
                    for u in plateCodesStocks:
                        symbols = u['windCodes']
                        fsymbols.extend(symbols)
                fsymbols = list(set(fsymbols))
                rest.fsymbols = fsymbols
            else:
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    # 需要获取最新的
                    userstocksSet = mydb["userstocks"]
                    querys = {}
                    querys["userId"] = {"$in": userIds}
                    userStocks = userstocksSet.find(querys, {"_id": 0})
                    for u in userStocks:
                        symbols = u['stocks']
                        fsymbols.extend(symbols)  # 用户订阅的股票池列表
                    fsymbols = list(set(fsymbols))
                    rest.fsymbols = fsymbols

            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                stocklistset = mydb["stocklist"]
                stocks = stocklistset.find({}, {"_id": 0, 'windCode': 1, 'ord': 1})
                ordCodes = [x for x in stocks if x['windCode'] in rest.fsymbols]
                # order 排序
                # ordCodes =sorted(ordCodes,key=lambda x:x['ord'])
            ordCodes = sorted(ordCodes, key=lambda x: x['ord'])
            hisfsymbols=rest.fsymbols
            rest.fsymbols=[s['windCode'] for s in ordCodes]
            ordOutherCodes = list(set(hisfsymbols) - set(rest.fsymbols))
            if len(ordOutherCodes)>0:
                rest.fsymbols.extend(ordOutherCodes)
            #股票代码按市值排序
        except:
            Logger.errLineNo(msg="getStockPool error")
            rest.errorData(errorCode="")
            print(traceback.format_exc())
        return rest

    def getAuthorIds(self):
        authorIddt={}
        with dbmongo.Mongo('common') as md:
            mydb = md.db
            qcookies = mydb["researchAuthor"]
            # _query = {'source': "neoubs"}
            _query = {}
            qdatas = qcookies.find(_query)
            for qdata in qdatas:
                authors = qdata['authors']
                authorIds=[x['authorId'] for x in authors]
                authorIddt[qdata['source']]={"authors":authors,"authorIds":authorIds}
        return authorIddt
    def getStockPool(self,userIds=[],trackFlag="1",preDays=-2,nextDays=0,filterUsers=[],dataType=None):
        rest = ResultData()
        rest.fsymbols = [] #系统默认股票
        rest.trackCodes=[] #跟踪股票
        rest.allCodes=[] #
        try:
            #系统股票池 用户自定义股票+系统默认股票+板块
            rtsp=self.getSycStockPool(userIds=userIds,filterUsers=filterUsers)
            sysCodes = rtsp.fsymbols
            rest.fsymbols=sysCodes
            allCodes=sysCodes.copy()
            #SPX500
            spx500 = []
            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                # 需要获取最新的
                SPXSet = mydb["stocks"]
                SPXStocks = SPXSet.find({"stockTypes": {"$in": ['SPX']}}, {"_id": 0})
                for u in SPXStocks:
                    symbols = u['stocks']
                    spx500.extend(symbols)  # 用户订阅的股票池列表
            #跟踪股票 跟踪股票需要包含系统股票池
            if trackFlag=="1":
                qdrts = self.getQuarterlyReport(preDays=preDays, nextDays=nextDays)
                # 跟踪出业绩股票  股票池+标普500
                ckAllCodes = list(set(spx500 + sysCodes))
                codeDataMaxDay_dt={}
                if dataType in ["sAts_"]:
                    # 过滤已经抓到的出了业绩的代码
                    database,dbset=self.getDBSetInfo(dataType)
                    codeDataMaxDay_dt=self.getMaxDataGroupWindCode(database,dbset)
                trackCodes = []
                preDtime=datetime.datetime.now() + datetime.timedelta(days=-2)
                itrackCodes=[]
                for qd in qdrts:
                    ckqd=qd['windCode']
                    if ckqd in ckAllCodes:#在标普500 + 自选股内
                        itrackCodes.append(ckqd)
                #过滤已经抓取了报告
                for ckqd in itrackCodes:
                    if ckqd in codeDataMaxDay_dt:
                        codeDataMax = codeDataMaxDay_dt[ckqd]
                        if "publishOn" in codeDataMax:
                            publishOn = codeDataMax['publishOn']
                            pdtime = datetime.datetime.strptime(publishOn, '%Y-%m-%d %H:%M:%S')
                            if pdtime > preDtime:
                                continue
                    trackCodes.append(ckqd)
                #按市值排序
                with dbmongo.Mongo("StockPool") as md:
                    mydb = md.db
                    stocklistset = mydb["stocklist"]
                    stocks = stocklistset.find({}, {"_id": 0, 'windCode': 1, 'ord': 1})
                    ordCodes = [x for x in stocks if x['windCode'] in trackCodes]
                    # order 排序
                    # ordCodes =sorted(ordCodes,key=lambda x:x['ord'])
                ordCodes = sorted(ordCodes, key=lambda x: x['ord'])
                trackfsymbols = [s['windCode'] for s in ordCodes]
                ordOutherCodes = list(set(trackCodes) - set(trackfsymbols))
                if len(ordOutherCodes) > 0:
                    trackfsymbols.extend(ordOutherCodes)
                allCodes.extend(trackfsymbols)
                rest.trackCodes = trackfsymbols
            allCodes = list(set(allCodes))
            rest.allCodes = allCodes
        except:
            Logger.errLineNo(msg="getStockPoolNews error")
            rest.errorData(errorMsg="getStockPoolNews error")
        return rest

    def getDBSetInfo(self,dataType):
        # 通过前缀识别数据源
        dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
        if not dbRst.errorFlag:
            return dbRst
        infomationDBs = dbRst.information_datas
        for idb in infomationDBs:
            idprefix = idb['idprefix']
            if  idprefix == dataType:
                database = idb['website']
                dbset = idb['dbset']
                return database,dbset
        return None,None

    def getSummaryNum(self, sids=[],sumaryFlag="1"):
        # 数据源
        dbRst = mg_cfg.getInfomationDataBase(sumaryFlag=sumaryFlag)
        if not dbRst.errorFlag:
            return dbRst
        infomationDBs = dbRst.information_datas
        qdbs = {}
        for idb in infomationDBs:
            idprefix = idb['idprefix']
            qdbs[idprefix] = idb
        rtnids = []
        for qid in sids:
            qprefix = str(qid).split("_")[0] + "_"
            if qprefix in qdbs:
                qdbset = qdbs[qprefix]
                database = qdbset['website']
                dbset = qdbset['dbset']
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    myset = mydb[dbset]
                    # document_count = myset.count_documents({"id":qid,"summary":{"$ne":""}})
                    document_dt = myset.find_one({"id":qid,"summary":{"$ne":""}})
                    if document_dt is None:
                        rtnids.append({"id":qid,"num":0})
                    else:
                        cuserId="-1"
                        if "cuserId" in document_dt:
                            cuserId=document_dt['cuserId']
                        rtnids.append({"id": qid,"cuserId": cuserId, "num": 1, "dataType": document_dt['dataType'], "source": document_dt['source'],})
        if len(rtnids)==0:
            rtnids=[{"id": id, "num": 0}]
        return  rtnids
    def getTickers(self, websit, dbname, windCodes):
        rest = ResultData()
        rest.tickers = []
        try:
            with dbmongo.Mongo(websit) as md:
                mydb = md.db
                # 需要获取最新的
                userstocksSet = mydb[dbname]
                querys = {}
                querys["windCode"] = {"$in": windCodes}
                userStocks = userstocksSet.find(querys, {"_id": 0})
                rest.tickers = [s for s in userStocks]
        except:
            Logger.errLineNo(msg="getTickers error")
            rest.errorData(errorCode="")
            print(traceback.format_exc())
        return rest

    def createSummary(self, beginTime, endTime, dataBases=[], windCode=None, allCols=False):
        """
        生成摘要数据
        :param beginTime:
        :param endTime:
        :param dataBases[{"dataBase":'',"dbName":''}]:
        :return:
        """
        rest = ResultData()
        try:
            dbmap = {}
            wxUserIds = []
            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                summarySourceSet = mydb["summarySource"]
                smp = summarySourceSet.find_one({"id": 'pdf'})
                if smp is not None:
                    dbmap = smp['mp']
                sendTest = summarySourceSet.find_one({"id": 'isTest'})
                sendTestFlag = False
                if sendTest is not None:
                    sendTestValue = sendTest['value']
                    if str(sendTestValue) == "1":
                        sendTestFlag = True
                sendModes = summarySourceSet.find_one({"id": 'sendMode'})
                if sendModes is not None:
                    wxUserIds = sendModes['wxUserId']
                    testUserId = sendModes['testUserId']
                if sendTestFlag:
                    wxUserIds = testUserId

            pdfCtx = tools_utils.sendFileDir()
            pdffile = os.path.join(pdfCtx, 'pdf')
            if not os.path.exists(pdffile):
                os.makedirs(pdffile)

            # 创建一个新的PDF文档
            pdfname = "%s_%s.pdf" % ("summary", datetime.datetime.now().strftime("%m%d%H%M"))
            pdffilename = os.path.join(pdffile, pdfname)
            summaryDatas = []
            # testSummary="summaryText"
            # testTime="insertTime"
            testSummary = "summary"
            testTime = "publishOn"
            for dbs in dataBases:
                dataBase = dbs['dataBase']
                dbName = dbs['dbName']
                sdbmap = "%s@%s" % (dataBase, dbName)
                # print("sdbmap:",sdbmap)
                vdbmap = ""
                if sdbmap in dbmap:
                    vdbmap = dbmap[sdbmap]
                with dbmongo.Mongo(dataBase) as md:
                    mydb = md.db
                    contentsSet = mydb[dbName]
                    querys = {testTime: {'$gte': beginTime, '$lte': endTime}}
                    if windCode is not None:
                        querys['windCode'] = windCode
                    # if str(sumaryFlag) == "1":
                    #     querys["updateTime"] = {"$exists": True}
                    # else:
                    querys["updateTime"] = {"$exists": True}
                    querys["summary"] = {"$exists": True}
                    rtCols = {"_id": 0}
                    if not allCols:
                        rtCols = {"_id": 0, "id": 1, "title": 1, testSummary: 1, "updateTime": 1, testTime: 1,
                                  "publishOn": 1, "windCode": 1, "tickers": 1,
                                  "source_url": 1, "summaryText": 1, "source": 1, 'dataType': 1}
                        rtCols = {"_id": 0}
                    print("createSummary querys:", querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([("windCode", 1)])
                    for s in contentsSets:
                        s['vdbmap'] = vdbmap
                        summaryDatas.append(s)

            # 从JSON数据获取报告列表
            json_data = summaryDatas

            # https://zhuanlan.zhihu.com/p/318390273
            from reportlab.lib.pagesizes import letter
            # from reportlab.platypus import SimpleDocTemplate, Paragraph
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable

            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib import fonts
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont
            from reportlab.pdfgen import canvas
            # 设置字体文件路径
            if tools_utils.isAlyPlatform():
                font_path = r"/opt/service/dsidmfactors/login/methods/portfolio/factsheet/fonts/msyhLight.ttf"
                font_bold = r"/opt/service/dsidmfactors/login/methods/portfolio/factsheet/fonts/微软雅黑Bold.ttf"
            else:
                font_path = r"C:\work\project\web\main\login\methods\portfolio\factsheet\fonts\msyhLight.ttf"
                font_bold = r"C:\work\project\web\main\login\methods\portfolio\factsheet\fonts\微软雅黑Bold.ttf"

            # 注册中文字体
            pdfmetrics.registerFont(TTFont("ChineseFont", font_path))
            pdfmetrics.registerFont(TTFont("ChineseFont_bold", font_bold))
            doc = SimpleDocTemplate(pdffilename, pagesize=letter)
            # 定义样式
            styles = getSampleStyleSheet()
            # styles.add(fontName="ChineseFont", fontSize=12)
            # styles.add(ParagraphStyle(fontName='SimSun', name='Song', leading=20, fontSize=12))  #自己增加新注册的字体
            styles['Normal'].fontName = 'ChineseFont'
            styles['Normal'].fontSize = 9
            styles['Normal'].leading = 20
            # styles['Normal'].leftIndent = 20  #设置左缩进为20

            styles['Title'].fontName = 'ChineseFont_bold'
            styles['Title'].fontSize = 12
            # styles['Title'].leftIndent = 10  #设置左缩进为20
            styles['Title'].alignment = 0
            styles.add(ParagraphStyle(parent=styles['Title'], name='ticker', leading=20,
                                      fontSize=10))  # 自己增加新注册的字体 ,leftIndent = 20
            styles.add(ParagraphStyle(parent=styles['Normal'], name='srcSummaryText', leading=15,
                                      fontSize=8))  # 自己增加新注册的字体 ,leftIndent = 20
            styles.add(
                ParagraphStyle(parent=styles['Normal'], fontName='ChineseFont_bold', name='normal_color', leading=15,
                               fontSize=8))  # 自己增加新注册的字体 ,leftIndent = 20
            styles.add(ParagraphStyle(parent=styles['Title'], name='srcSummaryTextTag', leading=10, fontSize=10,
                                      alignment=0))  # 自己增加新注册的字体 ,leftIndent = 20
            # styles.add(ParagraphStyle(fontName='ChineseFont_bold', name='ticker', leading=20, fontSize=10,leftIndent = 20 ))  #自己增加新注册的字体
            # 定义报告内容列表
            report_content = []
            # 循环生成每个报告的内容
            for data in json_data:
                # print('data:',data)
                if testSummary not in data:
                    continue
                # 總結
                fSummary = data[testSummary]
                if len(str(fSummary)) == 0:
                    continue
                tickers = ""
                if "windCode" in data:
                    tickers = data["windCode"]
                if "tickers" in data:
                    tickers = data["tickers"]
                if isinstance(tickers, list):
                    tickers = [x for x in tickers if x is not None]
                    tickers = "，".join(tickers)
                # if "summary" not in data:
                #     continue
                # 添加报告标题
                # title = "<u><b>{}</b></u>".format(data['title'])
                vTitle = data['title']
                if "title_zh" in data:
                    vTitle = data['title_zh']
                if int(data['importance_score']) !=-10 :
                    title = '<font color="#FF4961">{}</font> <b>{}</b>'.format(round(data['importance_score'],4),vTitle)
                else:
                    title = "<b>{}</b>".format(vTitle)
                report_content.append(Paragraph(title, styles["Title"]))
                # 添加时间
                if "source" not in data:
                    vsource = "其他"
                else:
                    vsource = data['source']
                if "dataType" not in data:
                    vdataType = "其他"
                else:
                    vdataType = data['dataType']
                if "publishOn" in data:
                    publishOn = data['publishOn']
                else:
                    publishOn = data['insertTime']
                # attrMap=['backColor', 'backcolor', 'bgcolor', 'color', 'face', 'fg', 'fontName', 'fontSize', 'fontname', 'fontsize', 'name', 'size', 'style', 'textColor', 'textcolor']
                tag1 = "<i><font color='red'>{} {}</font>  {} ID:{} </i>".format(vsource, vdataType, publishOn,
                                                                                 data['id'])
                # tag2 = "<i>{} {}</i>".format(vsource,vdataType)
                # report_content.append(Paragraph(tag1, styles["Normal"]))
                report_content.append(Paragraph(tag1, styles["srcSummaryText"]))
                # if data['vdbmap'] in ['news']:
                if "source_url" in data:
                    time = "<u>{}</u>".format(data['source_url'], data['source_url'])
                    report_content.append(Paragraph(time, styles["Normal"]))
                if tickers != "":
                    vtickers = "<i> tickers:{}</i>".format(tickers)
                    report_content.append(Paragraph(vtickers, styles["ticker"]))

                fSummary = str(fSummary).replace("\n", '<br/>').strip()
                content = "<font face='ChineseFont' size='12'>{}</font>".format(fSummary)
                report_content.append(Paragraph(content, styles["Normal"]))
                # 原文总结
                fsourceFlag = False
                if "summaryText" in data:
                    summaryText = data["summaryText"]
                    if isinstance(summaryText, list):
                        vsummaryText = []
                        for i, vt in enumerate(summaryText):
                            vsummaryText.append("%s、%s<br/>" % (i + 1, vt))
                        summaryText = "".join(vsummaryText)
                    else:
                        summaryText = str(summaryText)
                    if summaryText != "":
                        # summaryText=re.sub(r'\n+', '<br/>', summaryText)#多个
                        summaryText = re.sub(r'\n{2}', r'\n', summaryText)  # 2个
                        summaryText = re.sub(r'\n{2}', '<br/>', summaryText)  # 2个
                        content = " {} ".format(summaryText)
                        # report_content.append(
                        #     HRFlowable(width="50%", thickness=1, lineCap='round', spaceAfter=12,hAlign="LEFT"))  # 添加水平线
                        # Source = "<b>{}</b>".format("Source")
                        # report_content.append(Paragraph(Source, styles["srcSummaryTextTag"]))
                        # report_content.append(Paragraph(content, styles["srcSummaryText"]))
                        fsourceFlag = True
                # 原文总结
                if not fsourceFlag:
                    if "content" in data:
                        contentText = data["content"]
                        if isinstance(contentText, list):
                            vcontentText = []
                            for i, vt in enumerate(contentText):
                                vcontentText.append("%s、%s<br/>" % (i + 1, vt))
                            contentText = "".join(vcontentText)
                        else:
                            contentText = str(contentText)
                        contentText = re.sub(r'\n+', '<br/>', contentText)
                        content = " {} ".format(contentText)
                        # report_content.append(
                        #     HRFlowable(width="50%", thickness=1, lineCap='round', spaceAfter=12,hAlign="LEFT"))  # 添加水平线
                        # Source = "<b>{}</b>".format("Source")
                        # report_content.append(Paragraph(Source, styles["srcSummaryTextTag"]))
                        # report_content.append(Paragraph(content, styles["srcSummaryText"]))
                # 添加分隔线
                report_content.append(Paragraph("<br/>", styles["Normal"]))
                # 添加水平线
                # report_content.append(Spacer(1, 10))  # 添加间距
                report_content.append(HRFlowable(width="100%", thickness=1, lineCap='round', spaceAfter=12))  # 添加水平线
                # 我们使用HRFlowable类来添加水平线。HRFlowable的参数包括width（线条的宽度），thickness（线条的粗细），lineCap（线条的末端样式），spaceAfter（线条后面的间距）等
                report_content.append(Spacer(1, 10))  # 添加间距
            if len(report_content) > 0:
                # 将报告内容添加到PDF文档
                doc.build(report_content)
                print("pdf build", pdffilename)
                # pdffilename
                # wx_send.send_upload_file(pdffilename)
                print("wxUserIds:", wxUserIds)
                if len(wxUserIds) == 0:
                    sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file')
                    if not sendFlag:
                        sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file')
                else:
                    tousers = "|".join(wxUserIds)

                    sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file', touser=tousers)
                    if not sendFlag:
                        sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file', touser=tousers)
                rest.pdfname = pdfname
                rest.pdffilename = pdffilename
        except:
            Logger.errLineNo(msg="createSummary error")
            rest.errorData(errorCode="")
            print(traceback.format_exc())
        return rest
    def getStarmineFactors(self,windCodes=[]):
        #获取starmine指标数据
        rst=ResultData()
        rst.starminedt={}
        try:
            #从源指标缓存中获取
            V_starmineDF = cache_db.getCacheTableNewData(schemaTableName="NEWDATA.A_REUTERS_VALUATION")
            S_starmineDF = cache_db.getCacheTableNewData(schemaTableName="NEWDATA.A_REUTERS_STARMINECORE")
            #REV FY1% NI FY1%
            #RevenueMeanFY2RevenueMeanFY11、NetProfitMeanFY2NetProfitMeanFY11
            G_starmineDF = cache_db.getCacheTableNewData(schemaTableName="NEWDATA.A_REUTERS_GROWTH")
            #PricePctChg4W PricePctChgYTD
            #%4W %YTD
            T_starmineDF = cache_db.getCacheTableNewData(schemaTableName="NEWDATA.A_REUTERS_TRADING")
            V_starmineDt = V_starmineDF.set_index('WINDCODE').to_dict(orient='index')#高效率
            S_starmineDt = S_starmineDF.set_index('WINDCODE').to_dict(orient='index')
            G_starmineDt = G_starmineDF.set_index('WINDCODE').to_dict(orient='index')
            T_starmineDt = T_starmineDF.set_index('WINDCODE').to_dict(orient='index')
            #效率太慢
            # for idx,sd in V_starmineDF.iterrows():
            #     V_starmineDt[sd.WINDCODE]=dict(sd)
            # for idx,sd in S_starmineDF.iterrows():
            #     S_starmineDt[sd.WINDCODE]=dict(sd)
            # for idx,sd in G_starmineDF.iterrows():
            #     G_starmineDt[sd.WINDCODE]=dict(sd)
            # for idx,sd in T_starmineDF.iterrows():
            #     T_starmineDt[sd.WINDCODE]=dict(sd)
            starminedt={}
            for wc in windCodes:
                smdt = {'Mkt':"N/A",'PE NTM':"N/A",
                        'REVFY1':"N/A",'NIFY1':"N/A",
                        '4W':"N/A",'YTD':"N/A",
                        'AR':"N/A",'EQ':"N/A",'GR':"N/A",'VA':"N/A",'MO':"N/A",}
                fwc=wc
                if wc in ['GOOG.O']:
                    fwc="GOOGL.O"
                if fwc in V_starmineDt:
                    tsmdt = V_starmineDt[fwc]
                    smdt['Mkt']=tool_utils.uintFormat(tsmdt['MARKETCAPDS'],area='en')

                    forwardPE=tsmdt['FWDPENTM']
                    if not pd.isnull(forwardPE):
                        smdt['PE NTM']=round(float(forwardPE),2)

                if fwc in G_starmineDt:
                    tsmdt = G_starmineDt[fwc]
                    REVFY1 = tsmdt['REVENUEMEANFY2REVENUEMEANFY11']
                    if not pd.isnull(REVFY1):
                        smdt['REVFY1'] = round(float(REVFY1)*100, 2)
                    NIFY1 = tsmdt['NETPROFITMEANFY2NETPROFITMEANFY11']
                    if not pd.isnull(NIFY1):
                        smdt['NIFY1'] = round(float(NIFY1)*100, 2)

                if fwc in T_starmineDt:
                    tsmdt = T_starmineDt[fwc]
                    PricePctChg4W = tsmdt['PRICEPCTCHG4W']
                    if not pd.isnull(PricePctChg4W):
                        smdt['4W'] = round(float(PricePctChg4W), 2)
                    PricePctChgYTD = tsmdt['PRICEPCTCHGYTD']
                    if not pd.isnull(PricePctChgYTD):
                        smdt['YTD'] = round(float(PricePctChgYTD), 2)
                if fwc in S_starmineDt:
                    tsmdt = S_starmineDt[fwc]
                    AR = tsmdt['ARMINTRACOUNTRYSCORE']
                    if not pd.isnull(AR):
                        smdt['AR']=AR
                    EQ = tsmdt['EQCTRYRANK']
                    if not pd.isnull(EQ):
                        smdt['EQ']=EQ
                    VA = tsmdt['IVPRICETOINTRINSICVALUECOUNTRYRANK']
                    if not pd.isnull(VA):
                        smdt['VA']=VA
                    MO = tsmdt['PRICEMOCOUNTRYRANK']
                    if not pd.isnull(MO):
                        smdt['MO'] = MO
                starminedt[wc]=smdt
            rst.starminedt=starminedt
            return rst
        except:
            Logger.errLineNo(msg="remote mongodb get starmine error")
            rst.errorData(errorMsg="抱歉，获取指标数据异常，请稍后重试")
        return rst


    def getSummaryAI(self,titleTranslation,quarter='2023Q2'):
        summaryAI_dt={}
        with dbmongo.Mongo("AI") as md:
            mydb = md.db
            AISet = mydb["stocks"]
            AIdata = AISet.find({"quarter":quarter}, {"_id": 0,"summary_en":0,"summary":0,"summarys":0,"audioText":0,"summaryText":0,})
            for s in AIdata:
                if 'summary' in s:  # 摘要去空格
                    s['summary'] = str(s['summary']).strip()
                if titleTranslation in ['zh']:
                    if 'title_zh' in s:  # 用中文标题替换英文标题
                        s['title'] = s['title_zh']
                if titleTranslation in ['en']:
                    if 'summary_en' in s:  # 英文摘要
                        s_en = s['summary_en']
                        s_en = str(s_en).strip()
                        if s_en != "":
                            s['summary'] = s_en
                s['symbols_score']=[{"symbol": "NA", "importance_score": -100}]
                s['summaryText'] = ""
                s['content'] = ""
                s['cardContainerText'] = ""
                # if testMode =="1":
                s['summary'] = ""
                s['summary_en'] = ""
                s['summarys'] = ""
                # s['tickers']=""
                # print(" s['tickers']:",type( s['tickers']),len( s['tickers']))
                if "local_path" in s:  # 研报附件
                    s['local_path'] = s['local_path'].replace(r"C:\virtualD\work\project\download",
                                                              "http://8.129.218.237:8011/pdfs")
                s['secondType'] = "1"
                summaryAI_dt[s["windCode"]] =s

        return summaryAI_dt

    def getSummaryNewsAI(self,titleTranslation):
        summaryAI_dt={}
        with dbmongo.Mongo("AI") as md:
            mydb = md.db
            AISet = mydb["news"]
            AIdata = AISet.find({},{"_id": 0,"summary_en":0,"summary":0,"summarys":0,"summaryText":0,"audioText":0,})
            for s in AIdata:
                if 'summary' in s:  # 摘要去空格
                    s['summary'] = str(s['summary']).strip()
                if titleTranslation in ['zh']:
                    if 'title_zh' in s:  # 用中文标题替换英文标题
                        s['title'] = s['title_zh']
                if titleTranslation in ['en']:
                    if 'summary_en' in s:  # 英文摘要
                        s_en = s['summary_en']
                        s_en = str(s_en).strip()
                        if s_en != "":
                            s['summary'] = s_en
                s['symbols_score']=[{"symbol": "NA", "importance_score": -100}]
                s['summaryText'] = ""
                s['content'] = ""
                s['cardContainerText'] = ""
                # if testMode =="1":
                s['summary'] = ""
                s['summary_en'] = ""
                s['summarys'] = ""
                # s['tickers']=""
                # print(" s['tickers']:",type( s['tickers']),len( s['tickers']))
                if "local_path" in s:  # 研报附件
                    s['local_path'] = s['local_path'].replace(r"C:\virtualD\work\project\download",
                                                              "http://8.129.218.237:8011/pdfs")
                s['secondType'] = "1"
                summaryAI_dt[s["windCode"]] =s

        return summaryAI_dt

    def createSummary_v1(self, beginTime, endTime, isUpdateTime="0", upBeginTime=None, upEndTime=None, windCodes=[],
                         allCols=False, latest=True,performanceFlag="0"):
        """
        生成摘要数据
        :param beginTime:
        :param endTime:
        :param dataBases[{"dataBase":'',"dbName":''}]:
        :return:
        """
        rest = ResultData()
        rest.vsummaryDatas = []
        try:
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            infomationDBs = dbRst.information_datas
            ndate = datetime.datetime.now().strftime("%Y-%m-%d")
            # 上个批次发送编号
            existsSendIds = []
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                sendUuidSet = mydb["sendUuids"]
                sendUuidDatas = sendUuidSet.find({"sendDate": {"$lte": ndate}}).sort([("sendDate", -1)]).limit(1)
                sendUuidDatas = [x for x in sendUuidDatas]
                if len(sendUuidDatas) > 0:
                    existsSendIds = sendUuidDatas[0]['uuids']
            Logger.info("ndate:%s existsSendIds:%s" % (ndate, existsSendIds))
            pdfCtx = tools_utils.sendFileDir()
            pdffile = os.path.join(pdfCtx, 'pdf')
            if not os.path.exists(pdffile):
                os.makedirs(pdffile)
            summaryDatas = []
            # testSummary="summaryText"
            # testTime="insertTime"
            testSummary = "summary"
            testTime = "publishOn"
            # 新闻
            # 研报
            # 专家访谈
            v_news = []
            v_researchs = []
            v_zjfts = []
            for dbs in infomationDBs:
                datatype = dbs['datatype']
                if datatype in ['news']:
                    v_news.append(dbs)
                elif datatype in ['research']:
                    v_researchs.append(dbs)
                    pass
                elif datatype in ['专家访谈']:
                    v_zjfts.append(dbs)
                    pass
            # news
            # nt = datetime.datetime.now()
            # new_begintime = (nt + datetime.timedelta(days=-2)).strftime("%Y-%m-%d %H:%M:%S")
            # # research
            # rs_begintime = (nt + datetime.timedelta(days=-7)).strftime("%Y-%m-%d %H:%M:%S")
            # endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            bt1=datetime.datetime.now()
            for dbs in infomationDBs:
                dataBase = dbs['website']
                dbName = dbs['dbset']
                # if dbName in ['jefferies']:
                #     continue
                # sdbmap="%s@%s"%(dataBase,dbName)
                # print("sdbmap:",sdbmap)
                # vdbmap=""
                # if sdbmap in dbmap:
                #     vdbmap=dbmap[sdbmap]
                with dbmongo.Mongo(dataBase) as md:
                    mydb = md.db
                    contentsSet = mydb[dbName]
                    if dataBase in ['news']:
                        querys = {testTime: {'$gte': beginTime, '$lte': endTime}}
                    else:
                        querys = {testTime: {'$gte': beginTime, '$lte': endTime}}

                    if isUpdateTime == "1":
                        querys2 = {"updateTime": {'$gte': upBeginTime, '$lte': upEndTime}}
                        querys.update(querys2)
                    else:
                        querys["updateTime"] = {"$exists": True}
                    if len(windCodes)>0:
                        querys['windCode'] ={"$in":windCodes}
                    # if str(sumaryFlag) == "1":
                    #     querys["updateTime"] = {"$exists": True}
                    # else:
                    querys["summary"] = {"$exists": True}
                    # querys = {'id':{"$in":existsSendIds}}
                    rtCols = {"_id": 0}
                    if not allCols:
                        rtCols = {"_id": 0, "id": 1, "title": 1, testSummary: 1, "updateTime": 1, testTime: 1,
                                  "publishOn": 1, "windCode": 1, "tickers": 1,
                                  "source_url": 1, "summaryText": 1, "source": 1, 'dataType': 1}
                        rtCols = {"_id": 0}
                    # print("querys:",querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([("windCode", 1)])
                    for s in contentsSets:
                        # 已经推送过的
                        # if s['id'] in existsSendIds:
                        #     continue
                        if 'importance_score' not in s:  # 重要性
                            s['importance_score'] = -10
                        else:
                            s['importance_score']=round(s['importance_score'], 4)

                        #存在多个取最大的
                        if "symbols_score" in s:
                            ss  = s['symbols_score']
                            s['importance_score'] = max([x['importance_score'] for x in ss])

                        if 'title_zh' in s:  # 用中文标题替换英文标题
                            s['title'] = s['title_zh']
                        if "local_path" in s:#研报附件
                            s['local_path'] = s['local_path'].replace(r"C:\virtualD\work\project\download","http://8.129.218.237:8011/pdfs")
                        summaryDatas.append(s)
            bt2 = datetime.datetime.now()
            print(bt1,bt2)
            srts = self.getStockPool()
            poolWindCodes = srts.allCodes
            poolWindCodes = list(set(poolWindCodes))

            #需要跟踪的标普500
            if  performanceFlag=="1":
                #业绩会
                poolWindCodes=self.getPressReleases(preDays=-1,nextDays=0)
            else:
                monitorCodes=self.getPressReleases()
                poolWindCodes.extend(monitorCodes)

            if len(windCodes)>0:
                poolWindCodes=list(set(windCodes))

            # 从JSON数据获取报告列表
            json_data = summaryDatas
            dataDF = pd.DataFrame(json_data)
            if dataDF.empty:
                rest.vsummaryDatas = []
                return rest
            # print("dataDF:",dataDF)
            ordCodes = []
            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                stocklistset = mydb["stocklist"]
                stocks = stocklistset.find({}, {"_id": 0, 'windCode': 1, 'ord': 1})
                ordCodes = [x for x in stocks if x['windCode'] in poolWindCodes]
            # order 排序
            # print("ordCodes:",ordCodes)
            # ordCodes =sorted(ordCodes,key=lambda x:x['ord'])
            ordCodes = sorted(ordCodes, key=lambda x: x['ord'])

            # 新闻
            vsummaryDatas = []
            for x in ordCodes:
                if performanceFlag == "1":
                    # 业绩报告
                    yjbk_data = dataDF[(dataDF['dataType'].isin(['transcripts', 'Press Releases'])) & (
                                dataDF['windCode'] == x['windCode'])]
                    if not yjbk_data.empty:
                        yjbk_data = yjbk_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    yjbk_data = tools_utils.dfToListInnerDict(yjbk_data)
                    news_data = []
                    research_data = []
                    zjft_data = []
                else:
                    yjbk_data = dataDF[(dataDF['dataType'].isin(['transcripts', 'Press Releases'])) & (
                                dataDF['windCode'] == x['windCode'])]
                    if not yjbk_data.empty:
                        yjbk_data = yjbk_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    yjbk_data = tools_utils.dfToListInnerDict(yjbk_data)

                    news_data = dataDF[(dataDF['dataType'].isin(['news']) ) & (dataDF['windCode'] == x['windCode'])]
                    if not news_data.empty:
                        news_data = news_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    research_data = dataDF[(dataDF['dataType'] == 'research') & (dataDF['windCode'] == x['windCode'])]
                    if not research_data.empty:
                        research_data = research_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    zjft_data = dataDF[(dataDF['dataType'] == '专家访谈') & (dataDF['windCode'] == x['windCode'])]
                    if not zjft_data.empty:
                        zjft_data = zjft_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    news_data = tools_utils.dfToListInnerDict(news_data)
                    research_data = tools_utils.dfToListInnerDict(research_data)
                    zjft_data = tools_utils.dfToListInnerDict(zjft_data)
                if len(news_data) == 0 and len(research_data) == 0 and len(zjft_data) == 0 and len(yjbk_data) == 0:
                    continue
                vsummaryDatas.append(
                    {"windCode": x["windCode"].split(".")[0], "newsData": news_data,"yjbkData": yjbk_data, "researchData": research_data,
                     "zjftData": zjft_data, })
            # rest.vsummaryDatas=vsummaryDatas
            pdffilename = createPdfWithToc(vsummaryDatas)
            if pdffilename is None:
                return
            wxUserIds = []
            suuids = []  # 推送的报告编号
            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                summarySourceSet = mydb["summarySource"]
                sendTest = summarySourceSet.find_one({"id": 'isTest'})
                sendTestFlag = False
                if sendTest is not None:
                    sendTestValue = sendTest['value']
                    if str(sendTestValue) == "1":
                        sendTestFlag = True
                sendModes = summarySourceSet.find_one({"id": 'sendMode'})
                if sendModes is not None:
                    wxUserIds = sendModes['wxUserId']
                    testUserId = sendModes['testUserId']
                if sendTestFlag:
                    wxUserIds = testUserId

            Logger.info("wxUserIds:%s" % wxUserIds)
            # print("performanceFlag:",performanceFlag,wxUserIds)
            if performanceFlag=="0":
                if len(wxUserIds) == 0:
                    sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file')
                    if not sendFlag:
                        sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file')
                else:
                    tousers = "|".join(wxUserIds)
                    # print("tousers:",tousers)
                    # print("pdffilename:",pdffilename)
                    sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file', touser=tousers)
                    # print("sendFlag:",sendFlag)
                    if not sendFlag:
                        sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file', touser=tousers)
            #send email
            # 发送邮件
            filename=os.path.basename(pdffilename)
            import login.sendemail as send_email
            attachmentlist = [{'filename': filename, 'filefullpath': pdffilename}]
            # send_email.sendemailAndAttachments(['yuanzj@pinnacle-cap.cn'], filename, filename, attachmentlist, "robby")
            if socket.gethostname() == 'iZ2vcc0k0a629n6e2al2udZ':
                if performanceFlag == "1":
                    send_email.sendemailAndAttachments(['yuanzj@pinnacle-cap.cn','396404823@qq.com'], filename, filename, attachmentlist, "robby")
                pass
            rest.pdfname = os.path.basename(pdffilename)
            rest.pdffilename = pdffilename
            if len(windCodes) == 0:
                with dbmongo.Mongo("common") as md:
                    mydb = md.db
                    sendUuidSet = mydb["sendUuids"]
                    exUidSet = sendUuidSet.find_one({"sendDate": ndate})
                    if exUidSet is not None:
                        sendUuidSet.update_one({"sendDate": ndate},
                                               {"$set": {"uuids": suuids, "fileName": rest.pdfname, "filePath": pdffilename,
                                                         "updateTime": datetime.datetime.now()}})
                    else:
                        sendUuidSet.insert_one({"sendDate": ndate, "uuids": suuids,
                                                "fileName": rest.pdfname, "filePath": pdffilename,
                                                "updateTime": datetime.datetime.now()})
            return rest
        except:
            Logger.errLineNo(msg="createSummary_v1 error")
            rest.errorData(errorMsg="摘要报告生成失败，请联系系统管理员处理！")
            print(traceback.format_exc())
        return rest


    def createSummary_v2(self, beginTime, endTime, isUpdateTime="0", upBeginTime=None, upEndTime=None, windCodes=[],
                         allCols=False, latest=True,performanceFlag="0"):
        """
        生成摘要数据
        :param beginTime:
        :param endTime:
        :param dataBases[{"dataBase":'',"dbName":''}]:
        :return:
        """
        rest = ResultData()
        rest.vsummaryDatas = []
        try:
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            infomationDBs = dbRst.information_datas
            ndate = datetime.datetime.now().strftime("%Y-%m-%d")
            # 上个批次发送编号
            existsSendIds = []
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                sendUuidSet = mydb["sendUuids"]
                sendUuidDatas = sendUuidSet.find({"sendDate": {"$lte": ndate}}).sort([("sendDate", -1)]).limit(1)
                sendUuidDatas = [x for x in sendUuidDatas]
                if len(sendUuidDatas) > 0:
                    existsSendIds = sendUuidDatas[0]['uuids']
            Logger.info("ndate:%s existsSendIds:%s" % (ndate, existsSendIds))
            pdfCtx = tools_utils.sendFileDir()
            pdffile = os.path.join(pdfCtx, 'pdf')
            if not os.path.exists(pdffile):
                os.makedirs(pdffile)
            summaryDatas = []
            # testSummary="summaryText"
            # testTime="insertTime"
            testSummary = "summary"
            testTime = "publishOn"
            # 新闻
            # 研报
            # 专家访谈
            v_news = []
            v_researchs = []
            v_zjfts = []
            for dbs in infomationDBs:
                datatype = dbs['datatype']
                if datatype in ['news']:
                    v_news.append(dbs)
                elif datatype in ['research']:
                    v_researchs.append(dbs)
                    pass
                elif datatype in ['专家访谈']:
                    v_zjfts.append(dbs)
                    pass
            # news
            # nt = datetime.datetime.now()
            # new_begintime = (nt + datetime.timedelta(days=-2)).strftime("%Y-%m-%d %H:%M:%S")
            # # research
            # rs_begintime = (nt + datetime.timedelta(days=-7)).strftime("%Y-%m-%d %H:%M:%S")
            # endTime = nt.strftime("%Y-%m-%d %H:%M:%S")
            bt1=datetime.datetime.now()
            for dbs in infomationDBs:
                dataBase = dbs['website']
                dbName = dbs['dbset']
                # if dbName in ['jefferies']:
                #     continue
                # sdbmap="%s@%s"%(dataBase,dbName)
                # print("sdbmap:",sdbmap)
                # vdbmap=""
                # if sdbmap in dbmap:
                #     vdbmap=dbmap[sdbmap]
                with dbmongo.Mongo(dataBase) as md:
                    mydb = md.db
                    contentsSet = mydb[dbName]
                    if dataBase in ['news']:
                        querys = {testTime: {'$gte': beginTime, '$lte': endTime}}
                    else:
                        querys = {testTime: {'$gte': beginTime, '$lte': endTime}}

                    if isUpdateTime == "1":
                        querys2 = {"updateTime": {'$gte': upBeginTime, '$lte': upEndTime}}
                        querys.update(querys2)
                    else:
                        querys["updateTime"] = {"$exists": True}
                    if len(windCodes)>0:
                        querys['windCode'] = {"$in":windCodes}
                    # if str(sumaryFlag) == "1":
                    #     querys["updateTime"] = {"$exists": True}
                    # else:
                    querys["summary"] = {"$exists": True}
                    # querys = {'id':{"$in":existsSendIds}}
                    rtCols = {"_id": 0}
                    if not allCols:
                        rtCols = {"_id": 0, "id": 1, "title": 1, testSummary: 1, "updateTime": 1, testTime: 1,
                                  "publishOn": 1, "windCode": 1, "tickers": 1,
                                  "source_url": 1, "summaryText": 1, "source": 1, 'dataType': 1}
                        rtCols = {"_id": 0}
                    # print("querys:",dbName,querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([("windCode", 1)])
                    for s in contentsSets:
                        # 已经推送过的
                        # if s['id'] in existsSendIds:
                        #     continue
                        if 'importance_score' not in s:  # 重要性
                            s['importance_score'] = -10
                        else:
                            s['importance_score'] =round(s['importance_score'] ,4)

                        #存在多个取最大的
                        if "symbols_score" in s:
                            ss  = s['symbols_score']
                            s['importance_score'] = max([x['importance_score'] for x in ss])

                        if 'title_zh' in s:  # 用中文标题替换英文标题
                            s['title'] = s['title_zh']
                        if "local_path" in s:#研报附件
                            s['local_path'] = s['local_path'].replace(r"C:\virtualD\work\project\download","http://8.129.218.237:8011/pdfs")
                        summaryDatas.append(s)
            bt2 = datetime.datetime.now()
            print(bt1,bt2,len(summaryDatas))
            srts = self.getStockPool()
            poolWindCodes = srts.allCodes
            poolWindCodes = list(set(poolWindCodes))

            #需要跟踪的标普500
            if  performanceFlag=="1":
                #业绩会
                poolWindCodes=self.getPressReleases(preDays=-1,nextDays=0)
            else:
                monitorCodes=self.getPressReleases()
                poolWindCodes.extend(monitorCodes)

            if len(windCodes)>0:
                poolWindCodes=list(set(windCodes))

            poolWindCodes=list(set(poolWindCodes))
            # 从JSON数据获取报告列表
            json_data = summaryDatas
            dataDF = pd.DataFrame(json_data)
            if dataDF.empty:
                rest.vsummaryDatas = []
                return rest
            # print("dataDF:",dataDF)
            ordCodes = []
            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                stocklistset = mydb["stocklist"]
                stocks = stocklistset.find({}, {"_id": 0, 'windCode': 1, 'ord': 1})
                ordCodes = [x for x in stocks if x['windCode'] in poolWindCodes]
            # order 排序
            # print("ordCodes:",ordCodes)
            # ordCodes =sorted(ordCodes,key=lambda x:x['ord'])
            ordCodes = sorted(ordCodes, key=lambda x: x['ord'])

            # 新闻
            vsummaryDatas = []
            for x in ordCodes:
                if performanceFlag == "1":
                    # 业绩报告
                    yjbk_data = dataDF[(dataDF['dataType'].isin(['transcripts', 'Press Releases'])) & (
                                dataDF['windCode'] == x['windCode'])]
                    if not yjbk_data.empty:
                        yjbk_data = yjbk_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    yjbk_data = tools_utils.dfToListInnerDict(yjbk_data)
                    news_data = []
                    research_data = []
                    zjft_data = []
                else:
                    yjbk_data = dataDF[(dataDF['dataType'].isin(['transcripts', 'Press Releases'])) & (
                                dataDF['windCode'] == x['windCode'])]
                    if not yjbk_data.empty:
                        yjbk_data = yjbk_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    yjbk_data = tools_utils.dfToListInnerDict(yjbk_data)

                    news_data = dataDF[(dataDF['dataType'].isin(['news']) ) & (dataDF['windCode'] == x['windCode'])]
                    if not news_data.empty:
                        news_data = news_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    research_data = dataDF[(dataDF['dataType'] == 'research') & (dataDF['windCode'] == x['windCode'])]
                    if not research_data.empty:
                        research_data = research_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    zjft_data = dataDF[(dataDF['dataType'] == '专家访谈') & (dataDF['windCode'] == x['windCode'])]
                    if not zjft_data.empty:
                        zjft_data = zjft_data.sort_values(["importance_score",'publishOn'], ascending=False)
                    news_data = tools_utils.dfToListInnerDict(news_data)
                    research_data = tools_utils.dfToListInnerDict(research_data)
                    zjft_data = tools_utils.dfToListInnerDict(zjft_data)
                if len(news_data) == 0 and len(research_data) == 0 and len(zjft_data) == 0 and len(yjbk_data) == 0:
                    continue
                news_data=[]
                research_data=[]
                zjft_data=[]
                vsummaryDatas.append(
                    {"windCode": x["windCode"].split(".")[0], "newsData": news_data,"yjbkData": yjbk_data, "researchData": research_data,
                     "zjftData": zjft_data, })
            # rest.vsummaryDatas=vsummaryDatas
            pdffilename = createPdfWithToc(vsummaryDatas,prefix="业绩")
            if pdffilename is None:
                return
            wxUserIds = []
            suuids = []  # 推送的报告编号
            with dbmongo.Mongo("StockPool") as md:
                mydb = md.db
                summarySourceSet = mydb["summarySource"]
                sendTest = summarySourceSet.find_one({"id": 'isTest'})
                sendTestFlag = False
                if sendTest is not None:
                    sendTestValue = sendTest['value']
                    if str(sendTestValue) == "1":
                        sendTestFlag = True
                sendModes = summarySourceSet.find_one({"id": 'sendMode'})
                if sendModes is not None:
                    wxUserIds = sendModes['wxUserId']
                    testUserId = sendModes['testUserId']
                if sendTestFlag:
                    wxUserIds = testUserId

            Logger.info("wxUserIds:%s" % wxUserIds)
            if len(wxUserIds) == 0:
                sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file')
                if not sendFlag:
                    sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file')
            else:
                tousers = "|".join(wxUserIds)
                sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file', touser=tousers)
                if not sendFlag:
                    sendFlag = wx_send.send_upload_file(pdffilename, wxId='inari', type='file', touser=tousers)
            #send email
            # 发送邮件
            filename=os.path.basename(pdffilename)
            import login.sendemail as send_email
            attachmentlist = [{'filename': filename, 'filefullpath': pdffilename}]
            if socket.gethostname() == 'iZ2vcc0k0a629n6e2al2udZ':
                # if performanceFlag == "1":
                #     send_email.sendemailAndAttachments(['yuanzj@pinnacle-cap.cn','396404823@qq.com'], filename, filename, attachmentlist, "robby")
                pass
            # print("windCodes:",windCodes)
            if len(windCodes)>0:
                wname = ",".join(windCodes[0:5])
                filename="(%s)摘要报告_%s"%(wname,datetime.datetime.now().strftime("%Y-%m-%d"))
                print("filename:",filename)
                #'yuanzj@pinnacle-cap.cn',
                send_email.sendemailAndAttachments(['396404823@qq.com'], filename,
                                                   filename, attachmentlist, "robby")
            else:
                if socket.gethostname() == 'iZ2vcc0k0a629n6e2al2udZ':
                    send_email.sendemailAndAttachments(['yuanzj@pinnacle-cap.cn','396404823@qq.com'], filename, filename, attachmentlist,
                                                   "robby")
                pass
            rest.pdfname = os.path.basename(pdffilename)
            rest.pdffilename = pdffilename
            if len(windCodes) == 0:
                with dbmongo.Mongo("common") as md:
                    mydb = md.db
                    sendUuidSet = mydb["sendUuids"]
                    exUidSet = sendUuidSet.find_one({"sendDate": ndate})
                    if exUidSet is not None:
                        sendUuidSet.update_one({"sendDate": ndate},
                                               {"$set": {"uuids": suuids, "fileName": rest.pdfname, "filePath": pdffilename,
                                                         "updateTime": datetime.datetime.now()}})
                    else:
                        sendUuidSet.insert_one({"sendDate": ndate, "uuids": suuids,
                                                "fileName": rest.pdfname, "filePath": pdffilename,
                                                "updateTime": datetime.datetime.now()})
            return rest
        except:
            Logger.errLineNo(msg="createSummary_v2 error")
            rest.errorData(errorMsg="摘要报告生成失败，请联系系统管理员处理！")
            print(traceback.format_exc())
        return rest




    def callFill(self,  windCodes=[],  sumaryFlag="0", allCols=False,
                      rtcolumns={},status=-1):
        """
        获取新闻资讯编号
        :param beginDate:
        :param endDate:
        :param windCode:
        :param userPools:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        try:
            _lock.acquire()

            nt = datetime.datetime.now()
            beginTime = nt + datetime.timedelta(days=-40)

            beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
            endTime = nt.strftime("%Y-%m-%d %H:%M:%S")

            authorDT=self.getAuthorIds()
            rstStocks = self.getStockPool(userIds=[])
            stockpool = rstStocks.allCodes
            #日本股票
            # stockpool=[x for x in stockpool if str(x).endswith('.T')]
            # print("stockpool:",stockpool)
            nstockpool = []
            rtnids = []
            for code in stockpool:
                nstockpool.append(code)
            for s,ak in authorDT.items():
                nstockpool.extend(ak['authorIds'])
            nstockpool.append("Audio")

            if len(windCodes) > 0:
                nstockpool = windCodes  # querys["windCode"] = {"$in": windCodes}
            else:
                # querys["windCode"] = {"$in": nstockpool}
                pass
            querys = {"publishOn": {'$gte': beginTime, '$lte': endTime},"summaryText":{"$ne":""}}
            if sumaryFlag == "1":
                querys["updateTime"] = {"$exists": False}
            rtCols = {"_id": 0, 'windCode': 1}
            if not allCols:
                rtCols = {"_id": 0, "id": 1, 'windCode': 1,'tickers': 1, 'source': 1, 'dataType': 1, 'insertTime': 1}
            if len(rtcolumns) > 0:
                rtCols = rtcolumns
                rtCols.update({'_id': 0})

            # 数据源
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                return dbRst
            infomationDBs = dbRst.information_datas
            for idb in infomationDBs:
                database = idb['website']
                dbset = idb['dbset']
                print("dbset:",dbset)
                # if not dbset in ['yahoo_contetns_jp']:
                if not dbset in ['aletheia']:
                    continue
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    # 研报发布时间
                    print("getInfomationIds querys:%s"%querys)
                    contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])  # 0不显示，1：显示； 查询数据
                    nids=[]
                    for ec in contentsSets:
                        if 'windCode'not in ec:
                            continue
                        if ec['windCode'] in nstockpool:
                            nids.append(ec)#
                    # nids = [x['id'] for x in contentsSets if x['windCode'] in nstockpool]
                    #将id进行分类
                    rtnids.extend(nids)
            print("nids:",len(nids),nids)
            dbError={}
            dbDoing={}
            dbPull={}
            dbNone= {}
            dbEnd= {}
            existsId=[]
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                summarySet = mydb["summaryIds"]
                sdata = summarySet.find({},{'_id':0})#{'status':{"$in":[0,2,9]}}#0:未处理，2：pull,3:处理中 ；9：异常 1：完成
                for sd in sdata:
                    sstatus=sd['status']
                    sid=sd['id']
                    if sstatus ==0:
                        dbNone[sid]=sd
                        existsId.append(sid)
                    elif sstatus ==1:
                        dbEnd[sid]=sd
                        existsId.append(sid)
                    elif sstatus ==2:
                        dbDoing[sid]=sd
                        existsId.append(sid)
                    elif sstatus ==3:
                        dbPull[sid]=sd
                        existsId.append(sid)
                    elif sstatus ==9:
                        dbError[sid]=sd
                        existsId.append(sid)
                    else:
                        continue

            addRows=[]
            levels={"Press Releases":{"level":1,"gptv":['gpt3.5']},
                    "transcripts": {"level": 2, "gptv": ['gpt4']},
                    "research":{"level":3,"gptv":['gpt4']},
                    "Audio":{"level":0,"gptv":['gpt4']},
                    "news":{"level":4,"gptv":['gpt4']},}
            for nd in rtnids:
                if nd["id"] in existsId:
                    continue
                vdataType = nd['dataType']
                if vdataType in levels:
                   vlevels = levels[vdataType]
                else:
                    return
                nt=datetime.datetime.now()
                insertTime=nd['insertTime']
                insertTime=datetime.datetime.strptime(insertTime,"%Y-%m-%d %H:%M:%S")
                nrow={"id": nd["id"],"createTime": insertTime,"pullTime": nt,"pushTime":nt,"status": 0,"dataType":vdataType,"level": vlevels['level'],"gptv":  vlevels['gptv'],}
                print("nrow:",nrow)
                addRows.append(nrow)
            #dbDoing 触发时间检查，如果pullTime 超过10分钟，重置
            nt = datetime.datetime.now()
            #是否执行中的gpt4 一次只能有一个在运行
            with dbmongo.Mongo("common") as md:
                mydb = md.db
                summarySet = mydb["summaryIds"]
                #新的列表
                for d in addRows:
                    fone =summarySet.find_one({'id':d["id"]})
                    if fone is None:
                        print("add")
                        summarySet.insert_one(d)
                    else:
                        print("update")
                        # summarySet.update_one({'id':d['id']},{"$set":{'status':0}})
                # if len(addRows)>0:
                #     summarySet.insert_many(addRows)
            rest.data = rtnids
            rest.count = len(rtnids)
        except:
            Logger.errLineNo(msg="callFill error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        finally:
            try:
                _lock.release()
            except:pass
        return rest


def createPdfWithToc(vsummaryDatas,prefix=""):
    pdfCtx = tools_utils.sendFileDir()
    pdffile = os.path.join(pdfCtx, 'pdf')
    if not os.path.exists(pdffile):
        os.makedirs(pdffile)

    pdfname = "%s_%s.pdf" % (prefix+"摘要报告", datetime.datetime.now().strftime("%Y%m%d_%H%M"))
    pdffilename = os.path.join(pdffile, pdfname)

    from reportlab.lib.pagesizes import letter
    # from reportlab.platypus import SimpleDocTemplate, Paragraph
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable

    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import fonts
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.pdfgen import canvas
    # 设置字体文件路径
    if tools_utils.isAlyPlatform():
        font_path = r"/opt/service/dsidmfactors/login/methods/portfolio/factsheet/fonts/msyhLight.ttf"
        font_bold = r"/opt/service/dsidmfactors/login/methods/portfolio/factsheet/fonts/微软雅黑Bold.ttf"
    else:
        font_path = r"C:\work\project\web\main\login\methods\portfolio\factsheet\fonts\msyhLight.ttf"
        font_bold = r"C:\work\project\web\main\login\methods\portfolio\factsheet\fonts\微软雅黑Bold.ttf"
    # 注册中文字体
    pdfmetrics.registerFont(TTFont("ChineseFont", font_path))
    pdfmetrics.registerFont(TTFont("ChineseFont_bold", font_bold))
    doc = SimpleDocTemplate(pdffilename, pagesize=letter)
    # 定义样式
    styles = getSampleStyleSheet()
    # styles.add(fontName="ChineseFont", fontSize=12)
    # styles.add(ParagraphStyle(fontName='SimSun', name='Song', leading=20, fontSize=12))  #自己增加新注册的字体
    styles['Normal'].fontName = 'ChineseFont'
    styles['Normal'].fontSize = 9
    styles['Normal'].leading = 20
    # styles['Normal'].leftIndent = 20  #设置左缩进为20
    styles['Title'].fontName = 'ChineseFont_bold'
    styles['Title'].fontSize = 12
    # styles['Title'].leftIndent = 10  #设置左缩进为20
    styles['Title'].alignment = 0
    styles.add(
        ParagraphStyle(parent=styles['Title'], name='ticker', leading=20, fontSize=10))  # 自己增加新注册的字体 ,leftIndent = 20
    styles.add(ParagraphStyle(parent=styles['Normal'], name='srcSummaryText', leading=15,
                              fontSize=8))  # 自己增加新注册的字体 ,leftIndent = 20
    styles.add(ParagraphStyle(parent=styles['Normal'], fontName='ChineseFont_bold', name='normal_color', leading=15,
                              fontSize=8))  # 自己增加新注册的字体 ,leftIndent = 20
    styles.add(ParagraphStyle(parent=styles['Title'], name='srcSummaryTextTag', leading=10, fontSize=10,
                              alignment=0))  # 自己增加新注册的字体 ,leftIndent = 20
    # styles.add(ParagraphStyle(fontName='ChineseFont_bold', name='ticker', leading=20, fontSize=10,leftIndent = 20 ))  #自己增加新注册的字体
    # 生成目录

    # 创建一个空的文档对象
    # 创建样式
    heading_style = styles["Title"]
    # 创建目录
    contents = []
    # 添加目录项
    # contents.append(Paragraph("<u>TMT(科技、媒体、互联网）</u>", heading_style))
    contents.append(Spacer(1, 3))

    # 生成章节标题和链接

    def pageStockList(stockList, pageSize=300):
        allSize = len(stockList)
        if allSize % pageSize > 0:
            page = allSize // pageSize + 1
        else:
            page = allSize // pageSize

        pageSList = []
        for page in range(page):
            pageSList.append(stockList[page * pageSize:pageSize * (page + 1)])
        # print(pageSList)
        return pageSList

    pagedatas = pageStockList(vsummaryDatas, 10)
    for vsdatas in pagedatas:
        chapter_title_links = []
        chapter_style = ParagraphStyle("ChapterHeading", parent=heading_style, spaceAfter=4)
        chapter_style.textColor = "blue"
        for chapter in vsdatas:
            windCode = chapter['windCode']
            chapter_title = windCode
            chapter_link = str(windCode).split(".")[0] + "link"
            # 创建章节标题的样式
            # 创建章节标题和链接的对象
            chapter_title_link = f'<font color="blue"><a href="#{chapter_link}">{chapter_title}</a></font>'
            chapter_title_links.append(chapter_title_link)

        chapter_title_paragraph = Paragraph("\t\t".join(chapter_title_links), chapter_style)
        # 添加目录项
        contents.append(chapter_title_paragraph)
        contents.append(Spacer(1, 6))

    suuids = []
    testSummary = "summary"
    testTime = "publishOn"

    # 分类别， 新闻、研报、专家访谈
    report_content = []
    # 循环生成每个报告的内容
    contents.append(Paragraph("<br/><br/>", styles["Normal"]))
    for vsdata in vsummaryDatas:
        windCode = vsdata['windCode']
        chapter_link = str(windCode).split(".")[0] + "link"
        # news
        sdata = [{'name': "业绩报告", "dataName": "yjbkData", "color": "#28D094"},
                 {'name': "新闻", "dataName": "newsData", "color": "#FF9149"},
                 {'name': "研究报告", "dataName": "researchData", "color": "#FF4961"},
                 {'name': "专家访谈", "dataName": "zjftData", "color": "#716ACA"}, ]
        for vtag in sdata:
            tagColor = vtag["color"]
            tagName = vtag["name"]
            newsData = vsdata[vtag["dataName"]]
            # 添加章节内容
            if len(newsData) > 0:
                report_content.append(
                    Paragraph(f'<a name="{chapter_link}"></a><b><font color="blue">%s</font></b>' % windCode,
                              heading_style))
                report_content.append(Paragraph('<font color="%s">%s</font>' % (tagColor, tagName), styles["Title"]))
            for data in newsData:
                if testSummary not in data:
                    continue
                # 總結
                fSummary = data[testSummary]
                fSummary_en=""
                if "summary_en" in data:
                    fSummary_en = data["summary_en"]
                    if isinstance(fSummary_en, float) and math.isnan(fSummary_en):
                        fSummary_en=""
                if len(str(fSummary)) == 0:
                    continue
                tickers = ""
                if "windCode" in data:
                    tickers = data["windCode"]
                if "tickers" in data:
                    tickers = data["tickers"]
                if isinstance(tickers, list):
                    tickers = [x for x in tickers if x is not None]
                    tickers = "，".join(tickers)
                # if "summary" not in data:
                #     continue
                # 添加报告标题
                # title = "<u><b>{}</b></u>".format(data['title'])
                vTitle = data['title']
                if "title_zh" in data:
                    tTitle = data['title_zh']
                    if tTitle is not None and not pd.isnull(tTitle):
                        vTitle = tTitle
                if int(data['importance_score']) !=-10 :
                    title = '<font color="#FF4961">{}</font> <b>{}</b>'.format(data['importance_score'],vTitle)
                else:
                    title = '<b>{}</b>'.format(vTitle)
                # print("title:",title,data['importance_score'])
                report_content.append(Paragraph(title, styles["Title"]))
                # 添加时间
                if "source" not in data:
                    vsource = "其他"
                else:
                    vsource = data['source']
                if "dataType" not in data:
                    vdataType = "其他"
                else:
                    vdataType = data['dataType']
                if "publishOn" in data:
                    publishOn = data['publishOn']
                else:
                    publishOn = data['insertTime']
                # attrMap=['backColor', 'backcolor', 'bgcolor', 'color', 'face', 'fg', 'fontName', 'fontSize', 'fontname', 'fontsize', 'name', 'size', 'style', 'textColor', 'textcolor']
                # tag1 = "<i><font color='red'>{} {}</font>  {} ID:{} </i>".format(vsource, vdataType, publishOn, data['id'])
                tag1 = "<i>{} {} ID:{} </i>".format(vsource, publishOn, data['id'])
                # tag2 = "<i>{} {}</i>".format(vsource,vdataType)
                # report_content.append(Paragraph(tag1, styles["Normal"]))
                report_content.append(Paragraph(tag1, styles["srcSummaryText"]))
                # if data['vdbmap'] in ['news']:
                if vdataType in ['news']:
                    if "source_url" in data:
                        time = "<u>{}</u>".format(data['source_url'], data['source_url'])
                        report_content.append(Paragraph(time, styles["Normal"]))
                else:
                    #报告 \ email
                    if "local_path" in data:
                        vlocal_path = "<u>{}</u>".format(data['local_path'], data['local_path'])
                        report_content.append(Paragraph(vlocal_path, styles["Normal"]))

                # if tickers != "":
                #     vtickers = "<i> tickers:{}</i>".format(tickers)
                #     report_content.append(Paragraph(vtickers, styles["ticker"]))
                fSummary = str(fSummary).replace("\n", '<br/>').strip()
                # print("summaryText:", data['id'], summaryText)
                content = "<font face='ChineseFont' size='12'>{}</font>".format(fSummary)
                report_content.append(Paragraph(content, styles["Normal"]))


                # 业绩报告 两套
                if "summarys" in data:
                    _summarys = data['summarys']
                    if isinstance(_summarys, list):
                        if len(_summarys) >= 2:
                            for s in range(1, len(_summarys)):
                                _fsummarys = _summarys[s]
                                _fsummarys = str(_fsummarys).replace("\n", '<br/>').strip()
                                title_1 = '<font color="#FF4961">{}</font>'.format("第%s版摘要"% (s+1))
                                # 添加分隔线
                                report_content.append(Paragraph("<br/>", styles["Normal"]))
                                report_content.append(HRFlowable(width="100%", thickness=1, lineCap='round', spaceAfter=12))  # 添加水平线
                                _fsummarys = "<font face='ChineseFont' size='12'>{}</font>".format(_fsummarys)
                                report_content.append(Paragraph(title_1, styles["srcSummaryTextTag"]))
                                report_content.append(Paragraph(_fsummarys, styles["srcSummaryText"]))

                if len(fSummary_en)>0:
                    fSummary_en = str(fSummary_en).replace("\n", '<br/>').strip()
                    # print("summaryText:", data['id'], summaryText)
                    summary_en = "<font face='ChineseFont' size='12'>{}</font>".format(fSummary_en)
                    report_content.append(
                        HRFlowable(width="50%", thickness=1, lineCap='round', spaceAfter=12,hAlign="LEFT"))  # 添加水平线
                    if vdataType in ['Press Releases']:
                        title_en = "<b>{}</b>".format("第2版摘要")
                    else:
                        title_en = "<b>{}</b>".format("英文摘要")
                    report_content.append(Paragraph(title_en, styles["srcSummaryTextTag"]))
                    report_content.append(Paragraph(summary_en, styles["srcSummaryText"]))

                # 原文总结
                fsourceFlag = False
                if "summaryText" in data:
                    summaryText = data["summaryText"]
                    if isinstance(summaryText, list):
                        vsummaryText = []
                        for i, vt in enumerate(summaryText):
                            vsummaryText.append("%s、%s<br/>" % (i + 1, vt))
                        summaryText = "".join(vsummaryText)
                    else:
                        summaryText = str(summaryText)
                    if summaryText != "":
                        # summaryText=re.sub(r'\n+', '<br/>', summaryText)#多个
                        summaryText = re.sub(r'\n{2}', r'\n', summaryText)  # 2个
                        summaryText = re.sub(r'\n{2}', '<br/>', summaryText)  # 2个
                        content = " {} ".format(summaryText)
                        # report_content.append(
                        #     HRFlowable(width="50%", thickness=1, lineCap='round', spaceAfter=12,hAlign="LEFT"))  # 添加水平线
                        # Source = "<b>{}</b>".format("Source")
                        # report_content.append(Paragraph(Source, styles["srcSummaryTextTag"]))
                        # report_content.append(Paragraph(content, styles["srcSummaryText"]))
                        fsourceFlag = True
                # 原文总结
                if not fsourceFlag:
                    if "content" in data:
                        contentText = data["content"]
                        if isinstance(contentText, list):
                            vcontentText = []
                            for i, vt in enumerate(contentText):
                                vcontentText.append("%s、%s<br/>" % (i + 1, vt))
                            contentText = "".join(vcontentText)
                        else:
                            contentText = str(contentText)
                        contentText = re.sub(r'\n+', '<br/>', contentText)
                        content = " {} ".format(contentText)
                        # report_content.append(
                        #     HRFlowable(width="50%", thickness=1, lineCap='round', spaceAfter=12,hAlign="LEFT"))  # 添加水平线
                        # Source = "<b>{}</b>".format("Source")
                        # report_content.append(Paragraph(Source, styles["srcSummaryTextTag"]))
                        # report_content.append(Paragraph(content, styles["srcSummaryText"]))

                # 添加分隔线
                report_content.append(Paragraph("<br/>", styles["Normal"]))
                # 添加水平线
                # report_content.append(Spacer(1, 10))  # 添加间距
                report_content.append(HRFlowable(width="100%", thickness=1, lineCap='round', spaceAfter=12))  # 添加水平线
                # 我们使用HRFlowable类来添加水平线。HRFlowable的参数包括width（线条的宽度），thickness（线条的粗细），lineCap（线条的末端样式），spaceAfter（线条后面的间距）等
                report_content.append(Spacer(1, 10))  # 添加间距
                suuids.append(data['id'])
        # 修改推送时间
    if len(report_content) > 0:
        # 将报告内容添加到PDF文档
        doc.build(contents + report_content)
        print("pdf build", pdffilename)
        return pdffilename
    return None


if __name__ == '__main__':
    newMd = informationMode()

    # newMd.getCallData("AAPL")
    # s = newMd.getInfomationDataBase(summary="1")
    # s = newMd.getStockPool()
    # print(s.toDict())
    # s = newMd.getPressReleases(preDays=-3,nextDays=14)
    # print(s)
    # rst = newMd.getStockPool(trackFlag="1")
    # print(rst.toDict())
    # rs = newMd.getNewsIds_v1("2023-08-17 00:00:00", "2023-08-24 00:00:00",windCodes=['AMD.O'],  userIdsPool=[], sumaryFlag="0",allCols=False,rtcolumns={})

    # nt = datetime.datetime.now()

    # rst = newMd.getAudioSummaryView('2023-02-21 00:23:32', "2099-07-24 12:59:22",
    #                                     queryId="conferenceCall",companyIds=['pinnacle'])
    # print("rst:",rst.toDict())
    # print("end")
    # time.sleep(1000)
    # time.sleep(1000)
    # time.sleep(1000)

    # nt2 = datetime.datetime.now()
    # print("nt:",nt)
    # print("nt2:",nt2)

    nt = datetime.datetime.now() + datetime.timedelta(days=1)
    beginTime = nt + datetime.timedelta(days=-10)
    beginTime = beginTime.strftime("%Y-%m-%d %H:%M:%S")
    endTime = nt.strftime("%Y-%m-%d %H:%M:%S")

    rs = newMd.getNewsIds_v3(beginTime, endTime, userIdsPool=[], sumaryFlag="1",allCols=False,rtcolumns={})
    print(rs.toDict())
    print("end")
    time.sleep(1000)
    time.sleep(1000)
    time.sleep(1000)

    # resp = requests.post("http://192.168.2.6:8008/mobileApp/api/informationApi/",
    #                      data={'doMethod':"getNewsIds_v3",
    #                            'gptid':1, 'sumaryFlag':"1",
    #                            }
    # #                     data={'doMethod':"summaryFinishSend",
    # #                            # 'windCodes':"SPLK.O",
    # #                            'audioId':"Audio_4048a02a6cf24565df637ab4e1f27b44",
    # #                            }
    #                      )
    # print("xxxresp:",resp.json())
    # #
    # print("end")
    # time.sleep(1000)
    # time.sleep(1000)
    # time.sleep(1000)
    # upBeginTime = '2023-01-22 21:30:03'
    # upEndTime = "2099-07-24 12:59:22"
    # rs=newMd.getAudioIds(beginTime, endTime, companyIds=[], tickers=[], userIdsPool=[], sumaryFlag="1", allCols=False,
    #                   rtcolumns={})
    # rs=newMd.fillAudioText("Audio_5b318c9efe14d2f592013496fa6d8282", "Audio_5b318c9efe14d2f592013496fa6d8282")
    # rs=newMd.getSummaryViewByTag(vtags=["news"], isUpdateTime="1", upBeginTime=upBeginTime, upEndTime=upEndTime, tickers=[],
    #                    allCols=False,queryId="self_22810",titleTranslation="zh",newsDays=1,testMode="0",userId="1751",
    #                         companyId=None,searchRole=None)
    # print(rs.toDict())

    # st=newMd.getStarmineFactors(windCodes=["600519.SH",'AAPL.O'])
    rst = requests.post("http://192.168.2.6:8008/mobileApp/api/informationApi/",
    # # # # # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod':"fillSummary_v1", 'id':"Audio_4048a02a6cf24565df637ab4e1f27b44",
                         data={'doMethod':"updateSourceContent", 'id':"Audio_4048a02a6cf24565df637ab4e1f27b44",
                              'summary_zh':"abac",  'summary_en':"bca", 'title_zh':'bbbdd','title_en':'xxxxxx',
                               "importance_score": 1,
                               "transSummaryText": """苹果 销量 始，请稍后。
""",
                               "symbols_score": json.dumps([{"symbol": "msft", "importance_score": 12.5}, ],),
                               "reason": json.dumps([{"symbol": "msft", "importance_score": 12.5}, ],),
                               "relationCompanies":json.dumps({'company':[{'symbol':'MFST','name':'Microsoft'}],'industry':[{'name':'Internet'}]}),
                               "publicSentiment": "中性"
                               })
    #
    print("rst:",rst.json())
    # resp = requests.post("http://192.168.2.6:8008/mobileApp/api/informationApi/",
    # #1、获取需要翻译的音频 返回音频编号
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod':"getTranslateAudio","sumaryFlag":"1"
    #                            })
    # print("rst:",resp.json())
    # #2、通过音频编号获取 音频的文件路径
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod':"getNewsContents_v1","id":"Audio_a08186d74744f7ddf3502949e0e1ba8d"
    #                            })
    # rstjs = resp.json()
    # if rstjs['errorFlag']:
    #     datas=rstjs['data']
    #     for dt in datas:
    #         audio_path=dt['audio_path']#下载地址
    #         print("audio_path:",audio_path)
    #
    # #3、下载音频到本地
    #
    # #4、翻译音频
    #
    # #5、上传音频文本
    # audioText="翻译后的音频文本"
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod':"fillAudioText","id":"Audio_a08186d74744f7ddf3502949e0e1ba8d","audioText":audioText
    #                            })
    #
    print('end')
    time.sleep(10000)
    time.sleep(10000)
    time.sleep(10000)
    #
    # threads = []
    # for i in range(100):
    #     thread = threading.Thread(target=ts, args=(i,))
    #     threads.append(thread)
    #     thread.start()
    #
    # # 等待所有线程完成
    # for thread in threads:
    #     thread.join()
    #
    # id = '8-K_9229fd2b6d09bf23885584d9f76d1382ba'
    # resp=newMd.callBackNewStatus(id,9)
    # print(resp.toDict())
    #
    # import requests
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod':"getNewsIds_v1", 'beginTime':"2023-01-01 08:00:00",
    #                            'endTime':"2023-12-28 08:34:14","sumaryFlag":'1', "symbol":["ENPH.O"]})
    # print(resp.json())
    # #
    #
    # #['tb_176202', 'tb_176230', 'tb_176223', 'tb_176231']
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod':"getNewsContents_v1",'id':"sAts_4623310"})
    #
    # print(resp.json())

    # print('callfill')


    # with dbmongo.Mongo("common") as md:
    #     mydb = md.db
    #     summarySet = mydb["summaryIds"]
    #     ss=summarySet.find({})
    #     rs= summarySet.delete_many({"dataType":"research","createTime":{"$gt":datetime.datetime.strptime("2023-09-18 12:00:00","%Y-%m-%d %H:%M:%S")}})
    #     # rs= summarySet.delete_many({"dataType":"transcripts","createTime":{"$gt":datetime.datetime.strptime("2023-07-09 21:00:30","%Y-%m-%d %H:%M:%S")}})
    #     print(rs.deleted_count)
    #
    #
    # s=newMd.getStockPlateList2()
    # print(s)
    # # newMd.callFill(sumaryFlag="0")
    # print('end')
    #
    # time.sleep(10000)
    # time.sleep(10000)
    # time.sleep(10000)
    # rst = newMd.createSummary_v1('2023-02-21 00:23:32',"2023-07-24 12:59:22",isUpdateTime="1",
    #                              upBeginTime='2023-01-22 21:30:03',upEndTime="2023-07-24 12:59:22",
    #                              # performanceFlag="1"
    #                              )
    # rst = newMd.getMonitorIds(windCodes=['AMD.O'], userIdsPool=[], sumaryFlag="1", allCols=False,
    #                   rtcolumns={})

    # #
    # rst = requests.post("http://localhost:8000/mobileApp/api/informationApi/",
    # # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod':"fillSummary_v1", 'id':"sA_3978866",
    #                           'summary_zh':"a",  'summary_en':"b", 'title_zh':'aaaaa',
    #                            "importance_score": 1,
    #                            "symbols_score": json.dumps([{"symbol": "msft", "importance_score": 11.5}, ],),
    #                            "publicSentiment": "中性"
    #                            })
    #
    # print("rst:",rst.json())

    # fdis=["sAts_4621979","sAts_4621858","sAts_4621962","sAts_4621957","sAts_4621827","sAts_4621984","sAts_4622009","sAts_4621959","sAts_4621971","sAts_4622006","sAts_4622184","sAts_4622170","sAts_4622185","sAts_4622196","sAts_4622181","sAts_4622167","sAts_4622202","sAts_4622192","sAts_4622216","sAts_4622171","sAts_4622218","sAts_4622223","sAts_4622224","sAts_4622239","sAts_4622254","sAts_4622248","sAts_4622265","sAts_4622236","sAts_4622272","sAts_4622318","sAts_4622259","sAts_4622246","sAts_4622274","sAts_4622293","sAts_4622326","sAts_4622350","sAts_4622364","sAts_4622319","sAts_4622371","sAts_4622395","sAts_4622410","sAts_4622411","sAts_4622407","sAts_4622445","sAts_4622448","sAts_4622458","sAts_4622451","sAts_4622453","sAts_4622481","sAts_4622484","sAts_4622488","sAts_4622503","sAts_4622513","sAts_4622523","sAts_4622525","sAts_4622545","sAts_4622665","sAts_4622688","sAts_4622692","sAts_4622709","sAts_4622693","sAts_4622715","sAts_4622710","sAts_4622738","sAts_4622725","sAts_4622743","sAts_4622717","sAts_4622731","sAts_4622716","sAts_4622748","sAts_4622744","sAts_4622788","sAts_4622808","sAts_4622736","sAts_4622827","sAts_4622728","sAts_4622760","sAts_4622834","sAts_4622811","sAts_4622759","sAts_4622770","sAts_4622799","sAts_4622812","sAts_4622864","sAts_4622776","sAts_4622879","sAts_4622839","sAts_4622820","sAts_4622919","sAts_4622878","sAts_4622899","sAts_4622861","sAts_4622881","sAts_4622926","sAts_4622849","sAts_4622904","sAts_4622898","sAts_4622964","sAts_4622947","sAts_4623001","sAts_4623025","sAts_4623042","sAts_4623016","sAts_4623022","sAts_4623041","sAts_4623023","sAts_4623053","sAts_4623033","sAts_4623036","sAts_4623058","sAts_4623070","sAts_4623045","sAts_4623005","sAts_4623015","sAts_4623021","sAts_4623035","sAts_4623066","sAts_4623067","sAts_4623111","sAts_4623129","sAts_4623084","sAts_4623113","sAts_4623280","sAts_4623306","sAts_4623334","sAts_4623342","sAts_4623375","sAts_4623326","sAts_4623354","sAts_4623332","sAts_4623358","sAts_4623361","sAts_4623329","sAts_4623364","sAts_4623390","sAts_4623382","sAts_4623384","sAts_4623335","sAts_4623318","sAts_4623403","sAts_4623295","sAts_4623395","sAts_4623412","sAts_4623405","sAts_4623394","sAts_4623408","sAts_4623420","sAts_4623437","sAts_4623419","sAts_4623481","sAts_4623429","sAts_4623430","sAts_4623471","sAts_4623461","sAts_4623436","sAts_4623530","sAts_4623438","sAts_4623462","sAts_4623508","sAts_4623460","sAts_4623557","sAts_4623444","sAts_4623499","sAts_4623482","sAts_4623503","sAts_4623540","sAts_4623606","sAts_4623588","sAts_4623611","sAts_4623613","sAts_4623644","sAts_4623652","sAts_4623676","sAts_4623684","sAts_4623669","sAts_4623699","sAts_4623680","sAts_4623666","sAts_4623663","sAts_4623657","sAts_4623686","sAts_4623647","sAts_4623679","sAts_4623702","sAts_4623715","sAts_4623716","sAts_4623728","sAts_4623744","sAts_4623742","sAts_4623730","sAts_4623754","sAts_4623745","sAts_4623801","sAts_4624104","sAts_4624222","sAts_4624240","sAts_4624040","sAts_4624046","sAts_4624097","sAts_4624086","sAts_4624231","sAts_4624280","sAts_4624213","sAts_4624178","sAts_4624171","sAts_4624234","sAts_4624116","sAts_4624620","sAts_4624342","sAts_4624594","sAts_4624738","sAts_4624730","sAts_4624988","sAts_4624993","sAts_4625038","sAts_4625179","sAts_4625111","sAts_4625096","sAts_4624991","sAts_4624958","sAts_4625146","sAts_4625349","sAts_4625387","sAts_4625396","sAts_4625400","sAts_4625406","sAts_4625425","sAts_4625444","sAts_4625449","sAts_4625459","sAts_4625452","sAts_4625487","sAts_4625481","sAts_4625511","sAts_4625523","sAts_4625501","sAts_4625539","sAts_4625535","sAts_4625557","sAts_4625568","sAts_4625624","sAts_4625665","sAts_4625682","sAts_4625717","sAts_4625685","sAts_4625702","sAts_4625750","sAts_4625744","sAts_4625764","sAts_4625774","sAts_4625888","sAts_4625432","sAts_4625637","sAts_4623310","sAts_4622922"]
    #
    # with dbmongo.Mongo("common") as md:
    #     mydb = md.db
    #     summarySet = mydb["summaryIds"]
    #     ss=summarySet.update_many({"dataType":"transcripts","status":9} ,{"$set":{"status":0}})
    #     print(ss.modified_count)
        # 新的列表
        # for __id in fdis:
        #     fone = summarySet.find_one({'id': __id})
        #     ur=summarySet.update_one({'id':__id}, {'$set':{'status': 0,'createTime':datetime.datetime.now()}})
        #     print(__id,ur.modified_count)


    #
    # print('end')
    # time.sleep(1000)
    # time.sleep(1000)
    # time.sleep(1000)
    # time.sleep(1000)
    # time.sleep(1000000)



    # rst = newMd.createSummary_v2('2023-07-26 02:04:03', "2023-07-29 12:59:22", isUpdateTime="1",
    #                              upBeginTime='2023-07-26 02:04:03', upEndTime="2023-09-24 12:59:22")

    # rst = newMd.createSummary_v1('2023-01-26 00:00:00',"2023-12-24 12:59:22",isUpdateTime="1",upBeginTime='2023-07-01 00:10:00',upEndTime="2023-12-24 12:59:22")
    #beginTime:2023-02-21 00:23:32, endTime:2099-07-24 12:59:22, isUpdateTime:1, upBeginTime:2023-01-22 21:30:03, upEndTime:2099-07-24 12:59:22, windCode:NVDA.O,allCols:False, latest:True,queryId:0,titleTranslation:zh,newsDays:1,testMode:1
    #beginTime:2023-02-21 00:23:32, endTime:2099-07-24 12:59:22, isUpdateTime:1, upBeginTime:2023-01-22 21:30:03, upEndTime:2099-07-24 12:59:22, windCode:NVDA.O,allCols:False, latest:True,queryId:0,titleTranslation:en,newsDays:1,testMode:1

    time.sleep(1000)
    time.sleep(1000)
    time.sleep(1000)
    time.sleep(1000)
    time.sleep(1000000)


    rst = newMd.getSummaryView('2023-02-21 00:23:32', "2099-07-24 12:59:22", isUpdateTime="1",
                                        upBeginTime='2023-01-22 21:30:03', upEndTime="2099-07-24 12:59:22",
                                        queryId="conferenceCall",titleTranslation="en",newsDays=1,testMode="1")
    vsummaryDatas=rst.toDict()['vsummaryDatas']
    for v in vsummaryDatas:
        ns =v['newsData']
        for n in ns:
            print(n['title'],n['tickers'],n['publishOn'])
    # bt=datetime.datetime.now()
    # rst=newMd.getSycStockPool()
    # sendFlag = wx_send.send_upload_file("d:/temp/pdf/摘要报告_20230720_1631.pdf", wxId='inari', type='file', touser="robby.xia")
    # sendFlag = wx_send.send_wx_msg_operation("d:/temp/pdf\摘要报告_20230720_1631.pdf", wxId='inari',  touser="robby.xia")
    # print("sendFlag:",sendFlag)
    print("end")
    time.sleep(10000)
    time.sleep(10000)
    time.sleep(10000)
    time.sleep(10000)
    # bTime = '2023-01-01 01:00:00'  # "2023-06-19 21:00:00"
    # eTime = "2023-12-31 12:59:22"
    # upBeginTime = '2023-07-01 11:10:00'  # '2023-06-24 21:30:03'
    # upEndTime = "2023-07-01 21:59:22"
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod': "buildSummaryPdf", 'beginTime': bTime, "endTime": eTime, 'isUpdateTime': "1",
    #                            "upBeginTime": upBeginTime, "upEndTime": upEndTime, "symbol": "ENPH.O"})
    # print(resp.json())
    # time.sleep(10000)
    # time.sleep(10000)
    # bTime = '2023-07-17 00:00:00'  # "2023-06-19 21:00:00"
    # eTime = "2023-12-31 12:59:22"
    # upBeginTime = '2023-07-17 00:00:00'  # '2023-06-24 21:30:03'
    # upEndTime = "2023-09-04 21:59:22"
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod': "buildSummaryPdf_v1", 'beginTime': bTime, "endTime": eTime,
    #                            'isUpdateTime': "1",
    #                            "upBeginTime": upBeginTime, "upEndTime": upEndTime})#, "symbol": "ENPH.O"
    # print(resp.json())
    # print('end...')
    # print('end...')
    #
    # time.sleep(10000)
    # time.sleep(10000)
    # time.sleep(10000)
    # time.sleep(10000)

    # bTime = '2023-06-29 01:00:00'  # "2023-06-19 21:00:00"
    # eTime = "2023-12-31 12:59:22"
    # upBeginTime = '2023-07-01 05:10:00'  # '2023-06-24 21:30:03'
    # upEndTime = "2023-07-01 11:59:22"
    # resp = requests.post("https://www.daohequant.com/mobileApp/api/informationApi/",
    #                      data={'doMethod': "buildSummaryPdf_v1", 'beginTime': bTime, "endTime": eTime,
    #                            'isUpdateTime': "1", "upBeginTime": upBeginTime, "upEndTime": upEndTime})
    # print(resp.json())
    # print('end...')

    time.sleep(10000)
    time.sleep(10000)
    time.sleep(10000)
    time.sleep(10000)
    # 其他测试代码 在 newTest 里面
