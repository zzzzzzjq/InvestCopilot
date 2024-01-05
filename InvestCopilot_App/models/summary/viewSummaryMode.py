"""

"""

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

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.portfolio.portfolioMode import cportfolioMode
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
import InvestCopilot_App.models.toolsutils.dbutils as  dbutils

import logging
from InvestCopilot_App.models.toolsutils import dbmongo

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

class viewSummaryMode():
    def __init__(self):
        self.lock = threading.RLock()

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

    def getSummaryViewByTag(self,vtags=["news"],symbols=[],queryId=None,translation="zh",userId="",
                            companyIds=[],page=1,pageSize=20,gtScore=1, ltScore=10):
        #个股主页信息查询  按新闻、研报、电话会议、内部报告 分类获取前20条数据
        vTag_dt = {"news":{'name': "新闻", 'dataType': "news", "dataName": "newsData", "color": "#FF9149",
             "class": 'btn-outline-warning',"data":[]},
                   "research": {'name': "研究报告", 'dataType': "research", "dataName": "researchData", "color": "#FF4961","class":'btn-outline-danger',"data":[]},
                   "transcripts": {'name': "业绩公告", 'dataType': "transcript", "dataName": "yjbkData", "color": "#28D094","class":'btn-outline-success',"data":[]},
                   "Audio":{'name': "电话会议", 'dataType': "Audio", "dataName": "meetingData", "color": "#716ACA","class": 'btn-outline-success',"data":[]},
                   "innerResearch":{'name': "内部研究", 'dataType': "innerResearch", "dataName": "innerResearchData", "color": "#716ACA","class": 'btn-outline-success',"data":[]},
                   }
        """
        生成摘要数据  分页查询 按分类处理，研报、新闻、业绩报告
        :param queryId:自选股组合编号 或菜单编号  宏观策略、业绩报告
        :return:
        """
        rest = ResultData()
        rest.markIds=[]
        rest.data=[]
        rest.firstCodes=[]
        try:
            if "Audio" in vtags:
                return rest
            # 用户名与公司关系
            userCfg_dt = cache_db.getUserConfig_dt()
            #获取用户的公司编号
            user_companyId=""
            if userId in userCfg_dt:
                udt = userCfg_dt[userId]
                user_companyId = udt['COMPANYID']
            ybsummary_dt=cache_dict.getDictByKeyNo('2')#研报摘要权限查询 配置了这个类别中的公司才能查看这些研报摘要

            #搜索查询所有
            bt__1=datetime.datetime.now()
            if len(symbols)>0:
                    #自定义组合
                poolWindCodes=symbols
            elif str(queryId).startswith("self_"):
                    #自定义组合
                poolWindCodes=cportfolioMode().getPortfolioStocks(queryId,userId).windCodes
            elif str(queryId) in ["allHoldings"]:
                    #用户组合的所有股票
                poolWindCodes=cportfolioMode().getPortfolioStocks(None,userId).windCodes
            elif str(queryId).startswith("hgcl"):
                # 宏观策略
                authorDT = self.getAuthorIds()
                poolWindCodes = []
                for s, ak in authorDT.items():
                    poolWindCodes.extend(ak['authorIds'])
                hgrp = set()  # 宏观报告
                clrp = set()  # 策略报告
                for s, ak in authorDT.items():
                    authors = ak['authors']
                    for ca in authors:
                        if "宏观" in ca['showType'].split(","):
                            hgrp.add(ca['authorId'])
                        if "策略" in ca['showType'].split(","):
                            clrp.add(ca['authorId'])
            else:
                prts=cportfolioMode().getSummaryRelationStocks(queryId)
                if prts.errorFlag:
                    poolWindCodes=[]
                    for x in prts.portfolioStocksInfo:
                        poolWindCodes.append(x[0])
                else:
                    return rest
            #公司编号
            if len(companyIds)>0:
                poolWindCodes.extend(companyIds)
            tickers=poolWindCodes
            # bt__2 = datetime.datetime.now()
            # print("b1:",bt__2,bt__1,(bt__2-bt__1).total_seconds())
            #2、路由数据来源集合
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            infomationDBs = dbRst.information_datas
            bt=datetime.datetime.now()
            #3、查询解析数据
            rtdata_d=[]
            #按集合进行分类
            qsets={}
            for dbs in infomationDBs:#按数据库 对集合进行合并
                dataBase = dbs['website']
                dbName = dbs['dbset']
                datatype = dbs['datatype']
                if datatype not in vtags:
                    continue
                if datatype in ['research']:
                    if dbName in ybsummary_dt:
                        sccompanyids= str(ybsummary_dt[dbName]).split(",")
                        if  user_companyId not in sccompanyids:
                            continue
                if dataBase in qsets:
                    fqsets = qsets[dataBase]
                    dbsets = fqsets["dbsets"]

                    dbsets.append(dbName)
                else:
                    qsets[dataBase] = {"website":dataBase,"dbsets":[dbName]}
            # print("tickers:",tickers)
            # print("qsets:",qsets)
            querys = {}
            querys['tickers'] = {"$in": tickers}
            #querys['dataType'] = {"$in": vtags}
            querys["updateTime"] = {"$exists": True}
            querys["summary"] = {"$ne": ""}
            querys["data_display"] = {"$exists": False}
            if "news" in vtags:
                querys["total_score"] = {"$gte": gtScore,"$lte": ltScore}#原始值搜索

            if queryId in ['hgcl']:
                querys["dataType"] = "Economics & Strategy"  # 宏观策略
            projection = {"_id": 0, "id": 1,"title": 1, "title_en": 1,"title_zh": 1, "publishOn": 1,
                          "source": 1, "dataType": 1, "insertTime": 1,"updateTime": 1,  "cuserId": 1,
                          "outputType": 1, "source_url": 1 ,"symbols": 1,"pagecount":1,
                          "gpt_reportType": 1, "priceTarget": 1 ,"monthRating": 1,"total_score": 1,"news_class": 1,
                          "tickers": 1, "firstCode": 1,"secondType": 1,"text_emotion_score":1,"content_emotion_score":1,
                          }
            # 定义排序条件
            sort = {"publishOn": -1}  # 1表示升序，-1表示降序
            skip = (page - 1) * pageSize
            rvrecode={"benzinga":"bz","seekingAlpha":"sa"}
            for website,website_dt in qsets.items():
                dbsets = website_dt['dbsets']
                # print("dataBase:",dataBase)
                dblist=[{"$unionWith": dbs} for dbs in dbsets]
                dblist.append({"$match": querys})  # 过滤符合条件的文档
                dblist.append({"$sort": sort})  # 根据排序条件排序
                dblist.append({"$skip": skip})  # 跳过文档
                dblist.append({"$limit": pageSize})  # 限制结果数量
                dblist.append({"$project": projection})  # 筛选要显示的字段
                # print("dblist:",dblist)
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    contentsSets = list(mydb.collection_name.aggregate(dblist))
                markIds = [s["id"] for s in contentsSets]
                firstCodes=[]
                for s in contentsSets:
                    if translation in ['zh']:
                        if 'title_zh' in s:  # 中文标题
                            s['title'] = s.pop("title_zh")
                    elif translation in ['en']:
                        if 'title_en' in s:  # 英文标题
                            s['title'] = s.pop("title_en")
                    # if s['id'] not in  v_score_dt:
                    #     v_score_dt[s['id']]=[{'symbol':"N/A","windCode":"N/A","score":-10}]
                    # news_class 翻译
                    if s['source'] in rvrecode:
                        s['source'] = rvrecode[s['source']]
                    if "news_class" in s:
                        news_class = s["news_class"]
                        if news_class in mg_cfg.news_class_dt:
                            s["news_class"] =  mg_cfg.news_class_dt[news_class]
                    if s["dataType"] in ["research", 'Economics & Strategy']:
                        s["source_url"] = ""
                        s["html_url"] = ""
                        s["view_url"] = ""
                        s["local_html"] = ""

                    firstCode=""
                    if "firstCode" in s :
                        firstCode=s.pop('firstCode')
                    else:
                        tickers=s.pop('tickers')
                        if len(tickers)>0:
                            firstCode=tickers[0]
                    s['windCode']=firstCode
                    firstCodes.append(firstCode)
                    if "secondType" not in s:
                        s['secondType'] = "1"
                    #标记文档所属公司/员工
                    if "cuserId" in s:
                        s_userId = s.pop("cuserId")
                        if s_userId in userCfg_dt:
                            udt = userCfg_dt[s_userId]
                            usserName = udt['USERNICKNAME']
                            ucompanyName = udt['SHORTCOMPANYNAME']
                            companyId= udt['COMPANYID']
                            if  not companyId in companyIds:
                                continue
                            s['nusername'] = usserName
                            s['ncompanyname'] = ucompanyName
                    vdataType = s['dataType']
                    if vdataType in ["transcripts","Press Releases","EarningsCallPresentation"]:
                        if vdataType in ["Press Releases"]:
                            s['title']="(%s)  "%s['symbols'] +  s['title']
                            rtdata_d.append(s)
                        elif vdataType == "transcripts":
                            if "secondType" in s:
                                secondType = s['secondType']
                                if secondType =="NoTranscript":
                                    s['secondType']="fanyi"
                                    if translation in ['zh']:
                                        s['title']=s['title']+"  完整翻译版"
                                    else:
                                        s['title'] = s['title'] + "  Translation"
                                    rtdata_d.append(s)
                                else:
                                    rtdata_d.append(s)
                                    ns = s.copy()
                                    ns['secondType'] = "fanyi"
                                    if translation in ['zh']:
                                        ns['title'] = ns['title'] + "  完整翻译版"
                                    else:
                                        ns['title'] = ns['title'] + "  Translation"
                                    rtdata_d.append(ns)
                            else:
                                rtdata_d.append(s)
                                ns = s.copy()
                                ns['secondType']="fanyi"
                                if translation in ['zh']:
                                    ns['title']=ns['title']+"  完整翻译版"
                                else:
                                    ns['title'] = ns['title'] + "  Translation"
                                rtdata_d.append(ns)
                        else:
                            rtdata_d.append(s)
                    else:
                        rtdata_d.append(s)
            #排序
            # news_d=sorted(news_d,key=lambda x:x['publishOn'],reverse=True)
            # research_d=sorted(research_d,key=lambda x:x['publishOn'],reverse=True)
            # transcripts_d=sorted(transcripts_d,key=lambda x:x['publishOn'],reverse=True)
            # meeting_d=sorted(meeting_d,key=lambda x:x['publishOn'],reverse=True)
            # innerResearch_d=sorted(innerResearch_d,key=lambda x:x['publishOn'],reverse=True)
            # vTag_dt['news']['data']=news_d
            # vTag_dt['research']['data']=research_d
            # vTag_dt['transcripts']['data']=transcripts_d
            # vTag_dt['Audio']['data']=meeting_d
            # vTag_dt['innerResearch']['data']=innerResearch_d
            et = datetime.datetime.now()
            # Logger.info("part1 bt:%s,et:%s"%(bt,et))
            # print("part1 bt:%s,et:%s"%(bt,et),(et-bt).total_seconds())
            # for x in rtdata_d[0]:
            #     print("x:",x)
            # json_data = summaryDatas
            rest.data = rtdata_d
            rest.markIds =markIds
            rest.firstCodes =firstCodes
            return rest
        except:
            Logger.errLineNo(msg="getSummaryViewByTag error")
            rest.errorData(errorMsg="抱歉，数据加载失败，请稍后重试！")
        return rest

    def getStrategySumViewByTag(self,vtags=["research"],symbols=[],queryId=None,translation="zh",userId="",companyIds=[],page=1,pageSize=20):
        """
        宏观策略报告 ，需要按集合分页查询
        :return:
        """
        rest = ResultData()
        try:
            rtdata_dt={
                "hgrp":{"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[]},
                "clrp":{"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[]},
                       }
            hgrp = set()  # 宏观报告
            clrp = set()  # 策略报告
            if str(queryId).startswith("hgcl"):
                # 宏观策略
                authorDT = self.getAuthorIds()
                poolWindCodes = []
                for s, ak in authorDT.items():
                    poolWindCodes.extend(ak['authorIds'])
                for s, ak in authorDT.items():
                    authors = ak['authors']
                    for ca in authors:
                        if "宏观" in ca['showType'].split(","):
                            hgrp.add(ca['authorId'])
                        if "策略" in ca['showType'].split(","):
                            clrp.add(ca['authorId'])
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            infomationDBs = dbRst.information_datas
            bt=datetime.datetime.now()
            #3、查询解析数据
            #按集合进行分类
            qsets={}
            for dbs in infomationDBs:#按数据库 对集合进行合并
                dataBase = dbs['website']
                dbName = dbs['dbset']
                datatype = dbs['datatype']
                if datatype not in vtags:
                    continue
                if dataBase in qsets:
                    fqsets = qsets[dataBase]
                    dbsets = fqsets["dbsets"]
                    dbsets.append(dbName)
                else:
                    qsets[dataBase] = {"website":dataBase,"dbsets":[dbName]}
            # print("tickers:",tickers)
            # print("qsets:",qsets)
            querys = {}
            querys['tickers'] = {"$in": list(hgrp)}
            #querys['dataType'] = {"$in": vtags}
            querys["updateTime"] = {"$exists": True}
            querys["summary"] = {"$ne": ""}
            querys["data_display"] = {"$exists": False}
            if queryId in ['hgcl']:
                querys["dataType"] = "Economics & Strategy"  # 宏观策略
            projection = {"_id": 0, "id": 1,"title": 1, "title_en": 1,"title_zh": 1, "publishOn": 1,
                          "source": 1, "dataType": 1, "insertTime": 1,"updateTime": 1,  "cuserId": 1,
                          "outputType": 1, "source_url": 1 ,"symbols": 1,"pagecount":1,
                          "gpt_reportType": 1, "priceTarget": 1 ,"monthRating": 1,"total_score": 1,"news_class": 1,
                          "tickers": 1, "firstCode": 1,"secondType": 1,"text_emotion_score":1,"content_emotion_score":1,
                          }
            # 定义排序条件
            sort = {"publishOn": -1}  # 1表示升序，-1表示降序
            skip = (page - 1) * pageSize
            hg_data_d= {"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[]}
            for website,website_dt in qsets.items():
                dbsets = website_dt['dbsets']
                # print("dataBase:",dataBase)
                dblist=[{"$unionWith": dbs} for dbs in dbsets]
                dblist.append({"$match": querys})  # 过滤符合条件的文档
                dblist.append({"$sort": sort})  # 根据排序条件排序
                dblist.append({"$skip": skip})  # 跳过文档
                dblist.append({"$limit": pageSize})  # 限制结果数量
                dblist.append({"$project": projection})  # 筛选要显示的字段
                print("dblist:",dblist)

                # 分页总记录数
                qCountlist = [{"$unionWith": dbs} for dbs in dbsets]
                qCountlist.append({"$match": querys})  # 过滤符合条件的文档
                qCountlist.append({"$count": "total_orders"})  # # 第二个阶段：计算符合条件的文档总数
                # 总页数
                total_orders = 0
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    queryresult = list(mydb.collection_name.aggregate(qCountlist))
                    if queryresult:
                        total_orders = queryresult[0]["total_orders"]
                    hg_data_d['totalNum'] = total_orders
                    hg_data_d['pageTotal'] = math.ceil(total_orders / pageSize)

                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    contentsSets = list(mydb.collection_name.aggregate(dblist))
                hg_rows=[]
                for s in contentsSets:
                    if translation in ['zh']:
                        if 'title_zh' in s:  # 中文标题
                            s['title'] = s.pop("title_zh")
                    elif translation in ['en']:
                        if 'title_en' in s:  # 英文标题
                            s['title'] = s.pop("title_en")
                    if s["dataType"] in ["research", 'Economics & Strategy']:
                        s["source_url"] = ""
                        s["html_url"] = ""
                        s["view_url"] = ""
                        s["local_html"] = ""
                    if "secondType" not in s:
                        s['secondType'] = "1"
                    hg_rows.append(s)
                hg_data_d['data']=hg_rows

            cl_data_d= {"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[]}
            querys = {}
            querys['tickers'] = {"$in": list(clrp)}
            # querys['dataType'] = {"$in": vtags}
            querys["updateTime"] = {"$exists": True}
            querys["summary"] = {"$ne": ""}
            if queryId in ['hgcl']:
                querys["dataType"] = "Economics & Strategy"  # 宏观策略
            projection = {"_id": 0, "id": 1, "title": 1, "title_en": 1, "title_zh": 1, "publishOn": 1,
                          "source": 1, "dataType": 1, "insertTime": 1, "updateTime": 1, "cuserId": 1,
                          "outputType": 1, "source_url": 1, "symbols": 1, "pagecount": 1,
                          "gpt_reportType": 1, "priceTarget": 1, "monthRating": 1, "total_score": 1, "news_class": 1,
                          "tickers": 1, "firstCode": 1, "secondType": 1, "text_emotion_score": 1,
                          "content_emotion_score": 1,
                          }
            # 定义排序条件
            sort = {"publishOn": -1}  # 1表示升序，-1表示降序
            skip = (page - 1) * pageSize
            for website, website_dt in qsets.items():
                dbsets = website_dt['dbsets']
                # print("dataBase:",dataBase)
                dblist = [{"$unionWith": dbs} for dbs in dbsets]
                dblist.append({"$match": querys})  # 过滤符合条件的文档
                dblist.append({"$sort": sort})  # 根据排序条件排序
                dblist.append({"$skip": skip})  # 跳过文档
                dblist.append({"$limit": pageSize})  # 限制结果数量
                dblist.append({"$project": projection})  # 筛选要显示的字段
                # 分页总记录数
                qCountlist = [{"$unionWith": dbs} for dbs in dbsets]
                qCountlist.append({"$match": querys})  # 过滤符合条件的文档
                qCountlist.append({"$count": "total_orders"})  # # 第二个阶段：计算符合条件的文档总数
                # 总页数
                total_orders = 0
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    queryresult = list(mydb.collection_name.aggregate(qCountlist))
                    if queryresult:
                        total_orders = queryresult[0]["total_orders"]
                    cl_data_d['totalNum'] = total_orders
                    cl_data_d['pageTotal'] = math.ceil(total_orders / pageSize)
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    contentsSets = list(mydb.collection_name.aggregate(dblist))
                cl_rows=[]
                for s in contentsSets:
                    if translation in ['zh']:
                        if 'title_zh' in s:  # 中文标题
                            s['title'] = s.pop("title_zh")
                    elif translation in ['en']:
                        if 'title_en' in s:  # 英文标题
                            s['title'] = s.pop("title_en")
                    if s["dataType"] in ["research", 'Economics & Strategy']:
                        s["source_url"] = ""
                        s["html_url"] = ""
                        s["view_url"] = ""
                        s["local_html"] = ""
                    if "secondType" not in s:
                        s['secondType'] = "1"
                    cl_rows.append(s)
                cl_data_d['data']=cl_rows

            rtdata_dt={
                "hgrp":hg_data_d,
                "clrp":cl_data_d}
            rest.data = rtdata_dt
            return rest
        except:
            Logger.errLineNo(msg="getStrategySumViewByTag error")
            rest.errorData(errorMsg="抱歉，数据加载失败，请稍后重试！")
        return rest

    def getDocContent(self, ids=[],allCols=True, rtcolumns={}):
        """
        获取文件内容
        :param ids: 文档编号
        :param allCols: 控制返回的字段
        :param rtcolumns: 控制返回的字段列表
        :return:
        """
        rest = ResultData()
        rest.data = {"count":0,"data":[]}
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
                        querys = {"id": qid}
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
            rest.data ={"count":len(rtnids),"data":rtnids,}
        except:
            Logger.errLineNo(msg="getDocContent error")
            rest.errorData(errorMsg="抱歉，获取文档内容失败，请稍后重试")
        return rest

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

    def getEditSummarysStatusForUpdate(self, sid, userid):
        ###获取当前报告是否可编辑
        rest = ResultData()
        with self.lock:
            try:
                rtnids = viewSummaryMode().getSummaryNum(sids=[sid])
                snum = rtnids[0]["num"]
                if snum == 0:
                    rest.errorData(errorMsg="报告不存在，请重新选择！")
                    return rest
                cuserId = rtnids[0]['cuserId']
                # 检查数据是否存在
                u_user_dt = user_utils.getCacheUserInfo(userid)
                if u_user_dt is None:
                    rest.errorData(errorMsg="用户信息异常，请重新选择！")
                    return rest
                c_user_dt = user_utils.getCacheUserInfo(cuserId)
                if c_user_dt is None:
                    rest.errorData(errorMsg="用户信息异常，请重新选择！")
                    return rest
                u_companyId = u_user_dt['COMPANYID']
                c_companyId = c_user_dt['COMPANYID']
                if u_companyId != c_companyId:
                    rest.errorData(errorMsg="只允许修改同一公司下的报告！")
                    return rest
                sql_Query = "select status,cuserid from business.summarys_edit_status where sid=%s for update  "
                con, cur = dbutils.getConnect()
                cur.execute(sql_Query, [sid])
                rtds = cur.fetchall()
                fetchnum = cur.rowcount
                userName = "未知"
                if fetchnum > 0:  # 存在记录
                    # 检查状态
                    rtd = rtds[0]
                    nstatus = str(rtd[0])
                    nuserid = str(rtd[1])
                    if nuserid == str(userid) and nstatus in ["1"]:
                        sql_Query = "update business.summarys_edit_status set status='1',edittime=current_timestamp where sid=%s"
                        cur.execute(sql_Query, [sid])
                        con.commit()
                        rest.errorMsg = "状态可用[exists]"
                        return rest
                    if nstatus in ["1"]:
                        e_user_dt = user_utils.getCacheUserInfo(nuserid)
                        if e_user_dt is None:
                            rest.errorData(errorMsg="未查找到编辑用户信息，请重新选择！")
                            return rest
                        # u_companyId = e_user_dt['COMPANYID']
                        u_username = e_user_dt['USERREALNAME']
                        rest.errorData(errorCode="USE", errorMsg="[%s]用户正在编辑当前报告，请稍后再试。" % (u_username))
                        return rest
                    else:
                        # 可用
                        sql_Query = "update business.summarys_edit_status set status='1',userid=%s,edittime=current_timestamp  where sid=%s"
                        cur.execute(sql_Query, [userid, sid])
                        con.commit()
                        rest.errorMsg = "状态可用[his]"
                else:
                    # 可用，新增一条
                    sql_Query = "insert into business.summarys_edit_status (sid,cuserid,edittime,status) values(%s,%s,current_timestamp,%s)"
                    cur.execute(sql_Query, [sid, userid, "1"])
                    con.commit()
                    rest.errorMsg = "状态可用[new]"
            except:
                Logger.errLineNo(msg="getEditSummarysStatusForUpdate error")
                rest.errorData(errorMsg="抱歉，获取报告编辑状态异常，请稍后重试")
            finally:
                try:
                    cur.close()
                except:
                    pass
                try:
                    con.close()
                except:
                    pass
        return rest

    def releaseSummaryEditStatus(self, sid, userid):
        ###释放当前报告编辑状态
        rest = ResultData()
        try:
            con, cur = dbutils.getConnect()
            sql_Query = "delete from  business.summarys_edit_status where sid=%s and cuserid=%s"
            cur.execute(sql_Query, [sid, userid])
            con.commit()
        except:
            Logger.errLineNo(msg="releaseSummaryEditStatus error")
            rest.errorData(errorMsg="抱歉，重置报告编辑状态异常，请稍后重试")
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        return rest

    def editSummaryDataSave(self, sid, userid, editContent, languages):
        ###编辑后数据保存 需先调用getEditSummarysStatusForUpdate方法才可用
        rest = ResultData()
        with self.lock:
            try:
                rtnids = viewSummaryMode().getSummaryNum(sids=[sid])
                snum = rtnids[0]["num"]
                if snum == 0:
                    rest.errorData(errorCode="002", errorMsg="报告不存在，请重新选择！")
                    return rest
                dsource = rtnids[0]['source']
                dataType = rtnids[0]['dataType']
                nt = datetime.datetime.now()
                scontent = tools_utils.strEncode(editContent)  # 核心观点
                # id
                logid = tools_utils.md5(",".join([sid, str(userid), str(nt)]))
                sql_Query = "select status,cuserid from business.summarys_edit_status where sid=%s for update  "
                con, cur = dbutils.getConnect()
                cur.execute(sql_Query, [sid])
                rtds = cur.fetchall()
                fetchnum = cur.rowcount
                if fetchnum > 0:  # 存在记录
                    # 检查状态
                    rtd = rtds[0]
                    nstatus = str(rtd[0])
                    nuserid = str(rtd[1])
                    if nuserid == str(userid) and nstatus in ["1"]:
                        # sava data log
                        i_dta = "INSERT INTO business.summarys_edit_content (rid,sid, cuserid, ccontent,languages,edittime,dtype,dsource) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                        cur.execute(i_dta, [logid, sid, userid, scontent, languages, nt, dataType, dsource])
                        # mongodb save
                        sql_Query = "delete from business.summarys_edit_status where sid=%s and cuserid=%s"
                        cur.execute(sql_Query, [sid, userid])
                        rowcount = cur.rowcount
                        con.commit()
                        rest.data = {'rowcount': rowcount}
                        return rest
                    else:
                        # 报告状态未知
                        rest.errorData(errorMsg="抱歉，获取报告状态[失效]，请重新处理！")
                else:
                    rest.errorData(errorMsg="抱歉，获取报告状态[失效]，请重新处理！")
            except:
                Logger.errLineNo(msg="editSummaryDataSave error")
                rest.errorData(errorMsg="抱歉，保存摘要编辑内容异常，请稍后重试")
            finally:
                try:
                    cur.close()
                except:
                    pass
                try:
                    con.close()
                except:
                    pass
        return rest

    def getSummaryModifyLastVersionData(self, sid, languages='zh'):
        # 或者最后修改的摘要版本
        q_log = "select * from business.summarys_edit_content where sid=%s and languages=%s order by edittime desc limit 1  "
        sDF = dbutils.getPDQueryByParams(q_log, params=[sid, languages])
        return sDF
"""
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# 连接到 MongoDB
try:
    client = MongoClient("mongodb://localhost:27017/")
    print("Connected successfully to MongoDB")
except ConnectionFailure:
    print("Could not connect to MongoDB")

# 选择数据库和集合
db = client["your_database"]
collection = db["your_collection"]

# 在事务中执行操作
with client.start_session() as session:
    with session.start_transaction():
        try:
            # 在集合中插入文档
            collection.insert_one({"name": "Document1"})
            # 在另一个集合中插入文档
            another_collection = db["another_collection"]
            another_collection.insert_one({"name": "Document2"})

            # 提交事务
            session.commit_transaction()
            print("Transaction committed successfully")
        except Exception as e:
            # 发生错误时回滚事务
            session.abort_transaction()
            print(f"Transaction aborted: {e}")

"""
if __name__ == '__main__':
    adm = viewSummaryMode()
    # rt = adm.getEditSummarysStatusForUpdate("Audio_9a68601200b69f712e4ad5041af9443a", '1967')
    # print(rt.toDict())
    # rt = adm.getEditSummarysStatusForUpdate("Audio_9a68601200b69f712e4ad5041af9443a", '70')
    # print(rt.toDict())
    # rt = adm.getEditSummarysStatusForUpdate("Audio_9a68601200b69f712e4ad5041af9443a", '2046')
    # print(rt.toDict())
    # rt = adm.editSummaryDataSave("Audio_9a68601200b69f712e4ad5041af9443a", '1967',
    #                              "{'errorCode': 'USE', 'errorFlag': False, 'translateCode': '', 'errorMsg': '[robby.xia]用户正在编辑当前报告，请稍后再试。', 'data': {}}",
    #                              "en")
    # print(rt.toDict())
    # rt = adm.releaseSummaryEditStatus("Audio_9a68601200b69f712e4ad5041af9443a", '2046')
    # print(rt)
    # oneDF = adm.getSummaryModifyLastVersionData("Audio_9a68601200b69f712e4ad5041af9443a" )
    # rest=ResultData()
    # rest.data = {"count": 0}
    # if not oneDF.empty:
    #     summayList = oneDF[["SID", "RID", 'CCONTENT', 'LANGUAGES', 'EDITTIME', 'DTYPE', 'DSOURCE']].rename(
    #         columns=lambda x: x.capitalize()).to_json(orient='records')
    #     summayList = json.loads(summayList)
    #     rest.data = {"count": len(summayList), "data": summayList}
    #
    # print(rest.toDict())
    import docx
    from docx import Document
    from docx.shared import Pt
    from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
    def read_docx(file_path):
        doc = docx.Document(file_path)
        full_text = []
        for paragraph in doc.paragraphs:
            full_text.append(paragraph.text)
        return '\n'.join(full_text)
    text_content=read_docx(r"C:\Users\env\Downloads\禾赛和新势力.docx")

    lines = text_content.split("\n")
    summaryText = lines[4:]
    summaryText="\n".join(summaryText)
    print("*"*100)
    print(summaryText)
    print("*" * 100)