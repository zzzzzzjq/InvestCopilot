
import json
import math
import time
import traceback
import datetime
import re
import sys
import pandas as pd
import math
sys.path.append("../..")

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
from InvestCopilot_App.models.news.newsTitleMode import newsTitleMode
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict

import logging
from InvestCopilot_App.models.toolsutils import dbmongo

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()
class newsMode():
    def getNewsDataByTitleTag(self ,titleTag,title_cores, gtScore, ltScore,vtags=["news"],translation="zh",ordTag=None,queryTitle=None,news_class=None,
                              beginDate=None, endDate=None,page=1 ,pageSize=20,companyId=None):
        """
        新闻主题数据查询  分页查询 按分类处理
        :return:
        """
        rest = ResultData()
        rtdata={"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[],"markIds":[],"firstCodes":[],"marketData":[] }
        rest.data=rtdata
        try:
            # ybsummary_dt=cache_dict.getDictByKeyNo('2')#研报摘要权限查询 配置了这个类别中的公司才能查看这些研报摘要
            # 2、路由数据来源集合
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            infomationDBs = dbRst.information_datas
            rtdata_d =[]
            # 按集合进行分类
            qsets ={}
            for dbs in infomationDBs  :  # 按数据库 对集合进行合并
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
                    qsets[dataBase] = {"website" :dataBase ,"dbsets" :[dbName]}
            # print("tickers:",tickers)
            # print("qsets:",qsets)
            querys = {}
            # querys['dataType'] = {"$in": vtags}
            #全局匹配参数
            querys["updateTime"] = {"$exists": True}
            querys["summary"] = {"$ne": ""}
            querys["title_show"] = "1"
            querys["total_score"] = {"$gte": gtScore,"$lte": ltScore}#原始值搜索
            if news_class is not None :
                querys['news_class'] = news_class  # 类别搜索
            # querys["data_display"] = {"$exists": False}#重复数据不显示 gpt处理的
            # querys["title_cores"] = {"$exists": True}
            # querys["title_cores.%s"%titleTag] = {"$gt": title_cores}
            #主题配参数
            projection = {"_id": 0, "id": 1 ,"title": 1, "title_zh": 1 ,"title_en": 1, "publishOn": 1,"source": 1,"core_reason": 1,
                          "insertTime": 1, "total_score": 1,  "show_total_score": 1, "news_class": 1,
                          "tickers": 1,"firstCode": 1,"secondType": 1,"text_emotion_score":1,"content_emotion_score":1,
                          # "tickers":1,
                          }
            isTitleSearch=False
            qwindCodes=[]
            addFields=[]
            if titleTag in ['sg_US_SPV',"sg_HK_SPV"]:
                titleCodeDF=newsTitleMode().getTitleStocks(titleTag)
                if not titleCodeDF.empty:
                    qwindCodes=titleCodeDF['WINDCODE'].values.tolist()
                querys["tickers"]={"$in":qwindCodes}
                #只看48小时内的新闻
                beginDate=(datetime.datetime.now()+datetime.timedelta(hours=-48)).strftime("%Y-%m-%d %H:%M:%S")
                endDate=(datetime.datetime.now()+datetime.timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
            elif titleTag in ['all_title']:
                #先组装字段
                addFields.append( {
                        "$addFields": {
                            "title_display_array": {"$objectToArray": "$title_display"}
                        }
                    }
                )
                #在条件查询
                querys["title_display_array.v"]={"$gt": 0}
                # querys["$expr"]={
                #     "$gt": [{"$arrayElemAt": ["$title_display_array.v", 0]}, 0]
                # }
                projection['title_display_array']=1
                projection['title_display']=1
            else:
                querys["title_display.%s"%titleTag] =1
                projection = {"_id": 0, "id": 1 ,"title": 1, "title_zh": 1 ,"title_en": 1, "publishOn": 1,"source": 1,"core_reason": 1,
                              "insertTime": 1, "total_score": 1,  "show_total_score": 1,
                              "news_class": 1,
                              "tickers": 1,"firstCode": 1,"secondType": 1,"text_emotion_score":1,"content_emotion_score":1,
                              # "tickers":1,
                              }
                if companyId in [tools_utils.globa_companyId]:
                    projection["title_cores.%s"%titleTag] =  1
            # 定义排序条件
            if ordTag in ['updateTime']:
                sort = {"insertTime": -1,"show_total_score": -1,}  # 更新时间排序
            elif ordTag in ['show_total_score','total_title_score']:
                sort = { "show_total_score": -1,"insertTime": -1,}  #  重要性 全局衰减
            else:
                sort = { "show_total_score": -1,"insertTime": -1,}  # 重要性  全局衰减
            #时间搜索
            if beginDate is not None and endDate is not None:
                querys["insertTime"] = {"$gte": beginDate, "$lte": endDate}  # 原始值搜索
            elif beginDate is not None :
                querys["insertTime"] = {"$gte": beginDate}  # 原始值搜索
            elif endDate is not None :
                querys["insertTime"] = {"$lte": endDate}  # 原始值搜索

            #标题搜索
            if queryTitle is not None:
                orCols=[]
                # 构建正则表达式模式
                regex_pattern = '|'.join(map(re.escape, [queryTitle]))
                regex = re.compile(regex_pattern, re.IGNORECASE)
                # querys['tickers'] = {"$regex": regex}
                orCols.append({"tickers":{"$regex": regex}})
                regex = re.compile(queryTitle, re.IGNORECASE)# 使用正则表达式进行模糊查询
                if translation in ['zh']:
                    # querys['title_zh'] = {"$regex": regex}
                    orCols.append({"title_zh": {"$regex": regex}})
                else:
                    # querys['title_en'] = {"$regex": regex}
                    orCols.append({"title_en": {"$regex": regex}})
                if len(orCols)>0:
                    querys["$or"] = orCols
            skip = (page - 1) * pageSize
            rvrecode={"benzinga":"bz","seekingAlpha":"sa"}
            for website ,website_dt in qsets.items():
                dbsets = website_dt['dbsets']
                # print("dataBase:",dataBase)
                dblist =[{"$unionWith": dbs} for dbs in dbsets]
                if titleTag in ['all_title']:
                    dblist.extend(addFields)
                dblist.append({"$match": querys})  # 过滤符合条件的文档
                dblist.append({"$sort": sort})  # 根据排序条件排序
                dblist.append({"$skip": skip})  # 跳过文档
                dblist.append({"$limit": pageSize})  # 限制结果数量
                dblist.append({"$project": projection})  # 筛选要显示的字段
                # print("dblist:",dblist)
                #分页总记录数
                qCountlist =[{"$unionWith": dbs} for dbs in dbsets]
                if titleTag in ['all_title']:
                    qCountlist.extend(addFields)
                qCountlist.append({"$match": querys})  # 过滤符合条件的文档
                qCountlist.append({"$count": "total_orders"})  #  # 第二个阶段：计算符合条件的文档总数
                #总页数
                total_orders = 0
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    queryresult = list(mydb.collection_name.aggregate(qCountlist))
                    if queryresult:
                        total_orders = queryresult[0]["total_orders"]
                    rtdata['totalNum']=total_orders
                    rtdata['pageTotal']=math.ceil(total_orders/pageSize)
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    contentsSets = list(mydb.collection_name.aggregate(dblist))
                markIds = [s["id"] for s in contentsSets]
                rtdata['markIds']=markIds
                firstCodes=[]
                for s in contentsSets:
                    if translation in ['zh']:
                        if 'title_zh' in s:  # 中文标题
                            s['title'] = s.pop('title_zh')
                    elif translation in ['en']:
                        if 'title_en' in s:  # 英文标题
                            s['title'] = s.pop('title_en')
                    if s['source'] in rvrecode:
                        s['source'] = rvrecode[s['source']]
                    firstCode=""
                    if titleTag in ['sg_US_SPV',"sg_HK_SPV"]:
                        tickers = s.pop('tickers')
                        mgcodes=list(set(tickers)&set(qwindCodes))
                        # print("mgcodes:",mgcodes)
                        if len(mgcodes)>0:
                            firstCode= mgcodes[0]
                        else:
                            tickers = s.pop('tickers')
                            if len(tickers) > 0:
                                firstCode = tickers[0]
                    else:
                        if "firstCode" in s:
                            firstCode = s.pop('firstCode')
                        else:
                            tickers = s.pop('tickers')
                            if len(tickers) > 0:
                                firstCode = tickers[0]
                    s['windCode']=firstCode
                    s['updateTime']=s['insertTime']
                    firstCodes.append(firstCode)
                    #news_class 翻译
                    if "news_class" in s:
                        news_class=s["news_class"]
                        if news_class in mg_cfg.news_class_dt:
                            s["news_class"]=mg_cfg.news_class_dt[news_class]
                    # s['total_score'] = s['show_total_score']
                    rtdata_d.append(s)
                rtdata['data']=rtdata_d
                if titleTag in ['sg_US_SPV',"sg_HK_SPV"]:
                    firstCodes=qwindCodes
                rtdata['firstCodes']=firstCodes
            et = datetime.datetime.now()
            # for x in rtdata_d:
            #     print("x:",x)
            rest.data = rtdata
            return rest
        except:
            Logger.errLineNo(msg="getNewsDataByTitleTag error")
            rest.errorData(errorMsg="抱歉，数据加载失败，请稍后重试！")
        return rest

    def getNewsDaySummary(self,title_id,translation="zh"):
        #新闻每日摘要
        rest = ResultData()
        try:
            rtdata_d = []
            with dbmongo.Mongo("news") as md:
                mydb = md.db
                ndset=mydb['news_day_summary']
                s=ndset.find_one({"title_id":title_id},{"_id":0})
                fdata = s['data']
                for fd  in fdata:
                    if translation in ['zh']:
                        if 'content' in fd:  # 英文
                            fd.pop('content')
                        if 'content_ch' in fd:  # 中文内容
                            fd['content'] = fd.pop('content_ch')
                    elif translation in ['en']:
                        if 'content' in fd:  # 英文
                            fd['content'] = fd.pop('content')
                        if 'content_ch' in fd:  # 中文内容
                            fd.pop('content_ch')
                    if "update_time" in fd:
                        update_time=fd.pop('update_time')
                        fd["update_time"]=update_time.strftime("%Y-%m-%d %H:%M:%S")
                    rtdata_d.append(fd)
            rest.data = {"title_id":title_id,"data":rtdata_d}
            return rest
        except:
            Logger.errLineNo(msg="getNewsDataByTitleTag error")
            rest.errorData(errorMsg="抱歉，数据加载失败，请稍后重试！")
        return rest
        pass
if __name__ == '__main__':
    s=newsMode().getNewsDaySummary("chinese_consumer",translation='zh')
    print(s.toDict())