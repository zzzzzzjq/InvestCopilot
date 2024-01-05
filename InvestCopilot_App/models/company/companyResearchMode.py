
import datetime
import sys
import os
import math
import re

sys.path.append("../..")

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils import dbmongo
from InvestCopilot_App.models.cache import cacheDB as cache_db

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils

Logger = logger_utils.LoggerUtils()


class companyMode():

    def addCompanyReport(self, cpid,windCode, windName, userId,title, content,publishOn, rating=None, priceTarget=None,
                         attachments=[]):
        """
        添加公司内部研报
        :return:
        """
        rest = ResultData()
        rest.data = {'rowcount': 0}
        try:
            title_zh = "(%s) " % windCode + title
            _newdata = {
                "id": cpid,
                "title": title,
                "symbols": windCode,
                "windCode": windCode,
                "productName": windName,
                "tickers": [
                    windCode
                ],
                "summaryText": "",
                "source": "company",
                "dataType": "innerResearch",
                "publishOn": publishOn,
                "insertTime": publishOn,
                "summary": content,
                "cuserId": userId,
                "title_zh": title_zh,
                "updateTime": publishOn
            }
            if rating is not None:
                _newdata['monthRating']=rating
            if priceTarget is not None:
                _newdata['priceTarget']=priceTarget
            if len(attachments)>0:
                _newdata['attachments'] = attachments
            with dbmongo.Mongo("website") as md:
                mydb = md.db
                contentsSet = mydb["company"]
                existsData = contentsSet.find_one({'id': cpid})
                if existsData is None:
                    rts = contentsSet.insert_one(_newdata)
                    if rts.inserted_id is not None:
                        rest.data={'rowcount':1}
                else:
                    rest.errorData(errorMsg="与主题相关的研报内容已经存在，请重新修改",translateCode="addCompanyReport")
                    return rest
        except:
            Logger.errLineNo(msg="addCompanyReport error")
            rest.errorData(errorMsg="抱歉，添加报告数据失败，请稍后重试",translateCode="addCompanyReportError")
        return rest

    def editCompanyReport(self, editReportId, userId,title=None, summaryText=None, rating=None, priceTarget=None,
                         attachments=[]):
        """
        编辑公司内部研报
        :return:
        """
        rest = ResultData()
        rest.data={"rowcount":0}
        try:
            if editReportId != None:
                with dbmongo.Mongo("website") as md:
                    mydb = md.db
                    contentsSet = mydb["company"]
                    existsData = contentsSet.find_one({'id': editReportId, 'cuserId': userId})
                    if existsData is None:
                        rest.errorData(errorMsg="与主题相关的研报不存在，请重新选择",translateCode="editCompanyReport")
                        return rest
                    else:
                        windCode = existsData['windCode']
                        dbattachments=[]
                        if "attachments" in existsData:
                            dbattachments = existsData['attachments']
                        updt={}
                        if title is not None:
                            title_zh = "(%s) " % windCode + title
                            updt['title'] = title
                            updt['title_zh'] = title_zh
                        if rating is not None:
                            updt['monthRating'] = rating
                        if priceTarget is not None:
                            updt['priceTarget'] = priceTarget
                        if summaryText is not None:
                            updt['summary'] = summaryText
                        if len(attachments) > 0:
                            dbattachments.extend(attachments)
                            updt['attachments'] = dbattachments
                        rtsn = contentsSet.update_one({'id': editReportId, 'cuserId': userId},
                                                      {"$set":updt })
                        rest.data = {"rowcount": rtsn.modified_count}
        except:
            Logger.errLineNo(msg="editCompanyReport error")
            rest.errorData(errorMsg="抱歉，修改报告数据失败，请稍后重试",translateCode="editCompanyReportError")
        return rest

    def delCompanyReport(self, userId, rowId):
        """
        删除内部报告
        """
        rest = ResultData()
        rest.data={"rowcount":0}
        try:
            attachments=[]
            with dbmongo.Mongo("website") as md:
                mydb = md.db
                contentsSet = mydb["company"]
                existsData = contentsSet.find_one({'id': rowId, 'cuserId': userId})
                if existsData is None:
                    rest.errorData(errorMsg="与主题相关的研报不存在，请重新选择",translateCode="delCompanyReport")
                    return rest
                else:
                    if "attachments" in existsData:
                        attachments=existsData['attachments']
                    rts = contentsSet.delete_one({'id': rowId, 'cuserId': userId})
                    rowcount = rts.deleted_count
                    # if rts.deleted_count < 1:
                    #     rest.errorData(errorMsg="与主题相关的研报不存在，请重新选择",translateCode="delCompanyReport")
            rest.data = {"rowcount":rowcount}
            #删除附件
            try:
                if isinstance(attachments,list):
                    for dbattachment in attachments:
                        dattachmentPath = dbattachment['attachmentPath']
                        if os.path.exists(dattachmentPath):
                            os.remove(dattachmentPath)
                            Logger.info(f'文件 {dattachmentPath} 已删除')
            except:
                Logger.errLineNo(msg="删除公司研报附件异常")
        except:
            Logger.errLineNo(msg="delCompanyReport error")
            rest.errorData(errorMsg="抱歉，删除报告数据失败，请稍后重试",translateCode="delCompanyReportError")
        return rest

    def delCompanyReportAttachment(self, userId, rowId,attachmentId):
        """
        删除内部报告
        """
        rest = ResultData()
        rest.data={"rowcount":0}
        try:
            rowcount=0
            attachments=[]
            with dbmongo.Mongo("website") as md:
                mydb = md.db
                contentsSet = mydb["company"]
                existsData = contentsSet.find_one({'id': rowId, 'cuserId': userId})
                if existsData is None:
                    rest.errorData(errorMsg="与主题相关的研报不存在，请重新选择",translateCode="delCompanyReportAttachment")
                    return rest
                else:
                    if "attachments" in existsData:
                        attachments=existsData['attachments']
                        upattachments=[]
                        for dbattachment in attachments:
                            dattachmentId=dbattachment['attachmentId']
                            dattachmentPath=dbattachment['attachmentPath']
                            if attachmentId==dattachmentId:
                                if os.path.exists(dattachmentPath):
                                    os.remove(dattachmentPath)
                                    Logger.info(f'文件 {dattachmentPath} 已删除')
                                continue
                            upattachments.append(dbattachment)
                        if len(upattachments)==0:
                            rts = contentsSet.update_one({'id': rowId, 'cuserId': userId},{"$unset":{"attachments":1}})
                        else:
                            rts = contentsSet.update_one({'id': rowId, 'cuserId': userId},{"$set":{"attachments":upattachments}})
                        rowcount = rts.modified_count
                    # if rts.deleted_count < 1:
                    #     rest.errorData(errorMsg="与主题相关的研报不存在，请重新选择",translateCode="delCompanyReport")
            rest.data = {"rowcount":rowcount}
        except:
            Logger.errLineNo(msg="delCompanyReportAttachment error")
            rest.errorData(errorMsg="抱歉，删除报告附件失败，请稍后重试",translateCode="delCompanyReportAttachmentError")
        return rest

    def getSummaryDataByPage(self, dataBase, dbName, windCode=None, queryTitle=None,translation="zh",userIds=[],page=1,pageSize=5):
        rest = ResultData()
        rtdata={"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[]}
        rest.data=rtdata
        try:
            # 用户名与公司关系
            userCfg_dt = cache_db.getUserConfig_dt()
            querys = {"updateTime": {"$exists": True}}
            querys["cuserId"] = {"$in":userIds}  # 公司所有关公编号
            # 定义排序条件
            sort = {"publishOn": -1}  # 1表示升序，-1表示降序
            # 计算跳过的文档数
            skip = (page - 1) * pageSize
            projection = {"_id": 0, "title": 1,"id": 1, "title_zh": 1, "source": 1, "dataType": 1, "publishOn": 1,
                          "cuserId": 1,
                          "updateTime": 1, "monthRating": 1, "priceTarget": 1, "attachments": 1, "windCode": 1,}
            summaryDatas=[]
            with dbmongo.Mongo(dataBase) as md:
                mydb = md.db
                contentsSet = mydb[dbName]
                if windCode is not None:
                    windCode2 = str(windCode).split(".")[0]  # 无后缀
                    # Logger.info("[windCode,windCode2]:%s"%[windCode,windCode2])
                    querys['tickers'] = {"$in": [windCode, windCode2]}
                # 标题搜索
                if queryTitle is not None:
                    orCols = []
                    # 构建正则表达式模式
                    regex_pattern = '|'.join(map(re.escape, [queryTitle]))
                    regex = re.compile(regex_pattern, re.IGNORECASE)
                    orCols.append({"tickers": {"$regex": regex}})
                    regex = re.compile(queryTitle, re.IGNORECASE)  # 使用正则表达式进行模糊查询
                    if translation in ['zh']:
                        orCols.append({"title_zh": {"$regex": regex}})
                    else:
                        orCols.append({"title_en": {"$regex": regex}})
                    productNamex = re.compile(queryTitle, re.IGNORECASE)  # 使用正则表达式进行模糊查询
                    orCols.append({"productName": {"$regex": productNamex}})
                    if len(orCols) > 0:
                        querys["$or"] = orCols
                    # regex = re.compile(queryTitle, re.IGNORECASE)  # 使用正则表达式进行模糊查询
                    # querys['title_zh'] = {"$regex": regex}
                # print("querys:",querys)
                # 分页总记录数
                qCountlist =[]
                qCountlist.append({"$match": querys})  # 过滤符合条件的文档
                qCountlist.append({"$count": "total_orders"})  # # 第二个阶段：计算符合条件的文档总数
                queryresult = list(contentsSet.aggregate(qCountlist))
                total_orders = 0
                if queryresult:
                    total_orders = queryresult[0]["total_orders"]
                rtdata['totalNum'] = total_orders
                rtdata['pageTotal'] = math.ceil(total_orders / pageSize)
                # 同一个集合下分页
                contentsSets = list(contentsSet.aggregate([
                    {"$match": querys},  # 过滤符合条件的文档
                    {"$sort": sort},  # 根据排序条件排序
                    {"$skip": skip},  # 跳过文档
                    {"$limit": pageSize},  # 限制结果数量
                    {"$project": projection},  # 筛选要显示的字段
                ]))
                for s in contentsSets:
                    if 'summary' in s:  # 摘要去空格
                        s['summary'] = str(s['summary']).strip()
                    if translation in ['zh']:
                        if 'title_zh' in s:  # 用中文标题替换英文标题
                            s['title'] = s['title_zh']
                    if "cuserId" in s:
                        s_userId = s["cuserId"]
                        if s_userId in userCfg_dt:
                            udt = userCfg_dt[s_userId]
                            username = udt['USERNICKNAME']
                            ucompanyName = udt['SHORTCOMPANYNAME']
                            # companyId = udt['COMPANYID']
                            s['nusername'] = username
                            s['ncompanyname'] = ucompanyName
                    attachments=[]
                    if "attachments" in s:
                        attachments = s['attachments']
                    s['attachments'] = len(attachments)
                    s['secondType'] = "1"
                    summaryDatas.append(s)
                rtdata['data'] = summaryDatas
            rest.data = rtdata
            return rest
        except:
            Logger.errLineNo(msg="getSummaryDataByPage error")
            rest.errorData(errorMsg="抱歉，数据加载失败，请稍后重试！")
        return rest