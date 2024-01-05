# encoding: utf-8
#股票聊天 调用蔡博士API
import datetime

import math
from django.http import JsonResponse
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.business.likeMode import likeMode
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from InvestCopilot_App.models.toolsutils import dbmongo
from bson import ObjectId
Logger = logger_utils.LoggerUtils()

import json
import os
import re

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10


class stockChatMode():
    def getStockChatRequestNum(self,userId,companyId):
        #添加对话list
        rest = ResultData()
        try:
            if userId in['2029','2027']:
                return rest
            stockchatnum_dt=cache_dict.getDictByKeyNo('1')#股票聊天日上限
            dayquerynum=10
            if companyId in stockchatnum_dt:
                dayquerynum=int(stockchatnum_dt[companyId])
            with dbmongo.Mongo("gptchat") as md:
                mydb = md.db
                conversationsSet = mydb["stockchat_conv_result"]
                try:
                    btime=(datetime.datetime.now()+datetime.timedelta(hours=-24)).strftime("%Y-%m-%d %H:%M:%S")
                    #ctdt["$gte"]=datetime.datetime.strptime(gte,"%Y-%m-%d %H:%M:%S")
                    dbquerynum = conversationsSet.count_documents({'userId': userId,"create_time":{"$gte":datetime.datetime.strptime(btime,"%Y-%m-%d %H:%M:%S") }})
                    if dbquerynum>=dayquerynum:
                        errormsg = "对不起，您24小时内查询次数已达到上限[%s]次，请稍后再访问。感谢您的理解和支持。"%dayquerynum
                        rest.errorData(errorMsg=errormsg)
                    return rest
                except:
                    Logger.errLineNo(msg="getStockChatRequestNumError")
        except:
            Logger.errLineNo(msg="getStockChatRequestNumError")
            rest.errorData(errorMsg="抱歉，获取对话数据失败，请稍后重试",translateCode="getStockChatRequestNumError")
        return rest

    def addStockChatConv(self,chat_id,title,userId,create_time):
        #添加对话list
        rest = ResultData()
        rest.data = {'rowcount': 0}
        try:
            _newdata = {
                "conversation_id": chat_id,
                "title": title,
                "userId": userId,
                "create_time": create_time,
            }
            with dbmongo.Mongo("gptchat") as md:
                mydb = md.db
                conversationsSet = mydb["stockchat_conv"]
                try:
                    existsData = conversationsSet.find_one({'conversation_id': chat_id})
                    if existsData is None:
                        rts = conversationsSet.insert_one(_newdata)
                        if rts.inserted_id is not None:
                            rest.data={'rowcount':1}
                except:
                    Logger.errLineNo(msg="addConversationsError")
        except:
            Logger.errLineNo(msg="addConversationsError")
            rest.errorData(errorMsg="抱歉，添加对话数据失败，请稍后重试",translateCode="addConversationsError")
        return rest

    def addStockChatConvResult(self,chat_id,userId,title,update_time,resultData,result_id=None):
        #添加对话返回结果
        rest = ResultData()
        rdt= {'modified_count': 0,'add_count': 0,'result_id':"-1"}
        rest.data =rdt
        try:
            _newdata = {
                "conversation_id": chat_id,
                "userId": userId,
                "title": title,
                "create_time": update_time,
                "respdata":resultData
            }
            with dbmongo.Mongo("gptchat") as md:
                mydb = md.db
                conversationsSet = mydb["stockchat_conv"]
                rtudt=conversationsSet.update_one({'conversation_id': chat_id,'userId': userId},{"$set":{"update_time":update_time}})
                conversations_resultSet = mydb["stockchat_conv_result"]
                if result_id is not None:
                    hisdt=conversations_resultSet.update_one({'_id': ObjectId(result_id),'userId': userId},{"$set":{"display":0}})
                rtadt=conversations_resultSet.insert_one(_newdata)
                add_count = 0
                if rtadt.inserted_id is not None:
                    add_count=1
                rest.data ={'modified_count':rtudt.modified_count, 'add_count': add_count,'result_id':str(rtadt.inserted_id) }
        except:
            Logger.errLineNo(msg="addConversationsResultError")
            rest.errorData(errorMsg="抱歉，保存返回结果数据失败，请稍后重试",translateCode="addConversationsResultError")
        return rest

    def getStockChatConv(self,userId,page=1,pageSize=20):
        rest = ResultData()
        rtdata={"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[],}
        rest.data=rtdata
        try:
            website="gptchat"
            webset="stockchat_conv"
            sort = {"create_time": -1}  # 更新时间排序
            skip = (page - 1) * pageSize
            projection={"_id":0,"userId":0}
            querys = {}
            querys["userId"] =userId
            querys["display"] = {"$exists":False}
            dblist = []
            dblist.append({"$match": querys})  # 过滤符合条件的文档
            dblist.append({"$sort": sort})  # 根据排序条件排序
            dblist.append({"$skip": skip})  # 跳过文档
            dblist.append({"$limit": pageSize})  # 限制结果数量
            dblist.append({"$project": projection})  # 筛选要显示的字段
            # print("dblist:",dblist)
            # 分页总记录数
            qCountlist = []
            qCountlist.append({"$match": querys})  # 过滤符合条件的文档
            qCountlist.append({"$count": "total_orders"})  # # 第二个阶段：计算符合条件的文档总数
            # 总页数
            total_orders = 0
            with dbmongo.Mongo(website) as md:
                mydb = md.db
                setdb=mydb[webset]
                queryresult = list(setdb.aggregate(qCountlist))
                if queryresult:
                    total_orders = queryresult[0]["total_orders"]
                rtdata['totalNum'] = total_orders
                rtdata['pageTotal'] = math.ceil(total_orders / pageSize)
            with dbmongo.Mongo(website) as md:
                mydb = md.db
                setdb=mydb[webset]
                contentsSets = list(setdb.aggregate(dblist))
            rtdata['data'] = contentsSets
            rest.data = rtdata
        except:
            Logger.errLineNo(msg="getStockChatConvError")
            rest.errorData(errorMsg="抱歉，对话数据加载失败，请稍后重试！")
        return rest


    def delStockChatConv(self,userId,conversation_id):
        #删除对话列表
        rest = ResultData()
        rtdata={"rowcount":-1}
        rest.data=rtdata
        try:
            website="gptchat"
            webset="stockchat_conv"
            with dbmongo.Mongo(website) as md:
                mydb = md.db
                setdb=mydb[webset]
                hisdt = setdb.update_one({'conversation_id':conversation_id, 'userId': userId},
                                                           {"$set": {"display": 0}})
                #所有答案不显示
                conversations_resultSet = mydb["stockchat_conv_result"]
                hisdt2 = conversations_resultSet.update_many({'conversation_id':conversation_id, 'userId': userId},
                                                               {"$set": {"display": 0}})
                rtdata["rowcount"] = hisdt.modified_count
            rest.data = rtdata
        except:
            Logger.errLineNo(msg="delStockChatConv")
            rest.errorData(errorMsg="抱歉，删除对话数据失败，请稍后重试！")
        return rest

    def likeStockChatConv(self,userId,result_id,slike):
        #点赞对话列表
        rest = ResultData()
        rtdata={"rowcount":-1}
        rest.data=rtdata
        try:
            website="gptchat"
            with dbmongo.Mongo(website) as md:
                mydb = md.db
                conversations_resultSet = mydb["stockchat_conv_result"]
                fone = conversations_resultSet.find_one({'_id':  ObjectId(result_id), 'userId': userId})
                if fone is None:
                    rest.data = rtdata
                    return rest
                if slike in ['Cancel']:
                    hisdt = conversations_resultSet.update_one({'_id':  ObjectId(result_id), 'userId': userId},
                                                               {"$unset": {"slike": 1}})
                    rtdata["rowcount"] = hisdt.modified_count
                    rest.data=rtdata
                    return rest
                else:
                    if "slike" in fone:
                        dbslike = fone['slike']
                        if dbslike == slike:
                            rest.data = {'rowcount': 0}
                            return rest
                    hisdt2 = conversations_resultSet.update_one({'_id':  ObjectId(result_id), 'userId': userId},
                                                       {"$set": {"slike": slike}})
                    rtdata["rowcount"] = hisdt2.modified_count
            rest.data = rtdata
        except:
            Logger.errLineNo(msg="likeStockChatConv")
            rest.errorData(errorMsg="抱歉，对话点赞失败，请稍后重试！")
        return rest

    def editStockChatConv(self,userId,conversation_id,conv_title):
        #修改对话名字
        rest = ResultData()
        rtdata={"rowcount":-1}
        rest.data=rtdata
        try:
            website="gptchat"
            webset="stockchat_conv"
            update_time=datetime.datetime.now()
            with dbmongo.Mongo(website) as md:
                mydb = md.db
                setdb=mydb[webset]
                hisdt = setdb.update_one({'conversation_id':conversation_id, 'userId': userId},
                                                           {"$set": {"title": str(conv_title),"create_time":update_time}})
                rtdata["rowcount"] = hisdt.modified_count
            rest.data = rtdata
        except:
            Logger.errLineNo(msg="editStockChatConv")
            rest.errorData(errorMsg="抱歉，修改对话名称失败，请稍后重试！")
        return rest

    def getStockChatConvResult(self,conversation_id,userId,page=1,pageSize=20):
        rest = ResultData()
        rtdata={"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[],}
        rest.data=rtdata
        try:
            website="gptchat"
            webset="stockchat_conv_result"
            sort = {"create_time": -1}  # 更新时间排序
            skip = (page - 1) * pageSize
            projection={"userId":0}
            querys = {}
            querys["conversation_id"] =conversation_id
            querys["userId"] =userId
            querys["display"] = {"$exists":False}
            dblist = []
            dblist.append({"$match": querys})  # 过滤符合条件的文档
            dblist.append({"$sort": sort})  # 根据排序条件排序
            dblist.append({"$skip": skip})  # 跳过文档
            dblist.append({"$limit": pageSize})  # 限制结果数量
            dblist.append({"$project": projection})  # 筛选要显示的字段
            # print("dblist:",dblist)
            # 分页总记录数
            qCountlist = []
            qCountlist.append({"$match": querys})  # 过滤符合条件的文档
            qCountlist.append({"$count": "total_orders"})  # # 第二个阶段：计算符合条件的文档总数
            # 总页数
            total_orders = 0
            with dbmongo.Mongo(website) as md:
                mydb = md.db
                setdb=mydb[webset]
                queryresult = list(setdb.aggregate(qCountlist))
                if queryresult:
                    total_orders = queryresult[0]["total_orders"]
                rtdata['totalNum'] = total_orders
                rtdata['pageTotal'] = math.ceil(total_orders / pageSize)
            rtdatas=[]
            with dbmongo.Mongo(website) as md:
                mydb = md.db
                setdb=mydb[webset]
                contentsSets = list(setdb.aggregate(dblist))
                for sc in contentsSets:
                    _id=sc.pop("_id")
                    sc["result_id"]=str(_id)
                    rtdatas.append(sc)
            rtdata['data'] = rtdatas
            rest.data = rtdata
        except:
            Logger.errLineNo(msg="getStockChatConvResultError")
            rest.errorData(errorMsg="抱歉，对话数据加载失败，请稍后重试！")
        return rest


if __name__ == '__main__':
    chat_id="asd478818959fd6578s88d969"
    title="特斯拉这个季度股价表现如何"
    userId="1769"
    create_time=datetime.datetime.now()
    resultData={
    "Message": "",
    "Status": "Success",
    "embedding_time": 0.60286545753479,
    "group_texts": [
        {
            "date": "2023-10-09 14:11:50",
            "texts": "Related Link: Here's How Many Vehicles Tesla Has Delivered, Produced In Each Quarter Since 2019 Monday morning, Wells Fargo analyst Colin Langan maintained Tesla with an Equal-Weight rating and lowered the price target from $265 to $260. Tesla is set to report third-quarter financial results after the market closes on Oct. 18. The company is expected to report quarterly earnings of 80 cents per share and revenue of $24.79 billion, according to estimates from Benzinga Pro. TSLA Price Action: Tesla shares were down 2.26% at $254.65 at the time of publication, per Benzinga Pro. Photo: courtesy of Tesla.",
            "title": "Tesla Stock Is Trading Lower: What's Going On?"
        },
        {
            "date": "2023-10-18 15:03:37",
            "texts": "Tesla produced 430,488 vehicles during the third quarter, down from the 479,700 the company produced in the second quarter. The EV giant said the decline in vehicle production was due to factory upgrades. Read more here... Ahead of the event, two analysts weighed in on Tesla stock Monday. Piper Sander maintained an Overweight rating on Tesla and lowered a price target from $300 to $290. Wedbush analyst Daniel Ives reiterated an Outperform rating and maintained a price target of $350. From a technical analysis perspective, Tesla’s stock looks bearish heading into the event, trading in a downtrend and breaking down from a bearish inside bar pattern. It should be noted that holding stocks or options over an earnings print is akin to gambling because stocks can react bullishly to an earnings miss and bearishly to an earnings beat. Traders and Investors looking to play the possible upside in Tesla stock but with diversification may choose to take a position in the AXS 2X Innovation ETF TARK.",
            "title": "Trading Strategies For Tesla Stock Before And After Q3 Earnings"
        },
    ],
    "query_filter": " (company == 5)  and  (month == 202307 or month == 202308 or month == 202309 or month == 202310) ",
    "query_text": "How has the stock price of Tesla performed this quarter?",
    "search_time": 0.016914844512939453,
    "summaries": [
        {
            "model": "gpt-4",
            "summary": "在这个季度中，特斯拉的股价表现并不好。在第三季度公布财报后，由于公司生产数量以及收益较预期有所下滑，导致股票价格出现大幅下滑。具体来说，特斯拉在本季度的产量从上一季度的479,700辆下降到了430,488辆（参考段落[2]，[3]）。虽然公司在第二季度的表现好于预期，但是在公布第三季度财报之后，股价在一周内下降了近10%并在一周内下降了约15%（参考段落[3]，[8]）。此外，该公司的财报也低于分析师的预期，报告的收入为233.5亿美元，低于预期的247.9亿美元，且每股的收益为0.66美元，低于预期的0.73美元（参考段落[1]，[7]）。总的来说，特斯拉在本季度的股价表现不佳。",
            "time": 31.2778103351593
        },
        {
            "model": "gpt-4-1106-preview",
            "summary": "特斯拉在这个季度的股价表现是下跌的。第三季度，该公司的股票在宣布了比分析师预期更差的财务结果后交易量下降。尽管收入同比增加了8.9%，达到了233.5亿美元，但这低于分析师预期的24.79亿美元，并且每股收益也低于分析师的共识估计0.73美元，实际为0.66美元。此外，第三季度财报发布后，股价接近10%的下跌，并在随后的一周结束时下跌了15%，标志着该股票今年最差的周表现（参见[3]段和[7]段和[8]段）。此外，特斯拉在第三季度的车辆生产量下降，这与分析师预期的455,000辆相比也有所减少（参见[2]段和[6]段）。\n\n综上所述，特斯拉本季度的第三季度财报未能满足市场分析师的预期，导致其股价出现明显下滑。在CEO Elon Musk在财报电话会议上对经济表达出悲观的情绪后，分析师们变得更加谨慎，这进一步加剧了市场对股价的担忧（参见[8]段）。",
            "time": 22.414378881454468
        }
    ],
    "transform_time": 9.652528047561646
}

    # rt = stockChatMode().addStockChatConv(chat_id,title,userId,create_time)
    # rt = stockChatMode().addStockChatConvResult(chat_id,title,create_time,resultData)
    # rt = stockChatMode().getStockChatConv("1769")
    # rt = stockChatMode().getStockChatConvResult("asd478818959fd6578s88d969")
    # print(rt.toDict())
    # rtd = rt.toDict()
    # for d in rtd['data']['data']:
    #     print("d:",d)
    #     print("*"*10)

    s= cache_dict.getDictByKeyNo('1',reload=True)  #
    print(s)
    if "gs_njjhtz" in s:
        sss=s['gs_njjhtz']
        print(sss)

    rsp = stockChatMode().getStockChatRequestNum("1967",'pinnacle')
    print(rsp)