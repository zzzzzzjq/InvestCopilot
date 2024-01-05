import datetime
import traceback

from InvestCopilot_App.models.toolsutils import dbmongo
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
import logging
class taskMode():
    def futu_news_repeat(self):
        #处理futu新闻重复数据
        try:
            # 查找重复数据：
            pipeline = [
                # {
                #     "$match": {
                #         "wireId": {"$exists":True }
                #     }
                # },
                {
                    "$group": {
                        # "_id": "$wireId",
                        "_id": "$title",
                        "count": {"$sum": 1},
                        "duplicates": {"$addToSet": "$id"},
                        # "insertTime": {"$first": "$insertTime"}
                    }
                },
                {
                    "$match": {
                        "count": {"$gt": 1}
                    }
                },
            ]
            with dbmongo.Mongo("news") as md:
                mydb = md.db
                collection = mydb["futu_contents"]
                result = collection.aggregate(pipeline, allowDiskUse=True)
                for doc in result:
                    # 查找重复数据
                    fids = doc['duplicates']
                    cfdata = collection.find({"id": {"$in": fids}},
                                             {"_id": 0, "id": 1, "data_display": 1}).sort(
                        [("insertTime", 1)]).limit(1)
                    rmdt = list(cfdata)
                    if len(rmdt) > 0:
                        fdt = rmdt[0]
                        if "data_display" in fdt:
                            continue
                        # print('rmdt[0]["id"]', rmdt[0]["id"], rmdt[0]["insertTime"])
                        uid = rmdt[0]["id"]
                        rtrs=collection.update_one({"id":uid},{"$set":{"data_display":0}})
                        if rtrs.modified_count>0:
                            logging.info("futu:%s:data_display"%uid)
        except:
            logging.error(traceback.format_exc())
    def research_repeat(self,preDays=0):
        querys={}
        try:
            if preDays>0:
                querys['insertTime']=(datetime.datetime.now() - datetime.timedelta(days=preDays)).strftime("%Y-%m-%d %H:%M:%S")
            pipeline=[
                {"$match": querys},
                {
                "$addFields": {
                    "createdAtDateTime": {
                        "$dateFromString": {
                            "dateString": "$insertTime"
                        }
                    }
                }
                },
                {
                    "$group": {
                        "_id":  "$title" ,
                        "count": {"$sum": 1},
                        "minCreatedAt": {"$min": "$createdAtDateTime"},
                        "maxCreatedAt": {"$max": "$createdAtDateTime"}
                    }
                },
                {
                    "$match": {
                        "count": {"$gt": 1},
                        # "minCreatedAt": {"$gte": four_days_ago}
                    }
                }
            ]
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            for dbs in dbRst.information_datas:
                dataBase = dbs['website']
                dbName = dbs['dbset']
                datatype = dbs['datatype']
                if datatype not in ['research']:
                    continue
                if dbName in ['yipit']:
                    continue
                with dbmongo.Mongo(dataBase) as md:
                    mydb = md.db
                    collection = mydb[dbName]
                    result = collection.aggregate(pipeline, allowDiskUse=True)
                    for doc in result:
                        title = doc['_id']
                        minCreatedAt = doc['minCreatedAt']
                        maxCreatedAt = doc['maxCreatedAt']
                        fdays = (maxCreatedAt - minCreatedAt).days
                        # print("createdAtDateTime:", fdays, doc)
                        if fdays <= 4:
                            cfdata = collection.find({"title": title},
                                                     {"_id": 0, "id": 1, "data_display": 1}).sort(
                                [("insertTime", 1)]).limit(1)
                            rmdt = list(cfdata)
                            if len(rmdt) > 0:
                                fdt = rmdt[0]
                                if "data_display" in fdt:
                                    continue
                                # print(dbName,'rmdt[0]["id"]', rmdt[0]["id"])
                                uid = rmdt[0]["id"]
                                rtrs=collection.update_one({"id":uid},{"$set":{"data_display":0}})
                                if rtrs.modified_count>0:
                                    logging.info("%s:%s:data_display"%(dbName,uid))
        except:
            logging.error(traceback.format_exc())
    def news_repeat(self,preDays=0):
        querys={}
        try:
            if preDays>0:
                querys['insertTime']=(datetime.datetime.now() - datetime.timedelta(days=preDays)).strftime("%Y-%m-%d %H:%M:%S")
            pipeline=[ {
                "$addFields": {
                    "createdAtDateTime": {
                        "$dateFromString": {
                            "dateString": "$insertTime"
                        }
                    }
                }
            },
                {
                    "$group": {
                        "_id":  "$title" ,
                        "count": {"$sum": 1},
                        "minCreatedAt": {"$min": "$createdAtDateTime"},
                        "maxCreatedAt": {"$max": "$createdAtDateTime"}
                    }
                },
                {
                    "$match": {
                        "count": {"$gt": 1},
                        # "minCreatedAt": {"$gte": four_days_ago}
                    }
                }
            ]

            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            for dbs in dbRst.information_datas:
                dataBase = dbs['website']
                dbName = dbs['dbset']
                datatype = dbs['datatype']
                if datatype not in ['news']:
                    continue
                if dbName in ['yipit']:
                    continue
                with dbmongo.Mongo(dataBase) as md:
                    mydb = md.db
                    collection = mydb[dbName]
                    result = collection.aggregate(pipeline, allowDiskUse=True)
                    for doc in result:
                        title = doc['_id']
                        minCreatedAt = doc['minCreatedAt']
                        maxCreatedAt = doc['maxCreatedAt']
                        fdays = (maxCreatedAt - minCreatedAt).days
                        if fdays <=1:
                            cfdata = collection.find({"title": title},
                                                     {"_id": 0, "id": 1, "data_display": 1}).sort(
                                [("insertTime", 1)]).limit(1)
                            rmdt = list(cfdata)
                            if len(rmdt) > 0:
                                fdt = rmdt[0]
                                if "data_display" in fdt:
                                    continue
                                # print(dbName,'rmdt[0]["id"]', rmdt[0]["id"])
                                uid = rmdt[0]["id"]
                                rtrs=collection.update_one({"id":uid},{"$set":{"data_display":0}})
                                if rtrs.modified_count>0:
                                    logging.info("%s:%s:data_display"%(dbName,uid))
        except:
            logging.error(traceback.format_exc())

if __name__ == '__main__':
    # taskMode().research_repeat()
    taskMode().research_repeat(preDays=5)
    taskMode().news_repeat(preDays=5)