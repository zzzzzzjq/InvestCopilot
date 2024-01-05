import re
import math
import datetime
import traceback
import InvestCopilot_App.models.toolsutils.dbutils as  dbutils

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils

from InvestCopilot_App.models.toolsutils import dbmongo

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()


class transMode():
    def getCallDataById(self, cid,userId=None):
        if userId is not None:
            sql_Query = "select * from business.callmanger where cid=%s and cuserid=%s "
            callDF = dbutils.getPDQueryByParams(sql_Query, params=[cid,userId])
        else:
            sql_Query = "select * from business.callmanger where cid=%s "
            callDF = dbutils.getPDQueryByParams(sql_Query, params=[cid])
        return callDF

    def getCountUploadTransNum(self,userId,cdate):
        #cdate YYYY-MM-DD 当天上传会议数量检查
        upcount=-1
        try:
            q_one="select count(1) from business.callmanger c  where  cuserid  in(select userid from companyuser c2 where companyid  =(select companyid  from companyuser c  where userid =%s))" \
                  " and  csource ='trans'" \
                  "   and cdate =%s "#and audiotextpath is not null
            con,cur = dbutils.getConnect()
            cur.execute(q_one,[userId,cdate])
            upcount=cur.fetchall()[0][0]
            cur.close()
            con.close()
        except:
            Logger.error(traceback.format_exc())
        return upcount

    def addCallSummaryData(self,TransId,TransData):
        rst=ResultData()
        rst.data={'rowcount':-1}
        try:
            with dbmongo.Mongo("website") as md:
                mydb = md.db
                bernsteSet = mydb["Trans"]
                existsData = bernsteSet.find_one({'id': TransId})
                if existsData is None:
                    rts = bernsteSet.insert_one(TransData)
                    if rts.inserted_id is not None:
                        rst.data = {'rowcount': 1}
                    else:
                        rst.data = {'rowcount': 0}
        except:
            Logger.error("TransId:%s"%TransId)
            Logger.error(traceback.format_exc())
            rst.errorData(errorMsg="保存，数据保存失败，请稍后重试")
        return rst

    def addCallData(self,ctitle,cdate,ctime,csource,cuserid,companyId,audioType,cregiest=None,
                    creplay=None,csound=None,cremark=None,
                    calllink=None,callname=None,callpwd=None,AudioID=None):
        #cid, ctitle, cdate, ctime, csource, cuserid, cregiest, creplay, csound, cremark, updatetime
        rst=ResultData()
        try:
            #字符串长度检查
            updatetime=datetime.datetime.now()
            i_data="INSERT INTO business.callmanger" \
                   "(cid, ctitle, cdate, ctime, csource, cuserid,companyId, cregiest, creplay, csound,calllink,callname,callpwd,cremark, updatetime,audiotype)" \
                   "VALUES(%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            sval=[ctitle,cdate,ctime,csource,companyId]
            if AudioID is None:
                cid='Audio_'+tools_utils.md5("".join(sval))
            else:
                cid=AudioID
            q_one="select count(1) from business.callmanger where cid=%s and cuserid=%s"
            con,cur = dbutils.getConnect()
            cur.execute(q_one,[cid,cuserid])
            fnum=cur.fetchall()[0][0]
            if fnum>0:
                errormsg = "[%s] 会议+时间+来源已经存在，请重新处理"%ctitle
                rst.errorData(errorMsg=errormsg)
            cur.execute(i_data,[cid,ctitle,cdate,ctime,csource,cuserid,companyId,cregiest,creplay,csound,calllink,callname,callpwd,cremark,updatetime,audioType])
            con.commit()
        except:
            errormsg = "保存，数据保存失败，请稍后重试！"
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst
    def addCallFileData(self,ctitle,cdate,ctime,csource,cuserid,companyId,audioType,language,cregiest=None,
                    creplay=None,csound=None,cremark=None,
                    calllink=None,callname=None,callpwd=None,AudioID=None):
        #用户上传文件数据保存
        rst=ResultData()
        try:
            #字符串长度检查
            updatetime=datetime.datetime.now()
            i_data="INSERT INTO business.callmanger" \
                   "(cid, ctitle, cdate, ctime, csource, cuserid,companyId, cregiest, creplay, csound,calllink,callname,callpwd,cremark, updatetime,audiotype,language)" \
                   "VALUES(%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            sval=[ctitle,cdate,csource,companyId]
            if AudioID is None:
                cid='Trans_'+tools_utils.md5("".join(sval))
            else:
                cid=AudioID
            q_one="select count(1) from business.callmanger where cid=%s and cuserid=%s"
            con,cur = dbutils.getConnect()
            cur.execute(q_one,[cid,cuserid])
            fnum=cur.fetchall()[0][0]
            if fnum>0:
                errormsg = "[%s]文件来源已经存在，请重新处理"%ctitle
                rst.errorData(errorMsg=errormsg)
                return rst
            cur.execute(i_data,[cid,ctitle,cdate,ctime,csource,cuserid,companyId,cregiest,creplay,csound,calllink,callname,callpwd,cremark,updatetime,audioType,language])
            con.commit()
            rst.cid=cid
        except:
            errormsg = "保存，数据保存失败，请稍后重试！"
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst

    def modifyCallAudioData(self,cuserid,cid,audioName,audioPath):
        rst=ResultData()
        try:
            #音频文件上传
            u_data="UPDATE business.callmanger" \
                   " SET  audioname=%s, audiopath=%s, audioupdatetime=%s where cid=%s and cuserid=%s "
            updatetime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            con,cur = dbutils.getConnect()
            cur.execute(u_data,[audioName,audioPath,updatetime,cid,cuserid])
            con.commit()
        except:
            errormsg = "保存，文件上传失败，请稍后重试！"
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst


    def delCallData(self,userId,id):
        rst=ResultData()
        rst.data={"rowcount":0}
        try:
            d_data="delete from  business.callmanger where cid=%s and cuserid=%s "
            con,cur = dbutils.getConnect()
            cur.execute(d_data,[id,userId])
            rowcount=cur.rowcount
            con.commit()
            rst.data= {"rowcount": rowcount}
        except:
            errormsg = "保存，删除数据失败，请稍后重试！"
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst


    def delCallSummaryData(self,userId,id):
        rst=ResultData()
        rst.data= {"rowcount": 0}
        try:
            Logger.info("userId[%s] delCallSummaryData[%s]"%(userId,id))
            with dbmongo.Mongo("website") as md:
                mydb = md.db
                audioSet = mydb["Trans"]
                rtdt =audioSet.delete_many({'id': id,"cuserId":userId})
                rst.data = {"rowcount": rtdt.deleted_count}
        except:
            errormsg = "保存，删除会议摘要数据异常，请稍后重试！"
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        return rst

    def getTransSummaryView(self,vtags=["Trans"],translation="zh",userId=None,companyIds=[],fileTypes=[],queryTitle=None,page=1,pageSize=20):
        """
        电话会议摘要数据  分页查询
        meetingTypes  电话会议分类
        :return:
        """
        rest = ResultData()
        rtdata={"page":page,"pageSize":pageSize,"totalNum":0,"pageTotal":0,"data":[]}
        rest.data=rtdata
        try:
            # 用户名与公司关系
            userCfg_dt = cache_db.getUserConfig_dt()
            #搜索查询所有
            #2、路由数据来源集合
            dbRst = mg_cfg.getInfomationDataBase(sumaryFlag="1")
            if not dbRst.errorFlag:
                dbRst.vsummaryDatas = []
                return dbRst
            infomationDBs = dbRst.information_datas
            #3、查询解析数据
            rtdata_d=[]
            #按集合进行分类
            qsets={}
            for dbs in infomationDBs:#按数据库 对集合进行合并
                print("dbs:",dbs)
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
            visitFlag=False
            if len(companyIds)==1:
                #查询当前用户所在公司的所有会议
                companyUserIds=[]
                for q_userId,uinfo in userCfg_dt.items():
                    if uinfo['COMPANYID'] == companyIds[0]:
                        companyUserIds.append(q_userId)
                querys['cuserId'] = {"$in": companyUserIds}
            else:
                querys['tickers'] = {"$in": companyIds}#所有公司的会议

            if userId is not None:#用户自己的会议
                querys['cuserId']=str(userId)
            #会议分类
            if len(fileTypes)>0:
                querys['dataType'] = {"$in": fileTypes}
            #标题搜索
            if queryTitle is not None:
                regex = re.compile(queryTitle, re.IGNORECASE)# 使用正则表达式进行模糊查询
                if translation in ['zh']:
                    querys['title_zh'] = {"$regex": regex}
                else:
                    querys['title_en'] = {"$regex": regex}
            #querys['dataType'] = {"$in": vtags}
            #只显示摘要部分
            # querys["updateTime"] = {"$exists": True}
            # querys["summary"] = {"$ne": ""}
            projection = {"_id": 0, "id": 1,"title": 1, "title_en": 1,"title_zh": 1, "publishOn": 1,
                          "source": 1, "dataType": 1, "insertTime": 1,"updateTime": 1,  "cuserId": 1,
                          "outputType": 1, "source_url": 1 ,"meetingType": 1,
                          }
            # 定义排序条件
            sort = {"publishOn": -1}  # 1表示升序，-1表示降序
            skip = (page - 1) * pageSize
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
                #分页总记录数
                qCountlist =[{"$unionWith": dbs} for dbs in dbsets]
                qCountlist.append({"$match": querys})  # 过滤符合条件的文档
                qCountlist.append({"$count": "total_orders"})  #  # 第二个阶段：计算符合条件的文档总数
                #总页数
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    queryresult = list(mydb.collection_name.aggregate(qCountlist))
                    total_orders=0
                    if queryresult:
                        total_orders = queryresult[0]["total_orders"]
                    rtdata['totalNum']=total_orders
                    rtdata['pageTotal']=math.ceil(total_orders/pageSize)
                with dbmongo.Mongo(website) as md:
                    mydb = md.db
                    contentsSets = list(mydb.collection_name.aggregate(dblist))
                    for s in contentsSets:
                        if translation in ['zh']:
                            if 'title_zh' in s:  # 中文标题
                                s['title'] = s['title_zh']
                        elif translation in ['en']:
                            if 'title_en' in s:  # 英文标题
                                s['title'] = s['title_en']
                        # if s['id'] not in  v_score_dt:
                        #     v_score_dt[s['id']]=[{'symbol':"N/A","windCode":"N/A","score":-10}]
                        #摘要计算状态
                        # querys["updateTime"] = {"$exists": True}
                        # querys["summary"] = {"$ne": ""}
                        if "updateTime" in s:
                            s['status']="1"
                        else:
                            s['status']="0" #刚上传
                        s['title_zh']=""
                        s['title_en']=""
                        #标记文档所属公司/员工
                        if "cuserId" in s:
                            s_userId = s.pop("cuserId")
                            if s_userId in userCfg_dt:
                                udt = userCfg_dt[s_userId]
                                username = udt['USERNICKNAME']
                                ucompanyName = udt['SHORTCOMPANYNAME']
                                companyId= udt['COMPANYID']
                                if  not companyId in companyIds and not visitFlag:
                                    continue
                                s['nusername'] = username
                                s['ncompanyname'] = ucompanyName
                        s['source_url']=""
                        s['cuserId']=""
                        rtdata_d.append(s)
                rtdata['data']=rtdata_d
            et = datetime.datetime.now()
            # Logger.info("part1 bt:%s,et:%s"%(bt,et))
            # print("part1 bt:%s,et:%s"%(bt,et),(et-bt).total_seconds())
            # for x in rtdata_d[0]:
            #     print("x:",x)
            # json_data = summaryDatas
            rest.data = rtdata
            return rest
        except:
            Logger.errLineNo(msg="getTransSummaryView error")
            rest.errorData(errorMsg="抱歉，数据加载失败，请稍后重试！")
        return rest

    def getAudioIds(self, beginTime, endTime, companyIds=[], tickers=[],sumaryFlag="1",language=None):
        """
        需要翻译的音频编号
        :param beginDate:
        :param endDate:
        :param windCode:
        :param isSummaryFlag:
        :return:
        """
        rest = ResultData()
        rest.data=[]
        try:
            querys = {"insertTime": {'$gte': beginTime, '$lte': endTime}}
            if language is not None:
                querys["parserLanguage"] = language
            else:
                querys["parserLanguage"] = {"$exists": False}  # 不存在whisper解析后的语音
            if sumaryFlag == "1":
                querys["summary"] = {"$eq": ""}
                querys["summaryText"] = {"$eq": ""}
                querys["translateTextTime"] = {"$exists": False}
            if len(companyIds) > 0:
                querys["companyId"] = {"$in": companyIds}
            if len(tickers) > 0:
                querys["tickers"] = {"$in": tickers}
            rtCols = {"_id": 0, "id": 1,}# 'tickers': 1, 'companyId': 1
            rtnids=[]
            with dbmongo.Mongo("website") as md:
                mydb = md.db
                contentsSet = mydb["Audio"]
                # Logger.info("getInfomationIds querys:%s"%querys)
                contentsSets = contentsSet.find(querys, rtCols).sort([('insertTime', 1)])
                nids=[]
                for ec in contentsSets:
                    nids.append(ec["id"])
                # nids = [x['id'] for x in contentsSets if x['windCode'] in nstockpool]
                rtnids.extend(nids)
            # print("nids:",len(nids),nids)
            rest.data = rtnids
            rest.count = len(rtnids)
        except:
            Logger.errLineNo(msg="getAudioIds error")
            rest.errorData(errorMsg="抱歉，获取数据失败，请稍后重试")
        return rest

    def fillAudioText(self,audioId, language=None, audioText=None, translateMode=None):
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
                if language is not None:
                    updateCell['parserLanguage']=str(language).strip()
                if translateMode is not None:
                    updateCell['translateMode']=str(translateMode).strip()
                if audioText is not None:
                    updateCell['summaryText']=str(audioText).strip()
                    updateCell['translateTextTime']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            # wx_send.send_wx_msg_operation("audioId[%s]audioText save error!"%audioId)
        return rest

    def fillAudioLanguage(self,audioId, language=None):
        """
        回填audio 音频解析语言
        :param id:
        :param summary:
        :return:
        """
        rest = ResultData()
        try:
            if (language == "" or language is None) :
                rest.errorData("音频语言不能为空!")
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
                if language is not None:
                    updateCell['parserLanguage']=str(language).strip()
                with dbmongo.Mongo(database) as md:
                    mydb = md.db
                    contentsSet = mydb[dbset]
                    querys = {"id": audioId}
                    # fone = contentsSet.find_one({"id":audioId}, {"_id":0,"language":1})
                    # if fone is not None:
                    contentsSets = contentsSet.update_one(querys, {"$set": updateCell})
                    modified_count = contentsSets.modified_count
                    rest.data = [{"id": audioId, "status": modified_count}]
            else:
                rest.data = []
        except:
            Logger.errLineNo(msg="fillAudiolanguage error")
            rest.errorData(errorMsg="抱歉，保存数据失败，请稍后重试")
            Logger.error("audioId:%s,audioText:%s"%(audioId,language))
            # wx_send.send_wx_msg_operation("audioId[%s]audioText save error!"%audioId)
        return rest

