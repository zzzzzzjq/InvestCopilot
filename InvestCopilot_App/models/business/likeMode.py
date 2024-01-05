# -*- coding: utf-8 -*-
#点赞+点评 模块
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
import math
import threading
import pandas as pd
from pymongo import UpdateOne

from django.http import HttpResponseRedirect
sys.path.append("../..")

import InvestCopilot_App.models.cache.dict.dictCache as cache_dict

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.user.userMode import cuserMode
from  InvestCopilot_App.models.market.snapMarket import snapUtils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from  InvestCopilot_App.models.stocks.stockUtils import stockUtils

import logging
from InvestCopilot_App.models.toolsutils import dbmongo

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

class likeMode():
    def addLike(self,sid,userId,slike,markType="summary"):
        #用户摘要点赞
        userId=str(userId)
        slike=str(slike)
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            createDate = datetime.datetime.now()
            q_one = "select slike from business.summary_like where userid=%s and sid=%s and markType=%s"
            oneDF=pd.read_sql(q_one,con,params=[userId,sid,markType])
            if oneDF.empty:#add
                insertSql = "INSERT INTO business.summary_like(sid,userid,slike,updatetime,markType,flag) VALUES (%s,%s,%s,%s,%s,'1')"
                cur.execute(insertSql, [sid,userId,slike, createDate,markType])
                rowcount=cur.rowcount
                con.commit()
                rst.data={'rowcount':rowcount}
            else:#update
                if slike in ['Cancel']:
                    # 删除
                    delslike = "delete from  business.summary_like where  userid=%s and sid=%s and markType=%s"
                    cur.execute(delslike, [userId, sid, markType])
                    rowcount = cur.rowcount
                    con.commit()
                    rst.data = {'rowcount': rowcount}
                else:
                    if not oneDF.iloc[0].slike==str(slike):
                        upslike = "update business.summary_like set  slike=%s,updatetime=%s  where  userid=%s and sid=%s and markType=%s"
                        cur.execute(upslike, [slike,createDate,userId,sid,markType])
                        rowcount=cur.rowcount
                        con.commit()
                        rst.data={'rowcount':rowcount}
                    else:
                        rst.data = {'rowcount': 0}
            return rst
        except:
            msg = "Sorry, add data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="addLikeError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def delLike(self,sid,userId,markType='summary'):
        # 用户删除摘要点赞
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            d_like  = "delete from  business.summary_like  where userid=%s and sid=%s and markType=%s"
            cur.execute(d_like,[userId,sid,markType])
            rowcount=cur.rowcount
            con.commit()
            data={'rowcount':rowcount}
            rst.data=data
            return rst
        except:
            msg = "Sorry, delete data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="delLike",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getLikeNum(self,sid,markType="summary"):
        # 查询点赞数据
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            q_count = "select slike,count(slike) from  business.summary_like  where sid=%s and markType=%s group by slike"
            cur.execute(q_count,[sid,markType])
            rts={}
            rsdts = cur.fetchall()
            for rtd in rsdts:
                rts[rtd[0]]=rtd[1]
            rst.data=rts
            return rst
        except:
            msg = "Sorry, get data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getLikeNumError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getLikeData(self,sid,userId,markType="summary"):
        # 查询用户是否点赞
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            q_count = "select slike from  business.summary_like  where sid=%s and userid=%s and markType=%s"
            cur.execute(q_count,[sid,userId,markType])
            rts={}
            rsdts = cur.fetchall()
            for rtd in rsdts:
                rts[rtd[0]]="1"
            rst.data=rts
            return rst
        except:
            msg = "Sorry, get data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getLikeDataError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getMarkData(self,userId,sids,markType="news"):
        # 查询用户新闻标注
        rst = ResultData()
        try:
            if isinstance(sids,str):
                sids=[sids]
            con, cur = dbutils.getConnect()
            placeholders  = ', '.join(['%s' for _ in sids])
            q_count = "select sid,slike from  business.summary_like  where sid in ({}) and userid=%s and markType=%s".format(placeholders )
            sids.extend([userId,markType])
            cur.execute(q_count,sids)
            rts={}
            rsdts = cur.fetchall()
            for rtd in rsdts:
                rts[rtd[0]]=rtd[1]
            rst.data={"data":rts,"records":len(rts)}
            return rst
        except:
            msg = "Sorry, get data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getMarkDataError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def addComments(self,sid,userId,comments):
        # 添加评论
        try:
            sid=str(sid)
            userId=str(userId)
            rst = ResultData()
            con, cur = dbutils.getConnect()
            updatetime = datetime.datetime.now()
            qsql = "select count(1) from business.summary_comments where sid=%s and userid=%s"
            cur.execute(qsql, [str(sid), str(userId)])
            qnum = cur.fetchone()[0]
            if qnum > 0:
                usql = "update business.summary_comments set  comments=%s, updatetime=%s where sid=%s and userid=%s"
                cur.execute(usql, [comments, updatetime, str(sid), userId])
                rowcount = cur.rowcount
                con.commit()
                data = {"rowcount": rowcount}
                rst.data = data
                return rst
            selectSql = "SELECT nextval('business.seq_comment') AS commentid"
            cur.execute(selectSql)
            commentid = str(cur.fetchall()[0][0])
            insertSql = "INSERT INTO  business.summary_comments(cid,sid,userid,comments,updatetime,flag)" \
                        " VALUES (%s,%s,%s,%s,%s,'1')"
            cur.execute(insertSql, [commentid, sid, userId,comments, updatetime])
            rowcount = cur.rowcount
            con.commit()
            data = {'commentid': commentid, "rowcount": rowcount}
            rst.data = data
            return rst
        except:
            msg = "Sorry, Create Comments failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="addCommentsError", errorMsg=msg)
            return rst
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass

    def delComments(self,cid,sid,userId):
        # 用户删除摘要点评
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            d_like  = "delete from  business.summary_comments  where  cid=%s and sid=%s and userid=%s"
            cur.execute(d_like,[str(cid),sid,str(userId)])
            rowcount=cur.rowcount
            con.commit()
            data={'rowcount':rowcount}
            rst.data=data
            return rst
        except:
            msg = "Sorry, delete data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="delLike",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getCommentsData(self,sid=None,userId=None,companyId=None,page=1 ,pageSize=10):
        # 查询点评数据  分页查询 每页默认10条评论,page从1开始
        rst = ResultData()
        rtdata = {"page": page, "pageSize": pageSize, "totalNum": 0, "data": [],"pageTotal":1}
        rst.data=rtdata
        page = int(page)
        pageSize = int(pageSize)
        try:
            con, cur = dbutils.getConnect()
            if sid!=None and userId!=None:
                # 查询当前用户的点评数据总条数
                totalcount = "select count(*) from  business.summary_comments  where sid=%s and userid=%s;"
                cur.execute(totalcount, [str(sid), str(userId)])
                totalcount = cur.fetchone()[0]
                rtdata["totalNum"] = totalcount
                # pageSize  = int(pageSize)
                # pageotal最少为1
                pagetatol_1 =(int) (totalcount/pageSize) if (totalcount % pageSize==0) else (int)(totalcount/pageSize+1)
                pagetatol_1 = max(1,pagetatol_1)
                if page>pagetatol_1: page =pagetatol_1
                if page<1: page=1
                rtdata["pageTotal"] = pagetatol_1
                rtdata["page"] = page
                start_offset = (page-1)*pageSize
                # 查询当前用户的点评数据 区分我的点评
                q_count = f"select comments,userid,updatetime,cid from  business.summary_comments  where sid='{str(sid)}' and userid='{str(userId)}' order by updatetime desc limit {pageSize} offset {start_offset};"
                cur.execute(q_count)
            elif sid!=None and companyId==None:
                # 查询所有的的 显示需要翻译 用户/公司
                totalcount = "select count(*) from  business.summary_comments  where sid=%s;"
                cur.execute(totalcount, [str(sid)])
                totalcount = cur.fetchone()[0]
                rtdata["totalNum"] = totalcount
                pagetatol_1 = (int)(totalcount / pageSize) if (totalcount % pageSize == 0) else (int)(
                    totalcount / pageSize + 1)
                pagetatol_1 = max(1, pagetatol_1)
                if page > pagetatol_1: page = pagetatol_1
                if page < 1: page = 1
                rtdata["pageTotal"] = pagetatol_1
                rtdata["page"] = page
                start_offset = (page - 1) * pageSize
                #查询所有的的 显示需要翻译 用户/公司  区分我的点评
                q_count = f"select comments,userid,updatetime,cid from  business.summary_comments  where sid='{str(sid)}' order by updatetime desc limit {pageSize} offset {start_offset};"
                cur.execute(q_count)
            elif sid!=None and companyId!=None:
                totalcount = "select count(*) from  business.summary_comments  where sid=%s and userid in (select userid from companyuser where companyid=%s) ;"
                cur.execute(totalcount,[str(sid),str(companyId)])
                totalcount = cur.fetchone()[0]
                rtdata["totalNum"] = totalcount
                pagetatol_1 = (int)(totalcount / pageSize) if (totalcount % pageSize == 0) else (int)(
                    totalcount / pageSize + 1)
                pagetatol_1 = max(1, pagetatol_1)
                if page > pagetatol_1: page = pagetatol_1
                if page < 1: page = 1
                rtdata["pageTotal"] = pagetatol_1
                rtdata["page"] = page
                start_offset = (page - 1) * pageSize
                #查询当前公司用户所有的点评 显示需要翻译 用户/公司 区分我的点评
                q_count = "select comments,userid,updatetime,cid from  business.summary_comments  where sid=%s and userid in (select userid from companyuser where companyid=%s) order by updatetime desc; "
                cur.execute(q_count,[str(sid),str(companyId)])
            else:
                return rst
            rts=[]
            rsdts = cur.fetchall()
            for rtd in rsdts:
                rts.append(rtd)
            rtdata["data"]=rts
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, get data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getLikeDataError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getCommentsNum(self,sid,userId=None,companyId=None):
        # 查询点评数量
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            if sid!=None and userId!=None:
                #查询当前用户的点评数据 区分我的点评
                q_count = "select count(1)  from  business.summary_comments  where sid=%s and userid=%s  "
                cur.execute(q_count, [str(sid),str(userId)])
            elif sid!=None and companyId==None:
                #查询所有的的 显示需要翻译 用户/公司  区分我的点评
                q_count = "select count(1) from  business.summary_comments  where sid=%s   "
                cur.execute(q_count, [str(sid)])
            elif sid!=None and companyId!=None:
                #查询当前公司用户所有的点评 显示需要翻译 用户/公司 区分我的点评
                q_count = "select count(1)  from  business.summary_comments  where sid=%s and userid in (select userid from companyuser where companyid=%s) "
                cur.execute(q_count,[str(sid),str(companyId)])
            else:
                rst.data={'records':0}
                return rst
            rowcount = cur.fetchall()[0][0]
            rst.data={'records':rowcount}
            return rst
        except:
            msg = "Sorry, get data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getCommentsNum",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass


if __name__ == '__main__':
    lm = likeMode()
    # rt=lm.addLike("jpmorgan_GPS-4552808-0",'1967','P')
    # print(rt.toDict())
    # rt=lm.addLike("jpmorgan_GPS-4555300-0",'1967','P')
    # print(rt.toDict())
    # # rt=lm.addLike("jpmorgan_GPS-4555300-0",'1967','N')
    # # print(rt.toDict())
    # rt=lm.addLike("jpmorgan_GPS-4555300-0",'1968','N')
    # print(rt.toDict())
    # rt=lm.delLike("jpmorgan_GPS-4555300-0",'1968')
    # print(rt.toDict())
    #
    # rt=lm.getLikeData("jpmorgan_GPS-4552808-0")
    # print(rt.toDict())
    # rt=lm.getLikeData("jpmorgan_GPS-4555300-0")
    # print(rt.toDict())
    #
    #

    #
    # rt=lm.addComments("jpmorgan_GPS-4552808-0",'1967','Pxxx')
    # print(rt.toDict())
    # rt=lm.addComments("jpmorgan_GPS-4555300-0",'1967','Pxxx')
    # print(rt.toDict())
    # # rt=lm.addLike("jpmorgan_GPS-4555300-0",'1967','N')
    # # print(rt.toDict())
    # for i in range(0,30):
    #     rt=lm.addComments("mq_b9380273-85d1-419f-8bc2-04656898bb7b",'19'+str(i),'test'+str(i))
    #     print(rt.toDict())
    # rt = lm.addComments("mq_b9380273-85d1-419f-8bc2-04656898bb7b", '1967', 'test001')
    # print(rt.toDict())
    # # x=rt.toDict()["data"]
    # # print("x:",x)
    # # for i in range(0, 1000):
    # #     rt=lm.delComments(1260+i,"mq_b9380273-85d1-419f-8bc2-04656898bb7b",'19'+str(i))
    # #     print(rt.toDict())
    # #
    # rt=lm.getCommentsData(sid="mq_b9380273-85d1-419f-8bc2-04656898bb7b",page=2)
    # print(rt.toDict())
    # rt=lm.getCommentsData(sid="jpmorgan_GPS-4555300-0",userId="1967",page=2)
    # print(rt.toDict())
    #
    # rt=lm.getCommentsData("jpmorgan_GPS-4555300-0",companyId=None)
    # print(rt.toDict())
    # rt=lm.getCommentsData("jpmorgan_GPS-4555300-0",companyId='temp_account')

    # rt=lm.addLike("jpmorgan_GPS-4555300-0X",'1967','IMP',markType='news') #['IMP', 'NP',"LP"]
    # print(rt.toDict())
    # rt=lm.addLike("sA_4042130",'1967','NP',markType='news') #['IMP', 'NP',"LP"]
    # print(rt.toDict())
    rt=lm.addLike("benzinga_36078920",'1967','LP',markType='news') #['IMP', 'NP',"LP"]
    print(rt.toDict())

    rt=lm.getMarkData("1967",["benzinga_36078920","sA_4042029"],markType="news")
    print(rt.toDict())




