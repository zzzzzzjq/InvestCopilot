
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
from pymongo import UpdateOne,UpdateMany,ReplaceOne

sys.path.append("../..")
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.portfolio import portfolioMode
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
Logger = logger_utils.LoggerUtils()
import numpy as np
import pandas as pd
import decimal

Logger = logger_utils.LoggerUtils()

_lock = threading.RLock()
class summaryEditMode():

    def __init__(self):
        pass

    def getEditSummarysStatus(self,sid):
        sql_Query="select * from copilot_edit_summarys_status where sid=%s  "
        sDF =dbutils.getPDQueryByParams(sql_Query,params=[sid])
        return sDF

    def getSummaryModifyLastVersionData(self,sid,languages='zh'):
        q_log="select * from copilot_edit_summarys_log where sid=%s and languages=%s order by edittime desc limit 1  "
        sDF =dbutils.getPDQueryByParams(q_log,params=[sid,languages])
        return sDF

    def getEditSummarysStatusForUpdate(self,sid,userid,companyid):
        ###获取当前报告是否可编辑
        rest=ResultData()
        try:
            _lock.acquire()
            ifmode = informationMode()
            rtnids=ifmode.getSummaryNum(sids=[sid])
            snum = rtnids[0]["num"]
            if snum==0:
                rest.errorData(errorMsg="报告不存在，请重新选择！")
                return rest
            cuserId=rtnids[0]['cuserId']
            #检查数据是否存在
            u_util = userMode.cuserMode()
            # 用户名与公司关系
            ucDF = u_util.getUserCompanyRF()
            usser_dt = {}
            for uc in ucDF.itertuples():
                suserid = str(uc.USERID)
                usser_dt[suserid] = {'userName': uc.USERREALNAME, "companyName": uc.SHORTCOMPANYNAME, "companyId": uc.COMPANYID}

            if cuserId in usser_dt:
                companyId = usser_dt[cuserId]['companyId']
                if companyId!=companyid:
                    rest.errorData(errorMsg="只允许修改同一公司下的报告！")
                    return rest

            sql_Query="select status,cuserid from copilot_edit_summarys_status where sid=%s for update  "
            con,cur = dbutils.getConnect()
            cur.execute(sql_Query,[sid])
            rtds = cur.fetchall()
            fetchnum = cur.rowcount
            userName="未知"
            if fetchnum>0: #存在记录
                #检查状态
                rtd= rtds[0]
                nstatus= str(rtd[0])
                nuserid= str(rtd[1])
                if nuserid==str(userid) and nstatus in ["1"]:
                    sql_Query = "update copilot_edit_summarys_status set status='1',edittime=current_timestamp where sid=%s"
                    cur.execute(sql_Query,[sid])
                    con.commit()
                    rest.errorMsg="状态可用[exists]"
                    return rest
                if nstatus in ["1"]:
                    if nuserid in usser_dt:
                        userName=usser_dt[nuserid]['userName']
                        rest.errorData(errorCode="USE", errorMsg="[%s]用户正在编辑当前报告，请稍后再试。"%(userName))
                    else:
                        if userid not in usser_dt:
                            rest.errorData(errorMsg="未查找到用户，请稍后再试。")
                    return rest
                else:
                    #可用
                    sql_Query = "update copilot_edit_summarys_status set status='1',userid=%s,edittime=current_timestamp  where sid=%s"
                    cur.execute(sql_Query,[userid,sid])
                    con.commit()
                    rest.errorMsg="状态可用[his]"
            else:
                #可用，新增一条
                sql_Query = "insert into copilot_edit_summarys_status (sid,cuserid,edittime,status) values(%s,%s,current_timestamp,%s)"
                cur.execute(sql_Query, [sid,userid,"1"])
                con.commit()
                rest.errorMsg = "状态可用[new]"
        except:
            Logger.errLineNo(msg="getEditSummarysStatusForUpdate error")
            rest.errorData(errorMsg="抱歉，获取报告编辑状态异常，请稍后重试")
        finally:
            try:
                cur.close()
            except:pass
            try:
                con.close()
            except:pass
            try:
                _lock.release()
            except:pass
        return rest

    def releaseSummarysStatusForUpdate(self,sid,userid):
        ###是否当前报告编辑状态
        rest=ResultData()
        try:
            _lock.acquire()
            # ifmode = informationMode()
            # rtnids=ifmode.getSummaryNum(sids=[sid])
            # snum = rtnids[0]["num"]
            # if snum==0:
            #     rest.errorData(errorMsg="报告不存在，请重新选择！")
            #     return rest
            con, cur = dbutils.getConnect()
            sql_Query = "delete from copilot_edit_summarys_status where sid=%s and cuserid=%s"
            cur.execute(sql_Query, [sid, userid])
            con.commit()
        except:
            Logger.errLineNo(msg="getEditSummarysStatusForUpdate error")
            rest.errorData(errorMsg="抱歉，重置报告编辑状态异常，请稍后重试")
        finally:
            try:
                cur.close()
            except:pass
            try:
                con.close()
            except:pass
            try:
                _lock.release()
            except:pass
        return rest

    def editSummaryDataSave(self,sid,userid,editContent,languages):
        ###编辑后数据保存 需先调用getEditSummarysStatusForUpdate方法才可用
        rest = ResultData()
        try:
            _lock.acquire()
            ifmode = informationMode()
            rtnids = ifmode.getSummaryNum(sids=[sid])
            snum = rtnids[0]["num"]
            if snum == 0:
                rest.errorData(errorCode="002",errorMsg="报告不存在，请重新选择！")
                return rest
            dsource=rtnids[0]['source']
            dataType=rtnids[0]['dataType']
            nt=datetime.datetime.now()
            scontent = tools_utils.strEncode(editContent)  # 核心观点
            #id
            logid = tools_utils.md5(",".join([sid,str(userid),str(nt)]))
            # 检查数据是否存在
            u_util = userMode.cuserMode()
            # 用户名与公司关系
            ucDF = u_util.getUserCompanyRF()
            usser_dt = {}
            for uc in ucDF.itertuples():
                suserid = str(uc.USERID)
                usser_dt[suserid] = {'userName': uc.USERREALNAME, "companyName": uc.SHORTCOMPANYNAME}
            sql_Query = "select status,cuserid from copilot_edit_summarys_status where sid=%s for update  "
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
                    #sava data log
                    i_dta="INSERT INTO dsidmfactors.copilot_edit_summarys_log (rid,sid, cuserid, ccontent,languages,edittime,dtype,dsource) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                    cur.execute(i_dta,[logid,sid,userid,scontent,languages,nt,dataType,dsource])
                    #mongodb save
                    sql_Query = "delete from copilot_edit_summarys_status where sid=%s and cuserid=%s"
                    cur.execute(sql_Query, [sid,userid])
                    con.commit()
                    return rest
                else:
                    #报告状态未知
                    rest.errorData(errorMsg="抱歉，未获取到报告状态，请重新处理！")
            else:
                rest.errorData(errorMsg="抱歉，报告状态未知，请重新处理！")
        except:
            Logger.errLineNo(msg="editSummaryDataSave error")
            rest.errorData(errorMsg="抱歉，保存报告异常，请稍后重试")
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
            try:
                _lock.release()
            except:
                pass
        return rest


if __name__ == '__main__':
    sem = summaryEditMode()
    rts = sem.getEditSummarysStatusForUpdate("Audio_d6e9d4ad24b1da24b7e5febb072eb4b1","0562")
    print(rts.toDict())
    s = sem.getEditSummarysStatus("Audio_d6e9d4ad24b1da24b7e5febb072eb4b1")
    print(s)
    s = sem.editSummaryDataSave("Audio_d6e9d4ad24b1da24b7e5febb072eb4b1",'1933','sssssssssssssssssssssdfd')
    print(s.toDict())