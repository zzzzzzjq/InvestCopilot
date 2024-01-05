import re
import math
import datetime
import traceback
import InvestCopilot_App.models.toolsutils.dbutils as dbutils
import pandas as pd
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.comm import mongdbConfig as mg_cfg
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
import time
from InvestCopilot_App.models.toolsutils import dbmongo

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

class conferenceMode():
    def getCallDataById(self, cid,userId=None):
        if userId is not None:
            sql_Query = "select * from business.callmanger where cid=%s and cuserid=%s "
            callDF = dbutils.getPDQueryByParams(sql_Query, params=[cid,userId])
        else:
            sql_Query = "select * from business.callmanger where cid=%s "
            callDF = dbutils.getPDQueryByParams(sql_Query, params=[cid])
        return callDF

    def getCountUploadMeetingNum(self,userId,cdate):
        #cdate YYYY-MM-DD 当天上传会议数量检查
        upcount=-1
        try:
            q_one="select count(1) from business.callmanger c  where  cuserid  in(select userid from companyuser c2 where companyid  =(select companyid  from companyuser c  where userid =%s)) and  csource ='upload'" \
                  "   and cdate =%s "#and audiotextpath is not null
            con,cur = dbutils.getConnect()
            cur.execute(q_one,[userId,cdate])
            upcount=cur.fetchall()[0][0]
            cur.close()
            con.close()
        except:
            Logger.error(traceback.format_exc())
        return upcount

    def searchcompanyIdByUserid(self,userid):
        companyname = 'None'
        try:
            q_one = f"""select companyid from companyuser where userid='{userid}';"""
            con, cur = dbutils.getConnect()
            cur.execute(q_one)
            upcount = cur.fetchone()
            if(upcount == None ):
                companyname= 'None'
            else:
                companyname=upcount[0]
            cur.close()
            con.close()
        except:
            Logger.error(traceback.format_exc())
        return companyname

    def addConferenceInfo(self, ctitle, cdate, ctime, csource, cuserid, AudioID=None, companyId=None,
                          cregiest=False, creplay=False,csound=False, cremark=None):

        # audioType=None, language=None, calllink=None, callname=None, callpwd=None
        # 本功能添加会议信息到business.callmanger，如果上面显示录音，再调用其他接口
        # 会议主题，会议日期(yyyy-MM-dd)，会议时间，会议来源（四个必须输入项，ctitle, cdate, ctime, csource）
        # 用户id，公司id，用于用户只能查看自己公司的会议信息（cuserid, companyId）
        rst = ResultData()
        rtdata = {'msg': "" , 'cid':""}
        try:
            updatetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            i_data = f"""INSERT INTO business.callmanger 
                    (cid, ctitle, cdate, ctime, csource, cuserid, cregiest, creplay, csound, cremark, updatetime, companyId)
                    VALUES(%s, %s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s)"""
            if companyId is None:
                companyId = self.searchcompanyIdByUserid(userid=cuserid)
                if companyId == 'None':
                    rtdata['msg'] = "未查询到该用户所属公司."
                    rst.data = rtdata
                    rst.errorData(errorMsg="未查询到该用户所属公司")
                    return rst
            sval = [ctitle, cdate, csource, companyId]
            # cid处理
            if AudioID is None:
                cid = 'Audio_' + tools_utils.md5("".join(sval))
            else:
                cid = AudioID
            # 检查是否已经存在该条会议信息
            q_one = "select count(1) from business.callmanger where cid=%s and cuserid=%s"
            con, cur = dbutils.getConnect()
            cur.execute(q_one, [cid, cuserid])
            fnum = cur.fetchall()[0][0]
            if fnum > 0:
                errormsg = "[%s] 会议+时间+来源已经存在，请重新处理" % ctitle
                rtdata['msg'] = errormsg
                rst.data = rtdata
                rst.errorData(errorMsg=errormsg)
                return rst
            # 构造数据
            cur.execute(i_data,[cid, ctitle, cdate, ctime, csource, cuserid, cregiest, creplay, csound, cremark, updatetime, companyId])
            con.commit()
            rtdata['msg'] = "插入会议数据成功."
            rtdata['cid'] = cid
            rst.data = rtdata
        except:
            errormsg = "保存，数据保存失败，请稍后重试！"
            rtdata['msg'] = errormsg
            rst.data = rtdata
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst

    def editConferenceInfoCompany(self, ctitle, cdate, ctime, csource, cuserid, cid,
                          cregiest=False, creplay=False,csound=False, cremark=None):
        # audioType=None, language=None, calllink=None, callname=None, callpwd=None,AudioID=None, companyId=None
        # 本功能修改会议信息到business.callmanger，如果上面显示录音，再调用其他接口
        # 因为ctitle, cdate, csource可能被修改，所以需要传入cid
        # 会议主题，会议日期(yyyy-MM-dd)，会议时间，会议来源（四个必须输入项，ctitle, cdate, ctime, csource）
        # 用户id，公司id，用于用户只能更新自己公司的会议信息（cuserid, companyId）
        rst = ResultData()
        rtdata = {'msg': "" , 'cid':""}
        try:
            companyId = self.searchcompanyIdByUserid(userid=cuserid)
            if companyId == 'None':
                rst.data = rtdata
                rst.errorData(errorMsg="未查询到该用户所属公司")
                rtdata['msg'] = "未查询到该用户所属公司."
                return rst

            # 目前同一公司的才可以修改,不一定是该用户名下的
            # q_one = "select count(1) from business.callmanger where cid=%s and cuserid=%s and companyid=%s;"
            q_one = "select count(1) from business.callmanger where cid=%s and companyid=%s;"
            con, cur = dbutils.getConnect()
            cur.execute(q_one, [cid, companyId])
            fnum = cur.fetchall()[0][0]
            if fnum < 1:
                errormsg = "您所在的公司无权操作该会议信息" % ctitle
                rst.data = rtdata
                rst.errorData(errorMsg=errormsg)
                rtdata['msg'] = errormsg
                return rst
            updatetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_sql = f"""UPDATE business.callmanger SET 
            ctitle = %s, cdate= %s, ctime= %s, csource= %s, cregiest = %s, creplay = %s , 
            csound = %s , cremark = %s , updatetime = %s where cid=%s and companyid=%s; """
            cur.execute(update_sql, [ctitle, cdate, ctime, csource, cregiest, creplay, csound, cremark, updatetime, cid, companyId])
            con.commit()
            rtdata['msg'] = "修改会议数据成功."
            rtdata['cid'] = cid
            rst.data = rtdata
        except:
            errormsg = "更新数据失败，请稍后重试！"
            rtdata['msg'] = errormsg
            rst.data = rtdata
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst

    def editConferenceInfo(self, ctitle, cdate, ctime, csource, cuserid, cid,
                          cregiest=False, creplay=False,csound=False, cremark=None):
        # 1. audioType=None, language=None, calllink=None, callname=None, callpwd=None,AudioID=None, companyId=None
        # 2. 本功能修改会议信息到business.callmanger，如果上面显示录音，再调用其他接口
        # 3. 因为ctitle, cdate, csource可能被修改，所以需要传入cid
        # 4. 会议主题，会议日期(yyyy-MM-dd)，会议时间，会议来源（四个必须输入项，ctitle, cdate, ctime, csource）
        # 5. 用户id，公司id，用于用户只能更新自己公司的会议信息（cuserid, companyId）
        # 6. 更改1220    用户只能更新自己的会议信息
        rst = ResultData()
        rtdata = {'msg': "" , 'cid':""}
        try:
            # companyId = self.searchcompanyIdByUserid(userid=cuserid)
            # if companyId == 'None':
            #     rtdata['msg'] = "未查询到该用户所属公司."
            #     rst.data = rtdata
            #     rst.errorData(errorMsg="未查询到该用户所属公司")
            #     return rst

            # 目前同一公司的才可以修改,不一定是该用户名下的
            # q_one = "select count(1) from business.callmanger where cid=%s and cuserid=%s and companyid=%s;"
            q_one = "select count(1) from business.callmanger where cid =%s and cuserid =%s;"
            con, cur = dbutils.getConnect()
            cur.execute(q_one, [cid, cuserid])
            fnum = cur.fetchall()[0][0]
            if fnum < 1:
                errormsg = f"您无权操作该会议信息{ctitle}"
                rtdata['msg'] = errormsg
                rst.errorData(errorMsg=errormsg)
                rst.data = rtdata
                return rst
            updatetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_sql = f"""UPDATE business.callmanger SET 
            ctitle = %s, cdate= %s, ctime= %s, csource= %s, cregiest = %s, creplay = %s , 
            csound = %s , cremark = %s , updatetime = %s where cid=%s and cuserid=%s; """
            cur.execute(update_sql, [ctitle, cdate, ctime, csource, cregiest, creplay, csound, cremark, updatetime, cid, cuserid])
            con.commit()
            rtdata['msg'] = "修改会议数据成功."
            rtdata['cid'] = cid
            rst.data = rtdata
        except:
            errormsg = "更新数据失败，请稍后重试！"
            rtdata['msg'] = errormsg
            rst.data = rtdata
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst
    def editConferencecsound(self, cuserid, cid, csound=False):
        rst = ResultData()
        rtdata = {'msg': "" , 'cid':"", 'rownum':-1}
        try:
            companyId = self.searchcompanyIdByUserid(userid=cuserid)
            if companyId == 'None':
                rtdata['msg'] = "未查询到该用户所属公司."
                rst.data = rtdata
                rst.errorData(errorMsg="未查询到该用户所属公司")
                return rst

            # 目前同一公司的才可以修改,不一定是该用户名下的
            q_one = "select count(1) from business.callmanger where cid=%s and companyid=%s;"
            con, cur = dbutils.getConnect()
            cur.execute(q_one, [cid, companyId])
            fnum = cur.fetchall()[0][0]
            print(fnum)
            if fnum < 1:
                errormsg = f"您无权操作该会议录音信息"
                rtdata['msg'] = errormsg
                rst.errorData(errorMsg=errormsg)
                rst.data = rtdata
                return rst
            updatetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_sql = f"""UPDATE business.callmanger SET 
            csound = %s , updatetime = %s where cid=%s and companyid=%s; """
            cur.execute(update_sql, [csound, updatetime, cid, companyId])
            rtdata['rownum'] = cur.rowcount
            con.commit()
            rtdata['msg'] = "修改会议录音成功."
            rtdata['cid'] = cid
            rst.data = rtdata
        except:
            errormsg = "更新数据失败，请稍后重试！"
            rtdata['msg'] = errormsg
            rst.data = rtdata
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst

    def delConferenceInfo(self, cuserid, cid):
        rst = ResultData()
        rtdata = {'msg': ""}
        try:
            # companyId = self.searchcompanyIdByUserid(userid=cuserid)
            # if companyId == 'None':
            #     rst.errorData(errorMsg="未查询到该用户所属公司")
            #     return rst
            d_data = "delete from business.callmanger where cid=%s and cuserid=%s "
            con, cur = dbutils.getConnect()
            cur.execute(d_data, [cid, cuserid])
            rowcount = cur.rowcount
            con.commit()
            rst.data = {"rowcount": rowcount, "msg": "删除会议数据成功"}
        except:
            errormsg = "删除会议信息数据失败，请稍后重试！"
            rtdata['msg'] = errormsg
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
            rst.data = rtdata
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        return rst

    def getConferenceInfoOnlyOnedata(self, userid, cid):
        rst = ResultData()
        # 用于用户修改信息时，显示详情信息
        # 本公司的人可以查看自己公司上传会议的详情信息
        rtdata = {"data": [],"msg": ""}
        try:
            companyId = self.searchcompanyIdByUserid(userid= userid)
            if companyId == 'None':
                rtdata['msg'] = "未查询到该用户所属公司."
                rst.data = rtdata
                rst.errorData(errorMsg="未查询到该用户所属公司")
                return rst
            d_data = f"""select ctitle, cdate, ctime, csource, cregiest, creplay, csound, cremark,companyid 
                    from business.callmanger where companyid= %s and cid= %s; """
            con, cur = dbutils.getConnect()
            dfdata = pd.read_sql(d_data, con, params=[companyId, cid])
            if (len(dfdata)<1):
                rtdata['msg'] = "该cid无效,或者该会议信息不属于该用户所属公司."
                rst.data = rtdata
                rst.errorData(errorMsg="该cid无效,或者该会议信息不属于该用户所属公司")
                return rst
            dfdata = dfdata.rename(columns=lambda x: str(x).lower())
            data_list = []
            columns = dfdata.columns.tolist()
            for row in dfdata.itertuples():
                row_dict = dict()
                for column in columns:
                    row_dict[column] = str(getattr(row, column, '-'))
                data_list.append(row_dict)
            rtdata = {"msg": "获取会议数据成功", "data": data_list}
            rst.data = rtdata
        except:
            errormsg = "获取本会议信息数据失败，请稍后重试！"
            rtdata['msg'] = errormsg
            rst.data = rtdata
            Logger.errLineNo(msg=errormsg)
            rst.errorData(errorMsg=errormsg)
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass
        return rst

    def getConferenceInfowithTime(self,userid, cdate='', page=1, pageSize=10):
        con, cur = dbutils.getConnect()
        rst = ResultData()
        userid = str(userid)
        page = int(page)
        pageSize = int(pageSize)
        rtdata = {"page": page, "pageTotal": 0, "pageSize": pageSize, "totalNum": 0, "columns_count": 0,
                  "columns_name": [], "data": [],"msg":""}
        try:
            companyId = self.searchcompanyIdByUserid(userid=userid)
            if companyId == 'None':
                rtdata['msg'] = "未查询到该用户所属公司."
                rst.data = rtdata
                rst.errorData(errorMsg="未查询到该用户所属公司")
                return rst
            if cdate != "" and cdate != None:
                cdate = str(cdate)
                try:
                    cdate = time.strptime(cdate, '%Y-%m-%d')
                    cdate = time.strftime("%Y-%m-%d", cdate)
                except:
                    msg = "Sorry, cdate format error, please re-enter."
                    Logger.errLineNo(msg=msg)
                    rtdata['msg']="cdate格式错误，请重新输入"
                    rst.data = rtdata
                    rst.errorData(translateCode="getConferenceInfowithTimeError", errorMsg=msg)
                    return rst
            else:
                cdate = datetime.datetime.now() - datetime.timedelta(days=5 * 365)
                cdate = cdate.strftime('%Y-%m-%d')
            # select count(*) from business.callmanger c where companyid ='{companyId}' and cdate >='{cdate}'
            totalcount = f"""select count(*) from business.callmanger c where cuserid in
                (select userid from companyuser c2 where companyid= '{companyId}') and cdate >='{cdate}' and csource not in ('upload','trans');"""
            cur.execute(totalcount)
            totalcount = cur.fetchone()[0]
            rtdata["totalNum"] = totalcount
            pagetatol_1 = (int)(totalcount / pageSize) if (totalcount % pageSize == 0) else (int)(totalcount / pageSize + 1)
            pagetatol_1 = max(1, pagetatol_1)
            if page > pagetatol_1: page = pagetatol_1
            if page < 1: page = 1
            rtdata["pageTotal"] = pagetatol_1
            rtdata["page"] = page
            start_offset = (page - 1) * pageSize

            sql_get = f"""select cid,ctitle, cdate, ctime, csource, cregiest, creplay, csound, cremark,companyid,cuserid
                    from business.callmanger where companyid='{companyId}' and cdate>='{cdate}' and csource not in ('upload','trans') order by cdate limit {pageSize} offset {start_offset};"""
            dfdata = pd.read_sql(sql_get, con)
            dfdata = dfdata.rename(columns=lambda x: str(x).lower())
            # # 返回用户名，做删除修改操作
            user_ids_list = dfdata['cuserid'].to_list()
            if(len(user_ids_list)>=1):
                df = pd.DataFrame()
                # 设置'userid'列，并根据条件设置对应的列为True或False
                df['userid'] = list(set(user_ids_list))
                df['editFlag'] = df['userid'].apply(lambda x: True if x == userid else False)
                dfdata = dfdata.merge(df, left_on = 'cuserid', right_on='userid', how='left')
                dfdata = dfdata.drop(["userid","cuserid"],axis=1)
            # dfdata['csource'] = dfdata['csource'].apply(lambda x: '' if x == 'upload' else x)

                # print(len(dfdata))
            # if(len(user_ids_list)>= 1):
            #     if(len(user_ids_list)==1):
            #         sql_userid = f"select userid,username,usernickname from tusers where userid = '{user_ids_list[0]}';"
            #     else:
            #         sql_userid = f"select userid,username,usernickname from tusers where userid in {tuple(user_ids_list)};"
            #     df_usernames = pd.read_sql(sql_userid, con)
            #     dfdata = dfdata.merge(df_usernames, left_on = 'cuserid', right_on='userid', how='left')
            #     dfdata = dfdata.drop(["userid","cuserid"],axis=1)
            rtdata["columns_count"] = len(dfdata)
            rtdata["columns_name"] = dfdata.columns.tolist()
            rtdata["data"] = dfdata.values.tolist()
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rtdata['msg'] = msg
            rst.data = rtdata
            rst.errorData(translateCode="getConferenceInfowithTimeError", errorMsg=msg)
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

if __name__ == '__main__':
    cu=conferenceMode()
    cname = cu.searchcompanyIdByUserid('73')
    print(cname)
    print(type(cname))
    ctitle = "1213插入测试_12"
    cdate = "2023-12-13"
    ctime = "14:35:01"
    csource = "upload?"
    userid = '73'
    cdate_search = "2023-12-08"
    test_choice=3
    if test_choice==1:
        rs_1 =  cu.addConferenceInfo(ctitle,cdate,ctime,csource,userid,creplay=True)
        print(rs_1.toDict())
        print(rs_1.toDict()['data'].keys())
        if rs_1.errorFlag:
            print(rs_1.toDict()['data']['cid'])
            cid = rs_1.toDict()['data']['cid']
            rs_1 = cu.getConferenceInfoOnlyOnedata(cid=cid,userid=userid)
            print("getConferenceInfoOnlyOnedata")
            print(rs_1.toDict())
            rs_3 = cu.getConferenceInfowithTime(userid=userid,cdate=cdate)
            print("getConferenceInfowithTime")
            print(rs_3.toDict())
            rs_4 = cu.editConferenceInfo(ctitle='edit测试',cdate="2023-11-08",ctime='14:04',csource="upload",cregiest=True,cid=cid,cuserid=userid)
            print(rs_4.toDict())
            rs_5 = cu.getConferenceInfoOnlyOnedata(cid=cid, userid=userid)
            print("getConferenceInfoOnlyOnedata")
            print(rs_5.toDict())
        delete_cid = "Audio_00eabdb53da9e0c7a8a225398147cf61"
        rs_2 = cu.delConferenceInfo('73',delete_cid)  # 1212插入测试_4
        print(rs_2.toDict())
    elif test_choice==2:
        pass
        # rs_3 = cu.getConferenceInfowithTime(userid="1967", cdate="2023-11-01",page=2,pageSize=20)
        # print("getConferenceInfowithTime")
        # print(rs_3.toDict())
        # rs_3 = cu.getConferenceInfowithTime(userid=userid, cdate="2023-12-08", page=10, pageSize=10)
        # print("getConferenceInfowithTime")
        # print(rs_3.toDict())
        # rs_3 = cu.getConferenceInfowithTime(userid=userid, cdate="2023-12-09", page=1, pageSize=20)
        # print("getConferenceInfowithTime")
        # print(rs_3.toDict())
        # rs_3 = cu.getConferenceInfowithTime(userid=userid, cdate="2023-12-20", page=2, pageSize=20)
        # print("getConferenceInfowithTime")
        # print(rs_3.toDict())
        # rs_4 = cu.delConferenceInfo(cuserid=userid, cid="Audio_a2777c8835cfa065111536c887188837") #删除自己的
        # print(rs_4.toDict())
        # rs_5 = cu.delConferenceInfo(cuserid=userid, cid="Audio_28414a4bb04015b8bc983f72a4d4fgewfgewgwgea92d") #删除不存在的
        # print(rs_5.toDict())
        # rs_6 = cu.delConferenceInfo(cuserid=userid, cid="Audio_55b7f74f46324be9cc54ea0a36eea04c")  # 删除别人的,无法删除
        # print(rs_6.toDict())
        # rs_7 = cu.editConferenceInfo(ctitle='edit测试',cdate="2023-12-20",ctime='14:04',csource="upload",cregiest=True,cid="Audio_a919f2906c0332b95f6a7ad017819cca",cuserid=userid)
        # print(rs_7.toDict())
        # rs_7 = cu.editConferenceInfo(ctitle='edit测试2', cdate="2023-12-20", ctime='15:04', csource="upload",
        #                              cregiest=True, cid="Audio_d2408d90d9a7fc50e9c92742aeed7a47", cuserid=userid)
        # print(rs_7.toDict())
        # rs_7 = cu.editConferenceInfo(ctitle='1202测试环境测试电话会议1edit', cdate="2023-12-08", ctime='13:42:47', csource="upload",
        #                              cregiest=False, cid="Audio_55b7f74f46324be9cc54ea0a36eea04c", cuserid=userid)
        # print(rs_7.toDict())
    else:
        rs_1 = cu.editConferencecsound("73","Audio_c10cb2ff2aa3528a367211149bcce27a",False)
        print(rs_1.toDict())
        rs_2= cu.editConferencecsound("73","Audio_d2610401701207852f26c287cb624d47",True)
        print(rs_2.toDict())
        rs_3 = cu.editConferencecsound("73","Audio_1bd3f798582f31daaa434664252a6d4d",False)
        print(rs_3.toDict())



