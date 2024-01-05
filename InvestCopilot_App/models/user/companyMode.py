#公司信息维护
import datetime

from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()
class companyMode():
    def addCompany(self,companyCode,companyName,shortName):
        #添加公司信息
        rst=ResultData()
        try:
            con, cur = dbutils.getConnect()
            #check
            q_company1="select count(1) from company where companyid=%s"
            cur.execute(q_company1, [companyCode])
            chkum1=cur.fetchone()[0]
            if chkum1>0:
                msg="公司编号[%s]已经存在，请重新选择！"%(companyCode)
                rst.errorData(translateCode="setCompanyUser_ckCode",errorMsg=msg)
                return rst
            #check
            q_company2="select count(1) from company where companyname=%s"
            cur.execute(q_company2, [companyName])
            chkum2=cur.fetchone()[0]
            if chkum2>0:
                msg="公司名字[%s]已经存在，请重新选择！"%(companyName)
                rst.errorData(translateCode="setCompanyUser_ckName",errorMsg=msg)
                return rst
            #check
            q_company3="select count(1) from company where shortcompanyname=%s"
            cur.execute(q_company3, [shortName])
            chkum3=cur.fetchone()[0]
            if chkum3>0:
                msg="公司简称[%s]已经存在，请重新选择！"%(shortName)
                rst.errorData(translateCode="setCompanyUser_ckShortName",errorMsg=msg)
                return rst
            insertSql = 'INSERT INTO company(companyid,companyname,shortcompanyname,updatetime)VALUES(%s,%s,%s,%s)'
            cur.execute(insertSql, [companyCode, companyName,shortName,datetime.datetime.now()])
            rowcount=cur.rowcount
            con.commit()
            data={'rowcount':rowcount}
            rst.data=data
        except:
            msg = "抱歉，创建公司信息异常。"
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="setCompanyUserError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst

    def editCompany(self, companyCode, companyName, shortName):
        # 修改逻辑是companycode不变
        rst = ResultData()
        rtdata = {'msg': ""}
        companyCode = str(companyCode)
        companyName = str(companyName)
        shortName = str(shortName)
        try:
            con, cur = dbutils.getConnect()
            # check
            q_company1 = "select count(1) from company where companyid=%s"
            cur.execute(q_company1, [companyCode])
            chkum1 = cur.fetchone()[0]
            if chkum1 <= 0:
                msg = "公司编号[%s]不存在，请重新选择！" % (companyCode)
                rst.errorData(translateCode="editCompanyError", errorMsg=msg)
                rtdata['msg'] = msg
                rst.data = rtdata
                return rst
            # check
            q_company2 = "select count(1) from company where companyname=%s and companyid != %s"
            cur.execute(q_company2, [companyName,companyCode])
            chkum2 = cur.fetchone()[0]
            if chkum2 > 0:
                msg = "公司名字[%s]已经存在，请重新选择！" % (companyName)
                rst.errorData(translateCode="editCompanyError", errorMsg=msg)
                rtdata['msg'] = msg
                rst.data = rtdata
                return rst
            # check
            q_company3 = "select count(1) from company where shortcompanyname=%s and companyid != %s"
            cur.execute(q_company3, [shortName, companyCode])
            chkum3 = cur.fetchone()[0]
            if chkum3 > 0:
                msg = "公司简称[%s]已经存在，请重新选择！" % (shortName)
                rst.errorData(translateCode="editCompanyError", errorMsg=msg)
                rtdata['msg'] = msg
                rst.data = rtdata
                return rst
            editSql = ('update company set companyname = %s, shortcompanyname = %s,updatetime = %s where companyid=%s;')
            cur.execute(editSql, [companyName, shortName, datetime.datetime.now(),companyCode])
            rowcount = cur.rowcount
            con.commit()
            data = {'rowcount': rowcount,"msg": "修改公司信息成功"}
            rst.data = data
        except:
            msg = "抱歉，修改公司信息异常。"
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="editCompanyError", errorMsg=msg)
            rtdata['msg'] = msg
            rst.data = rtdata
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
        return rst

    def delCompany(self, companyCode):
        # 删除公司信息
        rst = ResultData()
        rtdata = {'msg': ""}
        companyCode = str(companyCode)
        try:
            con, cur = dbutils.getConnect()
            # check
            q_company1 = "select count(1) from company where companyid=%s"
            cur.execute(q_company1, [companyCode])
            chkum1 = cur.fetchone()[0]
            if chkum1 <= 0:
                msg = "公司编号[%s]不存在，请重新选择！" % (companyCode)
                rst.errorData(translateCode="delCompanyError", errorMsg=msg)
                rtdata['msg'] = msg
                rst.data = rtdata
                return rst

            delSql = ('delete from company where companyid= %s;')
            cur.execute(delSql, [companyCode])
            rowcount = cur.rowcount
            con.commit()
            data = {'rowcount': rowcount,"msg": "删除公司信息成功"}
            rst.data = data
        except:
            msg = "抱歉，删除公司信息异常。"
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="delCompanyError", errorMsg=msg)
            rtdata['msg'] = msg
            rst.data = rtdata
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
        return rst


    def getCompanyByCode(self,companyCode):
        #获取公司信息
        return dbutils.getPDQueryByParams("select * from company where companyid=%s",params=[companyCode])

    def getUsersCompanyByUserId(self,userId):
        #获取用户公司信息
        return dbutils.getPDQueryByParams("select c.* from company c,companyuser u where c.companyid=u.companyid and u.userid=%s",params=[userId])

    def getCompanyData(self):
        #获取全部公司信息
        return dbutils.getPDQuery("select * from company")

    def searchCompany(self,Name):
        Name = str(Name)
        return dbutils.getPDQuery(f"select * from company where companyname like '%{Name}%' or shortcompanyname like '%{Name}%';")
        # con, cur = dbutils.getConnect()
        # # check
        # q_company1 = f"select * from company where companyname like '%{Name}%' or shortcompanyname like '%{Name}%';"
        # dfdata = pd.read_sql(q_company1, con)
        # dfdata = dfdata.rename(columns=lambda x: str(x).lower())
        # data_list = []
        # columns = dfdata.columns.tolist()
        # for row in dfdata.itertuples():
        #     row_dict = dict()
        #     for column in columns:
        #         row_dict[column] = str(getattr(row, column, '-'))
        #     data_list.append(row_dict)
        # rtdata = {"msg": "获取搜索公司数据成功", "data": data_list}
        # rst.data = rtdata

    def setCompanyUser(self,userId, companyId):
        #设置用户所属公司
        rst=ResultData()
        try:
            con, cur = dbutils.getConnect()
            #简称公司和用户是否存在
            q_company="select count(1) from company where companyId=%s"
            cur.execute(q_company, [companyId])
            chkum=cur.fetchone()[0]
            if chkum==0:
                msg="公司代码[%s]不存在"%(companyId)
                rst.errorData(translateCode="setCompanyUser_ckCompanyId",errorMsg=msg)
                return rst
            q_company="select count(1) from tusers where userid=%s"
            cur.execute(q_company, [str(userId)])
            chkum=cur.fetchone()[0]
            if chkum==0:
                msg="用户编号[%s]不存在"%(userId)
                rst.errorData(translateCode="setCompanyUser_ckUserId",errorMsg=msg)
                return rst
            insertSql = 'INSERT INTO companyuser(companyid,userid)VALUES(%s,%s)'
            cur.execute(insertSql, [companyId, str(userId)])
            rowcount=cur.rowcount
            con.commit()
            data={'rowcount':rowcount}
            rst.data=data
        except:
            msg = "抱歉，关联用户所属公司异常。"
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="setCompanyUser",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
        return rst

if __name__ == '__main__':
    cm = companyMode()
    # rt=cm.addCompany("gfundsx","广发基金管理有限公司x",'广发基金x')
    # rt=cm.setCompanyUser("2","gfundsx")
    # userInfoPdData=cm.getCompanyData()
    # userInfoPdData = cm.addCompany('test_15:35','testjsr','testjsr')
    # userInfoPdData = cm.editCompany('test_15:35','test_jsr','中信信期')
    # userInfoPdData = cm.delCompany('test_15:35')
    # userInfoPdData = cm.searchCompany('中')
    userInfoPdData = cm.getCompanyByCode('gs_eciticzy')
    userInfoPdData = userInfoPdData.rename(columns=lambda x: x.capitalize()).to_dict(orient='records')# 效率高
    print("userInfoPdData:", userInfoPdData)
    # print("userInfoPdData:", userInfoPdData)

    # rt=cm.getCompanyByCode("gfunds")
    # rt=cm.getUsersCompanyByUserId("3")
    # print("rt:",rt)
    # print(rt.toDict())