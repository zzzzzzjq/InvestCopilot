from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import pandas as pd
Logger = logger_utils.LoggerUtils()


def getDictKeyList():
    """
    获取所有字典key
    """
    selectSql = "SELECT KEYNO,KEYDESC FROM sysdictionary T WHERE KEYVALUE='#' ORDER BY t.keyno ASC"
    data=dbutils.getPDQuery(selectSql)
    return data


def getDictValueList(keyNo):
    """
    获取字典,values
    """
    selectSql = "SELECT * FROM sysdictionary  where  keyno=%s and keyvalue!='#'  ORDER BY keyvalue asc, keyorder ASC "
    data=dbutils.getPDQueryByParams(selectSql,params=[keyNo])
    return data


def delDictByKey(keyNo):
    """
    刪除字典,values
    """
    rs = ResultData()
    con, cur = dbutils.getConnect()
    try:
        d_dict = "delete FROM sysdictionary  where  keyno=%s "
        cur.execute(d_dict, [str(keyNo)])
        con.commit()
    except:
        Logger.errLineNo()
        rs.errorData(errorMsg="抱歉，刪除字典异常，请稍后重试")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    return rs


def delDictValueByKey(keyNo, keyValue):
    """
    刪除字典值,values
    """
    rs = ResultData()
    con, cur = dbutils.getConnect()
    try:
        d_dict = "delete FROM sysdictionary  where  keyno=%s and keyvalue=%s"
        cur.execute(d_dict, [str(keyNo), str(keyValue)])
        con.commit()
    except:
        Logger.errLineNo()
        rs.errorData(errorMsg="抱歉，刪除字典值异常，请稍后重试")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    return rs


def addDictKey(keyName):
    """
    新增字典名称
    """
    rs = ResultData()
    con, cur = dbutils.getConnect()
    try:
        selectSql = '''
        SELECT NEXTVAL('seq_dictid') AS SEQNO
        '''
        cur.execute(selectSql)
        keyNo = cur.fetchall()[0][0]

        # 检查keyno是否重复
        q_dict = "select count(1) as KEYNO from sysdictionary where keyno =%s"
        cur.execute(q_dict, [str(keyNo)])
        while 1 == cur.fetchall()[0][0]:  # 存在相同的序列继续生成
            cur.execute(selectSql)
            keyNo = cur.fetchall()[0][0]
            cur.execute(q_dict, [str(keyNo)])
        # print(str(keyNo), '#', keyName, '1', '0')
        i_dict = "insert into sysdictionary (keyno,keyvalue,keydesc,keyenbale,keyorder) values (%s,%s,%s,%s,%s)"
        cur.execute(i_dict, [str(keyNo), '#', keyName, '1', '1'])
        con.commit()
        rs.keyNo = str(keyNo)
    except:
        Logger.errLineNo()
        rs.errorData(errorMsg="抱歉，新增字典异常，请稍后重试")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    return rs


def addDictValueByKey(keyNo, keyValue, keyName):
    """
    通过字典编新增字典值
    """
    rs = ResultData()
    con, cur = dbutils.getConnect()
    try:
        # 检查字典是否存在
        q_dict = "select count(1) as KEYCOUNT from sysdictionary where keyno =%s"
        cur.execute(q_dict, [str(keyNo)])
        keyCount = cur.fetchall()[0][0]
        if keyCount == 0:
            rs = rs.errorData(errorMsg="字典编号不存在，请先创建字典列表")
            return rs

        # 新增字典
        # 是否存在
        q_dict = "select count(1) as KEYNO from sysdictionary where keyno =%s and keyValue=%s"
        cur.execute(q_dict, [str(keyNo), str(keyValue)])
        if cur.fetchall()[0][0] > 0:
            rs = rs.errorData(errorMsg="字典编号已存在，请重新输入")
            return rs

        i_dict = "insert into sysdictionary (keyno,keyvalue,keydesc,keyenbale,keyorder) values (%s,%s,%s,%s,%s)"
        cur.execute(i_dict, [str(keyNo), str(keyValue), keyName, '1', int(keyCount) + 1])
        con.commit()
    except:
        Logger.errLineNo()
        rs.errorData(errorMsg="抱歉，添加字典值异常，请稍后重试")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    return rs


def editDictValueByKey(keyNo, oldKeyValue,newKeyValue,newKeyName):
    """
    通过字典编编辑字典值
    """
    rs = ResultData()
    con, cur = dbutils.getConnect()
    try:
        # 检查字典是否存在
        q_dict = "select count(1) as KEYCOUNT from sysdictionary where keyno =%s"
        cur.execute(q_dict, [str(keyNo)])
        keyCount = cur.fetchall()[0][0]
        if keyCount == 0:
            rs = rs.errorData(errorMsg="字典编号不存在，请先创建字典列表")
            return rs

        # 新增字典
        # 是否存在
        q_dict = "select count(1) as KEYNO from sysdictionary where keyno =%s and keyValue=%s"
        cur.execute(q_dict, [str(keyNo), str(oldKeyValue)])
        if cur.fetchall()[0][0] > 1:
            rs = rs.errorData(errorMsg="字典值[%s]已存在，请重新输入"%(str(oldKeyValue)))
            return rs

        u_dict = "update sysdictionary set  keyvalue=%(newKeyValue)s ," \
                 " keydesc=%(newKeyName)s where keyno=%(keyNo)s and keyvalue=%(oldKeyValue)s"
        cur.execute(u_dict, {"keyNo":keyNo,"oldKeyValue":oldKeyValue,"newKeyValue":newKeyValue,"newKeyName":newKeyName,})
        con.commit()
    except:
        Logger.errLineNo()
        rs.errorData(errorMsg="抱歉，更新字典值异常，请稍后重试")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    return rs


if __name__ == '__main__':
    rs = getDictKeyList()
    print(rs)
    # rs = delDictByKey('2000')
    # print(rs.toDict())
    # rs = getDictValueList('2000')
    # rs = addDictKey("xxx")

    # delDictValueByKey(5,'110')
    # delDictValueByKey(5,'114')
    rs = addDictValueByKey(5, '112', '天气预报')
    print(rs.toDict())
    rs = addDictValueByKey(5, '114', '交通帮助')
    print(rs.toDict())
