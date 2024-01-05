from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import pandas as pd

logger = logger_utils.LoggerUtils()

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.conf import settings as settings

DEFAULT_SUBMENUID = "'nomenu','submenu00'"


def getAllPrivRoleInfo():
    """
    获取所有的角色权限信息
    """
    con = dbutils.getDBConnect()
    selectSql = '''
    SELECT * FROM PRIVROLE t
    ORDER BY t.roleid ASC
    '''    
    data = pd.read_sql(selectSql, con=con)
    data.columns = data.columns.str.upper()
    con.close()
    return data


def deletePrivRoleByRoleId(roleId):
    #删除角色及用户与角色关联关系
    rs = ResultData()
    try:
        con, cur = dbutils.getConnect()
        if settings.DBTYPE == 'Oracle':
            u_privrole = 'delete from  privrole t  WHERE t.roleid =:roleId'
            u_userrole = "update userrole t set t.roleid='' WHERE t.roleid =:roleId"
        elif settings.DBTYPE == 'postgresql':
            u_privrole = 'delete from  privrole WHERE roleid =%s'
            u_userrole = "update userrole set roleid='' WHERE roleid =%s"
        cur.execute(u_privrole, [roleId])
        cur.execute(u_userrole, [roleId])
        con.commit()
    except:
        logger.errLineNo("删除角色失败")
        rs.errorData(errorMsg='抱歉，删除角色失败，请稍后重试！')
    finally:
        cur.close()
        con.close()
    return rs

def CorrectionMenuPriv():
    """
    修正菜单权限
    """
    con = dbutils.getDBConnect()    
    curr= con.cursor()
    selectSql = "SELECT * FROM PRIVROLE  order by roleid"
    curr.execute(selectSql)
    data = curr.fetchall()
    for row in data:
        roleid = row[0]
        menuidlist = row[2]
        if menuidlist == None or menuidlist == '':
            menuidlist = DEFAULT_SUBMENUID
            
        else:
            allSubMenuid = getAllMenuInfo()['MENUID'].values.tolist()
            allSubMenuid.append('nomenu')
            menuidlist = menuidlist.replace("'",'').split(',')            
            newMenuidlist = []
            for i in range(len(menuidlist)):
                if ',' in menuidlist[i]:
                    newList= menuidlist[i].replace("'",'').split(',')
                    for j in range(len(newList)):
                        if newList[j] not in newMenuidlist and newList[j] in allSubMenuid:
                            newMenuidlist.append(newList[j])                    
                else:
                    if menuidlist[i] not in newMenuidlist and menuidlist[i] in allSubMenuid:
                        newMenuidlist.append(menuidlist[i])
            newMenuidlist2  =[]
            for i in range(len(newMenuidlist)):
                if newMenuidlist[i] not in newMenuidlist2:
                    newMenuidlist2.append(newMenuidlist[i])
            
        menuidlist = list(map(lambda x: "'"+x+"'", newMenuidlist2))
        menuidlist = ','.join(menuidlist)
        if settings.DBTYPE == 'Oracle':
            updateSql = "update PRIVROLE set menuidlist =:menuidlist where roleid =:roleid"                
            curr.execute(updateSql,{'menuidlist':menuidlist,'roleid':roleid})
        elif settings.DBTYPE == 'postgresql':
            updateSql = "update PRIVROLE set menuidlist =%s where roleid =%s"
            curr.execute(updateSql,(menuidlist,roleid))                            
        
        con.commit()
    curr.close()
    con.close()


    

def getPrivRoleInfoByRoleId(roleId):
    """
    通过角色id获取对应角色的信息
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT * FROM PRIVROLE t
        WHERE t.ROLEID =:roleId
        '''
        data = pd.read_sql(selectSql, con=con, params={'roleId': roleId})
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT * FROM PRIVROLE t
        WHERE t.ROLEID =%s
        '''
        data = pd.read_sql(selectSql, con=con, params=[roleId])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getMenuInfoByMenuId(menuIdStr):
    """
    依据菜单id获取菜单信息
    """
    con = dbutils.getDBConnect()
    selectSql = '''
    SELECT * FROM menu t
    WHERE t.menuid IN (%s)
    ORDER BY t.parentorderid ASC,t.suborderid ASC
    ''' % menuIdStr
    data = pd.read_sql(selectSql, con=con)
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getAllMenuInfo():
    """
    获取所有的菜单信息
    """
    con = dbutils.getDBConnect()
    selectSql = '''
    SELECT t.* FROM menu t
    ORDER BY t.parentorderid ASC,t.suborderid ASC
    '''
    data = pd.read_sql(selectSql, con=con)
    data.columns = data.columns.str.upper()
    con.close()
    return data


def amendPrivMenu(roleId, subMenuIdStr):
    """
    修改角色
    """
    try:
        con, cur = dbutils.getConnect()
        if settings.DBTYPE == 'Oracle':
            updateSql = '''
            UPDATE privrole t
            SET t.menuidlist =:subMenuIdStr
            WHERE t.roleid =:roleId
            '''
        elif settings.DBTYPE == 'postgresql':
            updateSql = '''
            UPDATE privrole 
            SET menuidlist =%s
            WHERE roleid =%s
            '''
        cur.execute(updateSql, [subMenuIdStr, roleId])
        con.commit()
    except:
        logger.errLineNo()
    finally:
        cur.close()
        con.close()


def getSubMenuInfoByName(subMenuName):
    """
    依据二级菜单名称获取二级菜单信息
    """
    menuPdData = cache_db.getMenuDF()
    data = menuPdData.loc[menuPdData['MENUNAME'] == subMenuName]
    return data


def getUserMenuIdListByUserId(userId):
    """
    依据用户id获取其对应的菜单列表
    """
    data = cache_db.getUserMenuIdDF()
    data = data.loc[data['USERID'] == userId]
    menuIdStr = data['MENUIDLIST'].values.tolist()[0]
    menuIdList = menuIdStr.split(',')
    menuIdListFinal = list(map(lambda x: x.replace("'", ""), menuIdList))
    return menuIdListFinal


def addPrivRole(roleName):
    """
    添加角色
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'Oracle':
            seqSql = '''
            SELECT SEQ_USERID.nextval FROM dual
            '''
        elif settings.DBTYPE == 'postgresql':
            seqSql = '''
            SELECT nextval('seq_userid') as NEXTVAL
            '''
        data1 = pd.read_sql(seqSql, con=con)
        data1.columns = data1.columns.str.upper()
        roleId = data1['NEXTVAL'].values.tolist()[0]
        if settings.DBTYPE == 'Oracle':
            insertSql = '''
            INSERT INTO privrole(roleid,rolename,menuidlist)
            VALUES (:roleId,:roleName,:menuIdList)
            '''
        elif settings.DBTYPE == 'postgresql':
            insertSql = '''
            INSERT INTO privrole(roleid,rolename,menuidlist)
            VALUES (%s,%s,%s)
            '''
        menuIdStr = DEFAULT_SUBMENUID
        cur.execute(insertSql, [str(roleId), roleName, menuIdStr])
        con.commit()
        return roleId
    except:
        logger.errLineNo()
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass


if __name__ == '__main__':
    getAllMenuInfo()
    pass
