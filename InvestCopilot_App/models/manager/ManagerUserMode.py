from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.user import UserInfoUtil as user_info_utils
import pandas as pd


DEFAULT_PRIVILEGESET = '0'
DEFAULT_USERROLE = '0'
DEFAULT_ROLENAME = 'NOBODY'
DEFAULT_ROLEPRIVILEGES = '0'

logger = logger_utils.LoggerUtils()

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.conf import settings as settings


def getAllUserInfo0(queryStr ):
    """
    获取所有的用户信息
    """
    queryStr = '%'+queryStr+'%'  
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        DECODE(t.userstatus,'1','正常','0','已锁定') AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, wm_concat(roleid) as roleid, wm_concat(privrolename) as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid)
        group by userid
        ) tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        select * from (
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        (case when t.userstatus ='1' then '正常' else '已锁定' end) AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, string_agg(roleid,',') as roleid, string_agg(privrolename,',') as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid) as l_t1
        group by userid
        ) as tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC) as l_t2
        where  username like %s or userrealname like %s or usernickname like %s or 
        company like %s  or groupname like %s  or department like %s  or 
        privrolename like %s  or userstatus like %s 
        '''
    
    data = pd.read_sql(selectSql, con=con, params=[ queryStr, queryStr, queryStr, queryStr, queryStr, queryStr,queryStr, queryStr])    
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getAllUserInfo1(pageIndex,pageSize ):
    """
    获取所有的用户信息
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        DECODE(t.userstatus,'1','正常','0','已锁定') AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, wm_concat(roleid) as roleid, wm_concat(privrolename) as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid)
        group by userid
        ) tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        select * from (
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        (case when t.userstatus ='1' then '正常' else '已锁定' end) AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, string_agg(roleid,',') as roleid, string_agg(privrolename,',') as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid) as l_t1
        group by userid
        ) as tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC) as l_t2
        limit %s offset %s;
        '''
    data = pd.read_sql(selectSql, con=con, params=[ pageSize, (pageIndex-1)*pageSize])
    data.columns = data.columns.str.upper()
    con.close()
    return data

def getAllUserInfo2(pageIndex,pageSize ,queryStr):
    """
    获取所有的用户信息
    """
    queryStr = '%'+queryStr+'%'
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        DECODE(t.userstatus,'1','正常','0','已锁定') AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, wm_concat(roleid) as roleid, wm_concat(privrolename) as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid)
        group by userid
        ) tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        select * from (
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        (case when t.userstatus ='1' then '正常' else '已锁定' end) AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, string_agg(roleid,',') as roleid, string_agg(privrolename,',') as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid) as l_t1
        group by userid
        ) as tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC) as l_t2
        where  username like %s or userrealname like %s or usernickname like %s or 
        company like %s  or groupname like %s  or department like %s  or 
        privrolename like %s  or userstatus like %s 
        limit %s offset %s 
        '''
    
    data = pd.read_sql(selectSql, con=con, params=[queryStr, queryStr, queryStr, queryStr, queryStr, queryStr,queryStr, queryStr, pageSize, (pageIndex-1)*pageSize])        
    data.columns = data.columns.str.upper()
    con.close()
    return data

def getAllUserInfo():
    """
    获取所有的用户信息
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid ,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        DECODE(t.userstatus,'1','正常','0','已锁定') AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, wm_concat(roleid) as roleid, wm_concat(privrolename) as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid)
        group by userid
        ) tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.userid,
        t.username,
        t.userrealname,
        t.usernickname,
        tt4.companyname as company,
        tt4.companyid,
        '' as  groupname,
        t.department,
        tt3.roleid,
        tt3.privrolename,
        (case when t.userstatus ='1' then '正常' else '已锁定' end) AS userstatus
        FROM tusers t    
        LEFT JOIN (
        select userid, string_agg(roleid,',') as roleid, string_agg(privrolename,',') as privrolename from (    
        SELECT t4.userid,
        t5.roleid ,
        t5.rolename AS privrolename
        FROM userrole t4
        LEFT JOIN privrole t5 ON t4.roleid = t5.roleid) as l_t1
        group by userid
        ) as tt3
        ON t.userid = tt3.userid
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ORDER BY t.userid ASC;
        '''
    data = pd.read_sql(selectSql, con=con)
    data.columns = data.columns.str.upper()
    con.close()
    return data

def getAllUserInfoCount():
    """
    获取所有的用户信息条数
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT count(1) as count from tusers t
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT count(1) as count from tusers t
        '''
    data = pd.read_sql(selectSql, con=con)
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getAllUserInfoCount2(queryStr):
    """
    获取所有的用户信息条数
    """
    queryStr = '%'+queryStr+'%'
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        select count(1) as count from(
        SELECT t.userid, t.username, t.userrealname, t.usernickname, t.department, DECODE(t.userstatus,'1','正常','0','已锁定') AS userstatus, tt4.companyname from tusers t  
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ) ct 
        where ct.username like :queryStr or ct.userrealname like :queryStr or ct.usernickname like :queryStr or
        ct.department like :queryStr or ct.userstatus like :queryStr  or ct.companyname like :queryStr
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        select count(1) as count from(
        SELECT t.userid, t.username, t.userrealname, t.usernickname, t.department,(case when t.userstatus ='1' then '正常' else '已锁定' end) AS userstatus, tt4.companyname from tusers t  
        left join ( select cc.userid, c1.companyid, c1.companyname from companyuser cc left join company c1 on cc.companyid = c1.companyid ) tt4
        on t.userid = tt4.userid
        ) ct 
        where username like %s or userrealname like %s or usernickname like %s or
        department like %s or userstatus like %s or companyname like %s    
        '''
    data = pd.read_sql(selectSql, con=con, params=[queryStr, queryStr, queryStr, queryStr, queryStr,queryStr])
    data.columns = data.columns.str.upper()
    con.close()
    return data

def getAllCompanyInfo():
    """
    获取所有的公司
    """
    con = dbutils.getDBConnect()
    selectSql = '''
    SELECT * FROM company
    '''
    data = pd.read_sql(selectSql, con=con)
    con.close()
    return data


def getAllGroupInfo():
    """
    获取所有的用户组
    """
    con = dbutils.getDBConnect()
    selectSql = '''
    SELECT * FROM tgroups
    '''
    data = pd.read_sql(selectSql, con=con)
    con.close()
    return data


def addUser(email, userRealName, userNickName, encyptPwd, roleId):
    """
    添加用户
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'Oracle':
            selectSql = '''
            SELECT SEQ_USERID.NEXTVAL AS userid
            FROM DUAL
            '''
        elif settings.DBTYPE == 'postgresql':
            selectSql = '''
            SELECT nextval('seq_userid') AS userid
            '''
        res = cur.execute(selectSql)
        if settings.DBTYPE == 'Oracle':
            userId = res.fetchall()[0][0]
        elif settings.DBTYPE == 'postgresql':
            userId = cur.fetchall()[0][0]
        
        if settings.DBTYPE == 'Oracle':
            i_tusers = '''
            INSERT INTO tusers(userid,username,userpassword,privilegeset,userstatus,userrealname,usernickname,roleid)
            VALUES(:1,lower(:2),:3,:4,:5,:6,:7,:8)
            '''
            i_userrole = '''
            INSERT INTO userrole(userid,roleid)
            VALUES(:1,:2)
            '''
        elif settings.DBTYPE == 'postgresql':
            i_tusers = '''
            INSERT INTO tusers(userid,username,userpassword,privilegeset,userstatus,userrealname,usernickname,roleid)
            VALUES(%s,lower(%s),%s,%s,%s,%s,%s,%s)
            '''
            i_userrole = '''
            INSERT INTO userrole(userid,roleid)
            VALUES(%s,%s)
            '''
        cur.execute(i_tusers, [userId, email, encyptPwd, DEFAULT_PRIVILEGESET, '1', userRealName, userNickName, DEFAULT_USERROLE])
        userRoleIdList=  roleId.split(',')
        for userRoleId in userRoleIdList:
            cur.execute(i_userrole, [userId, userRoleId])
        con.commit()
        return userId
    except:
        logger.errLineNo()
        return None
    finally:
        cur.close()
        con.close()


def checkEmailExists(email):
    """
    检查用户名是否已经存在
    已经存在返回True 否则返回False
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'Oracle':
            selectSql = '''
                SELECT COUNT(1)
                FROM tusers t
                WHERE t.username =:email
                '''
        elif settings.DBTYPE == 'postgresql':
            selectSql = '''
                SELECT COUNT(1)
                FROM tusers 
                WHERE username =%s
                '''
        res = cur.execute(selectSql, [email])        
        if settings.DBTYPE == 'Oracle':
            check = res.fetchall()[0][0]
        elif settings.DBTYPE == 'postgresql':
            check = cur.fetchall()[0][0]
        if check >= 1:
            return True
        else:
            return False
    except:
        logger.errLineNo()
        return True
    finally:
        cur.close()
        con.close()

def setUserRole(userId, roleId):
    """
    设置用户所属的角色
    """
    con, cur = dbutils.getConnect()
    try:
        insertSql = '''
        INSERT INTO userrole(userid,roleid)
        VALUES(:1,:2)
        '''
        userRoleIdList=  roleId.split(',')
        for userRoleId in userRoleIdList:
            cur.execute(i_userrole, [userId, userRoleId])
        #cur.execute(insertSql, [userId, roleId])
        con.commit()
        return True
    except:
        logger.errLineNo()
        return False
    finally:
        cur.close()
        con.close()


def changeUserRole(userId, roleId,companyId):
    """
    设置用户所属的角色
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'Oracle':
            d_userrole = 'delete userrole t  where userid=:userId'
            d_usercompany = 'delete companyuser t  where userid=:userId'
            i_userrole = 'INSERT INTO userrole(userid,roleid)  VALUES(:1,:2) '
            i_usercompany = 'INSERT INTO companyuser(userid,companyid)  VALUES(:1,:2) '
        elif settings.DBTYPE == 'postgresql':
            d_userrole = 'delete from userrole t  where userid=%s'
            d_usercompany = 'delete from companyuser t  where userid=%s'
            i_userrole = 'INSERT INTO userrole(userid,roleid)  VALUES(%s,%s) '
            i_usercompany = 'INSERT INTO companyuser(userid,companyid)  VALUES(%s,%s) '
        #u_users = 'update tusers t set t.roleid=:roleId where userid=:userId'
        cur.execute(d_userrole, [userId])
        cur.execute(d_usercompany, [userId])
        userRoleIdList=  roleId.split(',')
        for userRoleId in userRoleIdList:
            cur.execute(i_userrole, [userId, userRoleId ])
        if companyId is not None and companyId != '':
            cur.execute(i_usercompany, [userId, companyId ])
        #cur.execute(u_users, [roleId, userId])
        con.commit()
        return True
    except:
        logger.errLineNo(msg="修改用户所属的角色异常")
        return False
    finally:
        cur.close()
        con.close()


def setUserGroup(groupName, userId):
    """
    设置用户所属权限组（对应研究报告的权限）
    """
    con, cur = dbutils.getConnect()
    try:
        groupInfoPdData = getAllGroupInfo()
        groupInfoPdData = groupInfoPdData[['GROUPID', 'GROUPNAME']]
        groupInfoPdData = groupInfoPdData.drop_duplicates(['GROUPNAME'])
        groupNameList = groupInfoPdData['GROUPNAME'].values.tolist()
        if groupName in groupNameList:
            tmpPdData = groupInfoPdData.loc[groupInfoPdData['GROUPNAME'] == groupName]
            groupId = tmpPdData['GROUPID'].values.tolist()[0]
        else:
            selectSql = '''
            SELECT SEQ_USERID.NEXTVAL AS groupId
            FROM DUAL
            '''
            res = cur.execute(selectSql)
            groupId = res.fetchall()[0][0]
        insertSql = '''
        INSERT INTO tgroups(groupid,groupname,userid,rolename,roleprivileges)
        VALUES (:1,:2,:3,:4,:5)
        '''
        cur.execute(insertSql, [groupId, groupName, userId, DEFAULT_ROLENAME, DEFAULT_ROLEPRIVILEGES])
        con.commit()
        return True
    except:
        logger.errLineNo()
        return False
    finally:
        cur.close()
        con.close()


def stopUser(userId):
    """
    停用用户
    """
    con, cur = dbutils.getConnect()
    isSuccess = True
    try:
        userStatus = user_info_utils.getUserLockedStatusWrap(userId)
        if userStatus is None:
            raise '不存在该用户'
        if str(userStatus) == '1':
            # 设置用户状态
            if settings.DBTYPE == 'Oracle':
                updateSql = '''
                UPDATE tusers t
                SET t.userstatus = '0'
                WHERE t.userid =:userId
                '''
            elif settings.DBTYPE == 'postgresql':
                updateSql = '''
                UPDATE tusers 
                SET userstatus = '0'
                WHERE userid =%s
                '''            
            cur.execute(updateSql, [userId])
            con.commit()

    except Exception as ex:
        logger.errLineNo()
        isSuccess = False
    finally:
        cur.close()
        con.close()
    return isSuccess


def startUser(userId):
    """
    启用用户
    """
    con, cur = dbutils.getConnect()
    isSuccess = True
    try:
        userStatus = user_info_utils.getUserLockedStatusWrap(userId)
        if userStatus is None:
            raise '不存在该用户'
        if str(userStatus) == '0':
            if settings.DBTYPE == 'Oracle':
                updateSql = '''
                UPDATE tusers t
                SET t.userstatus = '1'
                WHERE t.userid =:userId
                '''
            elif settings.DBTYPE == 'postgresql':
                updateSql = '''
                UPDATE tusers 
                SET userstatus = '1'
                WHERE userid =%s
                '''
            cur.execute(updateSql, [userId])
            con.commit()
    except Exception as ex:
        logger.errLineNo()
        isSuccess = False
    finally:
        cur.close()
        con.close()
    return isSuccess


if __name__ == '__main__':
    pass
