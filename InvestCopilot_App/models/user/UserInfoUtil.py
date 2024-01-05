__author__ = 'Robby'

import traceback

from django.conf import settings
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from django.http import HttpResponseRedirect, JsonResponse
from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.manager import ManagerMenuMode as menu_mode
from InvestCopilot_App.models.toolsutils import dbutils as dbutils

Logger = logger_utils.LoggerUtils()

import datetime
import threading
import functools

import pandas as pd
from InvestCopilot_App.models.cache import cacheDB as cache_db

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.conf import settings as settings


# 操作成功或失败标志
OPERA_SUCCESS = '1'
OPERA_FAILE = '0'

__metaclass__ = type


class UserInfoUtil(object):
    def addUserLog(self, userlogDict):
        try:
            return dbutils.insertDB('userlog', userlogDict)
        except Exception as ex:
            Logger.errLineNo()
            Logger.error("保存用户操作日志记录失败:%s" % ex)

    def buildUserLog(self, request):
        userId = request.session.get('user_id')
        sessionKey = request.session.session_key
        x_forwarded_for = request.META.get('HTTP_X_REAL_IP')
        remoteAddr = request.META.get('REMOTE_ADDR')
        if x_forwarded_for:
            remoteAddr = x_forwarded_for.split(',')[0]
        agent = request.META.get('HTTP_USER_AGENT', None)
        self.userid = userId
        self.sessionid = sessionKey
        self.remoteip = remoteAddr
        self.Platform = self.operaPlatform(agent)
        self.Browser = self.operaBrowser(agent)
        self.operadate = datetime.datetime.now()

    def saveUserLog(self, request, busincode='OTHER', state=1, content=None):
        try:
            self.buildUserLog(request)
            self.busincode = busincode  # 操作类型
            self.state = state  # 操作状态   bool(state) > True 或False 1:表示True。
            self.content = content  # 操作其他描述
            dictparams = self.props(self)  # 将对象属性转换为字典类型

            threading.Thread(target=self.addUserLog,
                             args=(dictparams,)).start()  # 线程操作 t1 = threading.Thread(target=run_thread, args=(5,))
        except Exception as ex:
            Logger.errLineNo()
            Logger.error("保存用户操作日志记录失败:%s" % ex)

    # 判别登录平台
    """ Android = "Mozilla/5.0 (Linux; Android 6.0; NEM-L21 Build/HONORNEM-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.91 Mobile Safari/537.36"
        Safari = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_2 like Mac OS X) AppleWebKit/602.3.12 (KHTML, like Gecko) Version/10.0 Mobile/14C92 Safari/602.1"
        weixin = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat QBCore/3.43.27.400 QQBrowser/9.0.2524.400"
        Chrome = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"
        Edge="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393"
      """

    def operaPlatform(self, agent):
        if 'Android' in agent:
            return "Android"
        elif 'iPhone' in agent:
            return "iPhone"
        elif 'Windows' in agent:
            return "Windows"
        return 'N/A'

    # 操作浏览器
    """
    Edge/14.14393 表示浏览器内核
    Edge="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393"
    """

    def operaBrowser(self, agent):
        agentlist = agent.split(' ')
        agent = agentlist[len(agentlist) - 1]
        return agent

    def props(self, obj):
        pr = {}
        for name in dir(obj):
            value = getattr(obj, name)
            if not name.startswith('__') and not callable(value):
                pr[name] = value
        return pr


def getAllUser():
    con = dbutils.getDBConnect()
    sqlStr = "SELECT * FROM TUSERS T order by to_number(userid)"
    data = pd.read_sql(sqlStr, con=con)
    con.close()
    return data

def getAllMenuInfo():
    con = dbutils.getDBConnect()
    sqlStr = "SELECT * FROM menu  order by parentorderid,suborderid"
    data = pd.read_sql(sqlStr, con=con)
    con.close()
    return data


def build_tree(data, parent_id):
    tree =[]        
    for row in data:
        if row[5] == parent_id:
            node = {
                'menuid': row[0],
                'menuname': row[1],
                'orderid': row[6],
                'children': []
            }
            tree.append(node)
    return tree

def getAllMenuInfo2():
    menuInfo = getAllMenuInfo()
    #将menuInfo转为树形结构
    menuInfo = menuInfo.values.tolist()
    
    lastParentorderid = ''
    tree =[]

    for row in menuInfo:
        
        if row[5]!=lastParentorderid:
            lastParentorderid = row[5]
            node = {
                    'menuid': row[5],
                    'menuname': row[3],
                    'orderid': row[5],
                    'children': build_tree(menuInfo, row[5])
                }
            tree.append(node)
        else:
            continue
    

    
    
    return tree
        


def getUserInfoByUserId(userId):
    con = dbutils.getDBConnect()
    sqlStr = "SELECT * FROM TUSERS T WHERE T.USERID=:USERID"
    data = pd.read_sql(sqlStr, con=con, params={'USERID': userId})
    con.close()
    return data


def getUserInfoByEmail(email):
    con = dbutils.getDBConnect()
    sqlStr = "SELECT * FROM TUSERS T WHERE T.USERNAME=:userName"
    data = pd.read_sql(sqlStr, con=con, params={'userName': email})
    con.close()
    return data


def userlogin(username, userpassword):
    # 20160603调试时直接登录，不做校验
    # request.session['userlogin'] = 1
    # request.session['user_id'] = '01'
    # request.session['userrealname'] = 'SkyZhou'
    # request.session['usernickname'] = 'SkyZhou'
    # return(['100','|','登录成功！'])

    UserName = username
    PassWord = userpassword
    UserLogined ={}

    Logger.debug(UserName)
    Logger.debug(PassWord)
    if settings.DBTYPE =='postgresql':
        SqlStr = "select t.userid,t.username,t.userpassword," \
                "(select roleid from userrole r where t.userid=r.userid) roleid,t.privilegeset,t.userstatus,t.USERREALNAME,t.USERNICKNAME" \
                " from tusers t" \
                " where  t.username= %s  and t.USERPASSWORD = %s limit 1 "
    elif settings.DBTYPE =='Oracle':
        SqlStr = "select t.userid,t.username,t.userpassword," \
                "(select roleid from userrole r where t.userid=r.userid) roleid,t.privilegeset,t.userstatus,t.USERREALNAME,t.USERNICKNAME" \
                " from tusers t" \
                " where t.username= :UserName  and t.USERPASSWORD = :PassWord and rownum =1"
    try:
        con, cur = dbutils.getConnect()
        Result = cur.execute(SqlStr, [UserName, PassWord])
        if settings.DBTYPE=='Oracle':
            UserLogined = Result.fetchall()
        elif settings.DBTYPE=='postgresql':
            UserLogined = cur.fetchall()
    except Exception:
        Logger.errLineNo()
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    
    Logger.info(UserLogined)
    return UserLogined


def userCheckForEmail(username):
    UserName = username
    UserLogined = {}
    Logger.debug(UserName)
    if settings.DBTYPE == 'postgresql':
        SqlStr = "select t.userid,t.username,t.userpassword," \
                 "(select roleid from userrole r where t.userid=r.userid) roleid,t.privilegeset,t.userstatus,t.USERREALNAME,t.USERNICKNAME" \
                 " from tusers t" \
                 " where  t.username= lower(%s)  limit 1 "
    elif settings.DBTYPE == 'Oracle':
        SqlStr = "select t.userid,t.username,t.userpassword," \
                 "(select roleid from userrole r where t.userid=r.userid) roleid,t.privilegeset,t.userstatus,t.USERREALNAME,t.USERNICKNAME" \
                 " from tusers t" \
                 " where t.username= lower(:UserName) and rownum =1"
    try:
        con, cur = dbutils.getConnect()
        Result = cur.execute(SqlStr, [UserName])
        if settings.DBTYPE == 'Oracle':
            UserLogined = Result.fetchall()
        elif settings.DBTYPE == 'postgresql':
            UserLogined = cur.fetchall()
    except Exception:
        Logger.errLineNo()
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    Logger.info(UserLogined)
    return UserLogined

def intserEmailTokenForChangePwd(userid,username,key):
    userid=str(userid)
    con, cur = dbutils.getConnect()
    # 逻辑用户id，存在则更新数据，不存在就插入数据
    sql_exists = f"select count(1) from business.resetpwdinfo where userid='{userid}';"
    cur.execute(sql_exists)
    count_1 = cur.fetchall()[0][0]
    if(count_1>=2):
        sql_delete = f"""delete from business.resetpwdinfo where userid='{userid}';"""
        cur.execute(sql_delete)
        count_1 = 0
    if(count_1<1):
        sql_insert = f"""insert into business.resetpwdinfo (userid, operadate, state, key, remoteip)
            VALUES (%s,%s,%s,%s,%s);"""
        operadate = datetime.datetime.now()
        cur.execute(sql_insert, [userid, operadate, "1", key, username])
    else:
        operadate = datetime.datetime.now()
        sql_insert = f"""update business.resetpwdinfo set operadate = %s,
        state = %s,key = %s,remoteip = %s where userid = %s;
        """
        cur.execute(sql_insert, [operadate, "1", key, username, userid])
    con.commit()
    cur.close()
    con.close()

def seachTokenForChangePwd(userid,token):
    userid=str(userid)
    con, cur = dbutils.getConnect()
    # 检查数据库中是否有该token,没有的话报错，有的话修改state为0
    sql_exists = f"""select key,state from business.resetpwdinfo where 
            userid=(select userid from tusers where userid='{userid}');"""
    cur.execute(sql_exists)
    q_one = cur.fetchall()
    if(len(q_one)<1):
        return 0
    token_database = q_one[0][0]
    if(str(token_database) != str(token)):
        cur.close()
        con.close()
        return 0
    elif (q_one[0][1]==0 or q_one[0][1]=='0'):
        cur.close()
        con.close()
        return 1
    else:
        operadate = datetime.datetime.now()
        sql_insert = f"""update business.resetpwdinfo set operadate = %s where userid = %s;
        """
        cur.execute(sql_insert, [operadate, userid])
        con.commit()
        cur.close()
        con.close()
        return 2

def ssouserlogin(username,userid):
    
    OaUserName = username
    OaUserId = userid
    UserLogined=''    
    SqlStr = "select t.userid,t.username,t.userpassword," \
             "(select wm_concat(roleid) from userrole r where t.userid=r.userid) roleid,t.privilegeset,t.userstatus,t.USERREALNAME,t.USERNICKNAME,t.srcsys,t.inserttime,t.updatetime,t.comments" \
             " from tusers t" \
             " where t.oausername= lower(:UserName) and t.oauserid = :userid and rownum =1 "
    try:
        con, cur = dbutils.getConnect()        
        Result = cur.execute(SqlStr, [OaUserName, OaUserId])
        UserLogined = Result.fetchall()
    except Exception:
        Logger.errLineNo()
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass
    #Logger.info(UserLogined)
    return UserLogined

def getUserLoginRedirectUrl(userId):
    """
    获取用户登录成功的跳转页面
    """
    try:
        con = dbutils.getDBConnect()
        if settings.DBTYPE =='Oracle':
            selectSql1 = '''
            SELECT t.menuidlist FROM privrole t
            INNER JOIN userrole tt ON t.roleid = tt.roleid
            WHERE tt.userid =:userId
            '''
            data1 = pd.read_sql(selectSql1, con=con, params={'userId': userId})
        elif settings.DBTYPE =='postgresql':
            selectSql1 = '''
            SELECT t.menuidlist FROM privrole t
            INNER JOIN userrole tt ON t.roleid = tt.roleid
            WHERE tt.userid = %s
            '''
            data1 = pd.read_sql(selectSql1, con=con, params=[userId])

        if data1.empty:
            return None
        data1.columns= data1.columns.str.upper()
        menuIdStr = data1['MENUIDLIST'].values.tolist()[0]
        if len(menuIdStr) == 0:
            return None

        if settings.DBTYPE =='Oracle':
            selectSql3 ='''
            select a.menuurl  from menu a where a.menuid =(
            select menuid  from tusermainpage t where t.userid =:userId
            and  instr (:menuIdStr,t.MENUID )>0
            )
            ''' 
            data3 = pd.read_sql(selectSql3, con=con,params ={'userId':userId ,'menuIdStr':menuIdStr})        
        elif settings.DBTYPE =='postgresql':
            selectSql3 ='''
            select a.menuurl  from menu a where a.menuid =(
            select menuid  from tusermainpage t where t.userid =%s
            and  instr (%s,t.MENUID )>0
            )
            ''' 
            data3 = pd.read_sql(selectSql3, con=con,params =[userId ,menuIdStr])        
            data3.columns= data3.columns.str.upper()

        
        if data3.empty:
            selectSql2 = '''
            SELECT t.MENUURL FROM MENU t
            WHERE t.MENUID IN (%s)
            ORDER BY t.PARENTORDERID ASC,t.SUBORDERID ASC
            ''' % menuIdStr
            data2 = pd.read_sql(selectSql2, con=con)
            data2.columns = data2.columns.str.upper()
            url = data2['MENUURL'].values.tolist()[0]
        else:
            url = data3['MENUURL'].values.tolist()[0]
            
        con.close()
        return url
    except Exception as ex:
        Logger.errLineNo()
        Logger.error(ex)
        raise


def getAllUrlList():
    """
    获取所有URLS的对应的菜单列表
    """
    con, cur = dbutils.getConnect()
    selectSql1 = '''
    SELECT * from urls
    '''
    res = cur.execute(selectSql1)
    if settings.DBTYPE=='Oracle':
        data1 = res.fetchall()
    elif settings.DBTYPE=='postgresql':
        data1 = cur.fetchall()    
    cur.close()
    con.close()
    return data1


def getAllPrivRoleList():
    """
    获取所有角色访问菜单的权限
    """
    con, cur = dbutils.getConnect()
    selectSql1 = '''
    SELECT * from PrivRole
    '''
    res = cur.execute(selectSql1)
    if settings.DBTYPE=='Oracle':
        data1 = res.fetchall()
    elif settings.DBTYPE=='postgresql':
        data1 = cur.fetchall()    
    cur.close()
    con.close()
    return data1


def getUserAccess(request):
    #获取用户id
    userId = request.session.get('user_id')
    #获取用户角色
    user_roleid = request.session.get('user_roleid')
    user_roleid_list= user_roleid.split(',')
    #获取用户可访问的url
    if len(settings.URLLIST) == 0:
        settings.URLLIST = getAllUrlList()
        settings.PRIVROLELIST = getAllPrivRoleList()
    userAccess = []
    for getPrivRoleList in settings.PRIVROLELIST:
        if getPrivRoleList[0] in user_roleid_list:
            submenulist= getPrivRoleList[2].replace('\'', '').split(',')
            for subMenuId in submenulist:
                if subMenuId != '' and subMenuId not in userAccess:
                    userAccess.append(subMenuId)
    return userAccess
        

def checkuserlogin(request):
    if request.session.get('userlogin', default=0) == 0:
        UserInfoUtil().saveUserLog(request, "登录超时", state='0', content='登录已超时，请重新登录！')
        return (['-200', '|', '登录已超时，请重新登录！'])
    else:
        # request.get_host()      获取请求地址
        # request.path                获取请求的path，不带参数
        # request.get_full_path()  获取完整参数
        # logger.info(request.get_host())
        # logger.info(request.path)

        url = request.get_full_path()
        user_roleid = request.session.get('user_roleid')
        user_roleid_list= user_roleid.split(',')

        if len(settings.URLLIST) == 0:
            settings.URLLIST = getAllUrlList()
            settings.PRIVROLELIST = getAllPrivRoleList()
        urls = url.replace('?','/').split('/')         
        # print(settings.URLLIST)
        # print(settings.PRIVROLELIST)
        # Logger.info("request urls:")
        # Logger.info(user_roleid_list)
        # Logger.info(urls)
        # return 1
        """
        user_roleid ='01'
        settings.URLLIST=[('submenu01', 'index.html'), ('submenu01', 'getFundposition'), ('submenu02', 'fundhold2.html'), ('submenu02', 'getFundposition2')]
        settings.PRIVROLELIST=[('01', '所有权限', "'submenu01','submenu02','submenu03'")]
        urls=['', 'index.html']
        """
        getsubmenu = ''
        for geturl in urls:
            if geturl != '' and geturl != 'api':
                for geturlinmenu in settings.URLLIST:
                    if geturl in geturlinmenu[1]:
                        getsubmenu = geturlinmenu[0]
                        break
                #20221226 修改为多角色判断
                for getPrivRole in settings.PRIVROLELIST:
                    if getPrivRole[0] in user_roleid_list:
                        submenulist= getPrivRole[2].replace('\'', '').split(',')
                        if getsubmenu in submenulist:
                            return 1
                return -1

def isSuperUser(userId):
    # 超级管理员用户ID
    SUPER_USERID = '1'
    if str(userId) == SUPER_USERID:
        return True
    return False


def urlprivilege(request):
    url = request.get_full_path()


def userlogout(request):
    UserInfoUtil().saveUserLog(request, "用户退出", state='1', content='用户退出登录！')
    request.session['userlogin'] = 0
    request.session['user_id'] = ''
    request.session['userrealname'] = ''
    request.session['usernickname'] = ''
    request.session['user_roleid'] = ''
    request.session['definefactor'] = None
    request.session['stockcodelist'] = None


def getUserLockedStatus(request):
    """
    获取用户的锁定状态
    """
    userId = request.session.get('user_id', None)
    if userId is None:
        return None
    return getUserLockedStatusWrap(userId)


def getUserLockedStatusWrap(userId):
    con, cur = dbutils.getConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.userstatus
        FROM tusers t
        WHERE t.userid =:userId
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.userstatus
        FROM tusers t
        WHERE t.userid = %s
        '''
    res = cur.execute(selectSql, [userId])    
    if settings.DBTYPE=='Oracle':
        tmp = res.fetchall()
    elif settings.DBTYPE=='postgresql':
        tmp = cur.fetchall()    
    if len(tmp) == 0:
        return None
    userStatus = tmp[0][0]
    cur.close()
    con.close()
    return userStatus


def userLoginCheckDeco(oldHandler):
    """
    检查用户登录的装饰器
    """

    def newHandler(request, *args, **kwargs):
        #20190430 FZH需求，加入用户访问的参数记录
        content_type = request.content_type
        if content_type != "multipart/form-data":
            threading.Thread(target=recorduserRequest,
                         args=(request,)).start()  # 线程操作
        if checkuserlogin(request) == 1 and getUserLockedStatus(request) == 1:
            return oldHandler(request, *args, **kwargs)
        else:
            rs = ResultData()
            rs.errorData(errorCode ='-200',errorMsg="登录超时或者用户权限不足，请重新登录！")
            return JsonResponse(rs.toDict())

    return newHandler


def operaPlatform(agent):
    if 'Android' in agent:
        return "Android"
    elif 'iPhone' in agent:
        return "iPhone"
    elif 'Windows' in agent:
        return "Windows"
    return 'N/A'


def operaBrowser(agent):
    agentlist = agent.split(' ')
    agent = agentlist[len(agentlist) - 1]
    return agent

def recorduserRequest(request):
    # header = request.META.get('HTTP_USER_AGE')
    x_forwarded_for = request.META.get('HTTP_X_REAL_IP')
    remoteAddr = request.META.get('REMOTE_ADDR')
    if x_forwarded_for:
        remoteAddr = x_forwarded_for.split(',')[0]
    agent = request.META.get('HTTP_USER_AGENT', None)
    Platform = operaPlatform(agent)
    Browser = operaBrowser(agent)
    # 获取地址
    path = request.path
    #Logger.info('Url:'+path)
    # 获取 ? 后面的数据(获取查询字符串数据)
    #Logger.info('Get:')
    #Logger.info(str(para))
    # 获取表单数据
    #Logger.info('POST:')
    #Logger.info(str(form_data))
    # 获取请求方法
    method = request.method
    #Logger.info('Method:')
    #Logger.info(method)
    userid = request.session.get('user_id')
    #去掉Rocky开发测试用户
    if userid != '001' :
        con, cur = dbutils.getConnect()
        try:
            form_data = tools_utils.requestDataFmt(request)
            form_data=form_data.toJson()
            # requestpara={} #form_data.toJson()
            # print("jl requestpara",requestpara)
            sqlstr= 'insert into userrequestlog (userid, requesturl, requestmethod , requestpara,remoteAddr ,Platform,Browser,requesttime) \
                values(%s,%s , %s ,%s,%s , %s ,%s, current_timestamp ) '
            cur.execute(sqlstr, [userid, path, method, form_data,remoteAddr ,Platform,Browser])
        except:
            print(traceback.format_exc())
            pass
        con.commit()
        con.close()
    



def userMenuPrivCheckDeco(subMenuName):
    """
    检查用户是否具体菜单权限的装饰器
    为安全考虑，防止用户跳过菜单配置直接访问页面的url
    """

    def newDeco(oldHandler):
        @functools.wraps(oldHandler)
        def newHandler(request, *args, **kwargs):
            # userId = request.session.get('user_id',None)
            # if userId is None or userId == '':
            #     return HttpResponseRedirect("/")
            # if getUserLockedStatus(request) != 1:
            #     return HttpResponseRedirect("/")
            # subMenuInfoPdData = menu_mode.getSubMenuInfoByName(subMenuName)
            # subMenuId = subMenuInfoPdData['MENUID'].values.tolist()[0]
            # userMenuIdList = menu_mode.getUserMenuIdListByUserId(userId)
            # if subMenuId not in userMenuIdList:
            #     return HttpResponseRedirect("/")
            # else:
            return oldHandler(request, *args, **kwargs)

        return newHandler

    return newDeco


from pyDes import *
import base64
import random
import string


def createUserPwdUrl(userId, remoteIp):
    """
    重置交易密码 ,v_state:0:新建；1： 重置密码成功；
    """
    resultData = ResultData()
    try:
        pwdEec, password = createPwd(8)
        pwdMd5 = tools_utils.md5(pwdEec)
        import time
        pwdMd5 = pwdMd5 + str(time.time())

        # 密钥key 邮件
        pwdLink = base64.b64encode(pwdMd5.encode()).decode('ascii')
        i_resetpwdinfo = """
            insert into resetpwdinfo (userid, operadate, state,key, remoteip)
            values(:v_userid,:v_operadate, :v_state,:v_key, :v_remoteip)
        """
        operadate = datetime.datetime.now()
        con, cur = dbutils.getConnect()

        cur.execute(i_resetpwdinfo, [userId, operadate, '0', pwdLink, remoteIp])

        con.commit()
        resultData.pwdLink = pwdLink
    except Exception as ex:
        Logger.error("抱歉，重置登录密码失败，请稍后重试！")
        Logger.errLineNo()
        return resultData.errorData(errorMsg="抱歉，重置登录密码失败，请稍后重试！")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass

    return resultData


def changepwd(userId, oldpasswd, newpasswd):
    """
    修改密码
    """
    try:
        oldpasswd1 = passWordEncode(oldpasswd)
        newpasswd1 = passWordEncode(newpasswd)
        resultData = ResultData()
        con, cur = dbutils.getConnect()
        if settings.DBTYPE == 'Oracle':
            u_tusers = " update tusers t set t.userpassword =:newPassWord where t.userid =:userId and t.userpassword=:oldPassWord"
        elif settings.DBTYPE == 'postgresql':
            u_tusers = " update tusers  set userpassword =%s where userid =%s and userpassword=%s"
        cur.execute(u_tusers, [newpasswd1, userId, oldpasswd1])
        updaterow = cur.rowcount
        con.commit()
        if updaterow > 0:
            resultData.errorMsg ="修改密码成功！"
            return resultData
        else:
            return resultData.errorData(errorMsg="抱歉，修改密码失败，检查原密码是否正确！")

    except Exception as ex:
        print(ex)
        Logger.error("抱歉，修改密码失败，请稍后重试！")
        Logger.errLineNo()
        return resultData.errorData(errorMsg="抱歉，修改密码失败，请稍后重试！")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass


def restPwd(userId, newpasswd):
    """
    重置密码
    """
    try:
        userId=str(userId)
        newpasswd1 = passWordEncode(newpasswd)
        resultData = ResultData()
        con, cur = dbutils.getConnect()
        if settings.DBTYPE == 'Oracle':        
            u_tusers = " update tusers set userpassword =:newPassWord where userid =:userId"
        elif settings.DBTYPE == 'postgresql':
            u_tusers = " update tusers set userpassword =%s where userid =%s"
        cur.execute(u_tusers, [newpasswd1, userId])
        updaterow = cur.rowcount
        con.commit()
        if updaterow > 0:
            operadate = datetime.datetime.now()
            sql_insert = f"""update business.resetpwdinfo set operadate = %s , state= %s where userid = %s;"""
            cur.execute(sql_insert, [operadate,"0",userId])
            resultData.errorMsg = "重置登录密码成功！"
            return resultData
        else:
            return resultData.errorData(errorMsg="抱歉，重置密码失败，请稍后重试！")

    except Exception as ex:
        print(ex)
        Logger.error("抱歉，修改密码失败，请稍后重试！")
        Logger.errLineNo()
        return resultData.errorData(errorMsg="抱歉，重置密码失败，请稍后重试！")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass


def resetPwdUrlFlag(userId, key, remoteIp):
    """
    重置密码状态  ,v_state:0:新建；1： 重置密码成功；
    """
    resultData = ResultData()
    try:
        con, cur = dbutils.getConnect()
        if settings.DBTYPE == 'Oracle':
            u_resetpwdinfo = " update resetpwdinfo t set t.remoteip =:remoteIp , t.state =:state  where t.userid =:userId and t.key =:key"
        elif settings.DBTYPE == 'postgresql':
            u_resetpwdinfo = " update resetpwdinfo t set t.remoteip =%s , t.state =%s  where t.userid =%s and t.key =%s"
        cur.execute(u_resetpwdinfo, [remoteIp, '1', userId, key])
        con.commit()
    except Exception as ex:
        Logger.error("抱歉，重置登录密码失败，请稍后重试！")
        Logger.errLineNo()
        return resultData.errorData(errorMsg="抱歉，重置登录密码失败，请稍后重试！")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass

    return resultData


def resetUserPwd(userId, newPassWord):
    """
    重置交易密码 ,v_state:0:新建；1： 重置密码成功；
    """
    resultData = ResultData()
    try:
        con, cur = dbutils.getConnect()
        u_tusers = " update tusers t set t.userpassword =:newPassWord where t.userid =:userId"
        cur.execute(u_tusers, [newPassWord, userId])
        resultData.errorMsg = "重置密码成功！"
        con.commit()
    except Exception as ex:
        Logger.error("抱歉，重置登录密码失败，请稍后重试！")
        Logger.errLineNo()
        return resultData.errorData(errorMsg="抱歉，重置登录密码失败，请稍后重试！")
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass

    return resultData


def createPwd(pwdLen=8):
    # 密码加解密操作
    abcFactor = [x for x in string.ascii_letters]
    numFactor = [x for x in string.digits]
    # 数字+字母大小写组合
    pwdFactors = abcFactor + numFactor
    # print('pwdFactors:',pwdFactors,len(pwdFactors))
    newPass = []
    # 8位长度
    for x in range(pwdLen):
        newPass.append(pwdFactors[random.randint(0, 61)])

    newPass = "".join(newPass)
    print('newPass:', newPass)
    password = passWordEncode(newPass)
    return password, newPass


def passWordEncode(passWord):
    # 加密
    key= settings.SYSCRYPTKEY
    k = des(key, CBC, "\0\0\0\0\0\0\0\0", padmode=PAD_PKCS5)
    password = base64.b64encode(k.encrypt(passWord)).decode('ascii')
    return password

def passWordDecode(encodePassWord):
    # 加密
    key= settings.SYSCRYPTKEY
    k = des(key, CBC, "\0\0\0\0\0\0\0\0", padmode=PAD_PKCS5)
    #password = base64.b64encode(k.encrypt(passWord)).decode('ascii')
    password = k.decrypt(base64.decodebytes(
        encodePassWord.encode('ascii'))).decode('ascii')    
    return password

def chekPwdKey(key):
    resultData = ResultData()
    # 检查key是否有效
    q_resetpwdinfo = "select * from resetpwdinfo t where t.key=:key"
    con = dbutils.getDBConnect()
    keyInfoDF = pd.read_sql(q_resetpwdinfo, con, params={'key': key})
    if keyInfoDF.empty:
        return resultData.errorData(errorMsg="请求链接不存在，请重试申请！")

    operaDate = keyInfoDF.loc[0]['OPERADATE']
    state = keyInfoDF.loc[0]['STATE']
    userId = keyInfoDF.loc[0]['USERID']
    if state == '1':  # ,v_state:0:新建；1： 重置密码成功；
        return resultData.errorData(errorMsg="请求链接已处理，请重试申请！")

    print('operaDate:', operaDate, str(operaDate))

    import datetime
    d = datetime.datetime.strptime(str(operaDate), "%Y-%m-%d %H:%M:%S")
    nextDay = d + datetime.timedelta(days=1)

    nowTime = datetime.datetime.now()
    print(nextDay, nowTime)

    if nowTime > nextDay:  # 24小时过期
        return resultData.errorData(errorMsg="请求链接已过期(24小时之内有效)，请重试申请！")

    resultData.userId = userId
    return resultData

def CheckUserFundPriv(userid, sourceDF, columnsName):
    if columnsName in sourceDF.columns.values.tolist():
        con = dbutils.getDBConnect()
        sql = "select  fundcode from fundpriv t where t.userrealname = (select userrealname from tusers a where a.userid = :Userid)"
        fundaccessDF =  pd.read_sql(sql,con, params={'Userid':userid})
        fundaccessDF1 = fundaccessDF.values.tolist()
        fundaccessList =[]
        for fund in fundaccessDF1:
            fundaccessList.append(fund[0])
        print(fundaccessList)
        if len(fundaccessList) >0 :
            result = sourceDF[sourceDF[columnsName].isin(fundaccessList)]  
            print(result)
            return result
        else:
            result = pd.DataFrame()
            return result
    else:
        return sourceDF



def checkIBPUserAgent(oldHandler):
    """
    检查IBP用户访问浏览代理的装饰器
    """

    def newHandler(request, *args, **kwargs):
        recorduserRequest(request)
        agent = request.META.get('HTTP_USER_AGENT', None)        
        if 'IBP/2019' in agent:
            return oldHandler(request, *args, **kwargs)
        else:
            return HttpResponseRedirect("/")

    return newHandler

def getCacheUserInfo(userId):#获取用户缓存信息
    userCfg_dt = cache_db.getUserConfig_dt()
    if userId in userCfg_dt:
        userCfg = userCfg_dt[userId]
        return userCfg
    return None
if __name__ == '__main__':
    # data = getUserInfoByUserId('1')
    # print(data)
    # createUserPwdUrl('111', '127.0.0.1')
    # 密码加解密操作
    pwdEec, password = createPwd(8)
    print(pwdEec, password)
    newPassWord = passWordEncode('gfjjfeuser')
    print(newPassWord)
    # data = resetUserPwd('051', newPassWord)
    # print(data.toDict())
    # 解密
    # password='89J+mipWq4VviXPkgDgTyw=='
    key= settings.SYSCRYPTKEY
    password = '4cWC3BlOQyNDw5uR9jt+ARbsTouTPjAg'
    password = '9aTqfBDZn7hjBnMxxPQpqQ=='
    password = 'sYoz2n1ubUVS6yuNV3I8Nw=='
    password = 'x6zQXgPgGaSlVXUSxrT1AQ=='
    # password = 'x6zQXgPgGaSlVXUSxrT1AQ=='
    password = 'UeMrXuxBMok83EseF2gtgg=='
    password = 'pfDBkKA1z5E='
    password = 'eISHGdftqLWUsYRgjKwouQ=='
    password = '72Ofx9wOmW4gxQairth4Xw=='
    # password = 'xqsDHP/sSzg='
    k = des(key, CBC, "\0\0\0\0\0\0\0\0", padmode=PAD_PKCS5)
    password2 = k.decrypt(base64.decodebytes(password.encode('ascii'))).decode('ascii')
    print('password2:', password2)
    print("\u56e0\u7248\u6743\u65b9\u8981\u6c42\uff0c\u8be5\u62a5\u544a\u4e0d\u5bf9\u5916\u5c55\u793a")