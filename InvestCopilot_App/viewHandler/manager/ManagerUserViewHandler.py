import threading

from django.conf import settings
import pandas as pd
import json
from django.http import JsonResponse
from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.manager import ManagerUserMode as manager_user_mode
from InvestCopilot_App.models.manager import ManagerMenuMode as menu_mode
from InvestCopilot_App.models.user import UserInfoUtil as user_util
from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.manager import ManagerMenuMode as manager_menu_mode
from InvestCopilot_App.models.user.companyMode import companyMode
from InvestCopilot_App.models.user import UserInfoUtil as user_utils
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.header import Header

logger = logger_utils.LoggerUtils()


@userMenuPrivCheckDeco('用户管理')
@userLoginCheckDeco
def managerUserPage(request):
    """
    用户管理权限页面
    """
    resultData = ResultData()
    ##################获取公司信息##################
    """
    companyInfoPdData = manager_user_mode.getAllCompanyInfo()
    companyInfoPdData = companyInfoPdData.rename(columns={'COMPANYID': 'id',
                                                          'COMPANYNAME': 'name'})
    companyKeyList = companyInfoPdData.columns.values.tolist()
    companyValueList = companyInfoPdData.values.tolist()
    companyList = [dict(zip(companyKeyList, valueList)) for valueList in companyValueList]
    print("companyList:", companyList)
    resultData.companyList = companyList
    ##################获取权限组信息##################
    groupInfoPdData = manager_user_mode.getAllGroupInfo()
    groupInfoPdData = groupInfoPdData[['GROUPID', 'GROUPNAME']]
    groupInfoPdData = groupInfoPdData.rename(columns={'GROUPID': 'id',
                                                      'GROUPNAME': 'name'})
    groupInfoPdData = groupInfoPdData.drop_duplicates(['name'])
    groupKeyList = groupInfoPdData.columns.values.tolist()
    groupValueList = groupInfoPdData.values.tolist()
    groupList = [dict(zip(groupKeyList, valueList)) for valueList in groupValueList]

    resultData.groupList = groupList
    """
    ##################获取角色信息##################
    roleInfoPdData = menu_mode.getAllPrivRoleInfo()
    roleInfoPdData = roleInfoPdData[['ROLEID', 'ROLENAME']]
    roleInfoPdData = roleInfoPdData.rename(columns={'ROLEID': 'id', 'ROLENAME': 'name'})
    roleKeyList = roleInfoPdData.columns.values.tolist()
    roleValueList = roleInfoPdData.values.tolist()
    roleList = [dict(zip(roleKeyList, valueList)) for valueList in roleValueList]
    resultData.firstRoleId = ''
    resultData.firstRoleName = ''
    if len(roleList) > 0:
        resultData.firstRoleId = roleList[0]['id']
        resultData.firstRoleName = roleList[0]['name']

    resultData.roleList = roleList
    returnPage = 'manager/manageuser.html'
    resultData.topMenu = '后台维护'
    resultData.subMenu = '用户管理'
    return resultData.buildRender(request, returnPage)


@userLoginCheckDeco
def getAllUserData(request):
    """
    获取所有的用户数据
    """
    try:
        resultData = ResultData()
        pageIndex = request.POST.get('pageIndex', None)
        pageSize = request.POST.get('pageSize', None)
        queryStr = request.POST.get('queryStr', None)
        if pageIndex is None:
            jsonRequest = json.loads(request.body)
            try :
                pageIndex = jsonRequest['pageIndex']
            except Exception as err:
                pageIndex = None
            try :
                pageSize = jsonRequest['pageSize']
            except Exception as err:
                pageSize = None
            try:
                queryStr = jsonRequest['queryStr']
            except Exception as err:
                queryStr = None
        if queryStr is not None:
            if len(queryStr)==0:
                queryStr = None
        if not  queryStr is  None and pageIndex is None:
            userInfoPdData = manager_user_mode.getAllUserInfo0(queryStr)
            allUserInfoCount = len(userInfoPdData)
        if queryStr is None  and not  pageIndex is None:
            userInfoPdData = manager_user_mode.getAllUserInfo1(pageIndex, pageSize)
            allUserInfoCountDF = manager_user_mode.getAllUserInfoCount()
            allUserInfoCount = allUserInfoCountDF['COUNT'].values.tolist()[0]
        if not queryStr is None  and not  pageIndex is None:
            userInfoPdData = manager_user_mode.getAllUserInfo2(pageIndex, pageSize,queryStr)
            allUserInfoCountDF = manager_user_mode.getAllUserInfoCount2(queryStr)
            allUserInfoCount = allUserInfoCountDF['COUNT'].values.tolist()[0]
        if queryStr is None and pageIndex is None:
            userInfoPdData = manager_user_mode.getAllUserInfo()
            allUserInfoCountDF = manager_user_mode.getAllUserInfoCount()
            allUserInfoCount = allUserInfoCountDF['COUNT'].values.tolist()[0]
        userInfoPdData = userInfoPdData.fillna('')
        dataLength = len(userInfoPdData.values.tolist())
        tmpPdData = pd.DataFrame({'操作': [''] * dataLength})
        userInfoPdData = pd.concat([userInfoPdData, tmpPdData], axis=1)
        userInfoPdData = userInfoPdData[
            ['ROLEID', 'USERID', 'USERNAME', 'USERREALNAME', 'USERNICKNAME', 'PRIVROLENAME', 'COMPANY','COMPANYID','USERSTATUS', '操作']]
        resultData.list = json.loads(userInfoPdData.to_json(orient='records'))

        resultData.recordsCount = allUserInfoCount
        userInfoPdData = userInfoPdData.rename(columns={'USERID': '用户ID',
                                                        'USERNAME': '登录名',
                                                        'USERREALNAME': '真实姓名',
                                                        'USERNICKNAME': '昵称',
                                                        'PRIVROLENAME': '所属角色',
                                                        'COMPANY':'公司',
                                                        'COMPANYID':'公司ID',
                                                        'USERSTATUS': '状态'})
        resultData.tbColumn = userInfoPdData.columns.values.tolist()
        resultData.tbData = userInfoPdData.values.tolist()

        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())
@userLoginCheckDeco
def companyApi(request):
    """
    获取所有的公司数据
    """
    resultData=ResultData()
    try:
        resultData = ResultData()
        logger.debug("getCompanyData request:"%request.GET)
        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        if  doMethod=="getCompanyData":
            userInfoPdData = companyMode().getCompanyData()
            userInfoPdData = userInfoPdData.fillna('')
            rtdata = userInfoPdData.rename(columns=lambda x: x.capitalize()).to_dict(orient='records')  # 效率高
            resultData.data=rtdata
            return JsonResponse(resultData.toDict())
        elif  doMethod=="addCompany":
            #companyCode,companyName,shortName
            companyCode=reqData.get("companyCode")
            companyName=reqData.get("companyName")
            shortName=reqData.get("shortName")
            if companyCode is None or str(companyCode).strip()=="":
                resultData.errorData(errorMsg='公司代码不能为空！')
                return JsonResponse(resultData.toDict())

            if companyName is None or str(companyName).strip()=="":
                resultData.errorData(errorMsg='公司全称不能为空！')
                return JsonResponse(resultData.toDict())

            if shortName is None or str(shortName).strip()=="":
                resultData.errorData(errorMsg='公司简称不能为空！')
                return JsonResponse(resultData.toDict())

            ck1 = tools_utils.charLength(companyCode, 20, checkObj="公司代码 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())

            ck1 = tools_utils.charLength(companyName, 100, checkObj="公司全称 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())

            ck1 = tools_utils.charLength(shortName, 50, checkObj="公司简 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())

            resultData = companyMode().addCompany(companyCode,companyName,shortName)
            return JsonResponse(resultData.toDict())
        elif  doMethod=="editCompany":
            #companyCode,companyName,shortName
            companyCode=reqData.get("companyCode")
            companyName=reqData.get("companyName")
            shortName=reqData.get("shortName")
            if companyCode is None or str(companyCode).strip()=="":
                resultData.errorData(errorMsg='公司代码不能为空！')
                return JsonResponse(resultData.toDict())

            if companyName is None or str(companyName).strip()=="":
                resultData.errorData(errorMsg='公司全称不能为空！')
                return JsonResponse(resultData.toDict())

            if shortName is None or str(shortName).strip()=="":
                resultData.errorData(errorMsg='公司简称不能为空！')
                return JsonResponse(resultData.toDict())

            ck1 = tools_utils.charLength(companyCode, 20, checkObj="公司代码 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())

            ck1 = tools_utils.charLength(companyName, 100, checkObj="公司全称 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())

            ck1 = tools_utils.charLength(shortName, 50, checkObj="公司简 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())
            resultData = companyMode().editCompany(companyCode,companyName,shortName)
            return JsonResponse(resultData.toDict())
        elif  doMethod=="delCompany":
            #companyCode,companyName,shortName
            companyCode=reqData.get("companyCode")
            if companyCode is None or str(companyCode).strip()=="":
                resultData.errorData(errorMsg='公司代码不能为空！')
                return JsonResponse(resultData.toDict())
            ck1 = tools_utils.charLength(companyCode, 20, checkObj="公司代码 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())
            resultData = companyMode().delCompany(companyCode)
            return JsonResponse(resultData.toDict())
        elif  doMethod=="searchCompany":
            #companyCode,companyName,shortName
            Name=reqData.get("searchName")
            if Name is None or str(Name).strip()=="":
                resultData.errorData(errorMsg='搜索名字不能为空！')
                return JsonResponse(resultData.toDict())
            ck1 = tools_utils.charLength(Name, 100, checkObj="公司全称 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())
            userInfoPdData = companyMode().searchCompany(Name=Name)
            userInfoPdData = userInfoPdData.fillna('')
            rtdata = userInfoPdData.rename(columns=lambda x: x.capitalize()).to_dict(orient='records')  # 效率高
            resultData.data = rtdata
            return JsonResponse(resultData.toDict())
        elif  doMethod=="seachCompanyByCode":
            #companyCode,companyName,shortName
            companyCode=reqData.get("companyCode")
            if companyCode is None or str(companyCode).strip()=="":
                resultData.errorData(errorMsg='公司代码不能为空！')
                return JsonResponse(resultData.toDict())
            ck1 = tools_utils.charLength(companyCode, 20, checkObj="公司代码 ")
            if not ck1.errorFlag:
                return JsonResponse(ck1.toDict())
            userInfoPdData = companyMode().getCompanyByCode(companyCode)
            userInfoPdData = userInfoPdData.fillna('')
            rtdata = userInfoPdData.rename(columns=lambda x: x.capitalize()).to_dict(orient='records')  # 效率高
            resultData.data = rtdata
            return JsonResponse(resultData.toDict())
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())

@userLoginCheckDeco
def getAllRoleData(request):
    """
    获取所有的角色信息数据
    """
    try:
        resultData = ResultData()
        if settings.PRIVROLELIST==[]:
            settings.URLLIST = user_util.getAllUrlList()
            settings.PRIVROLELIST = user_util.getAllPrivRoleList()            
        
        menuInfoDF = user_util.getAllMenuInfo()
        menuInfoData = menuInfoDF.values.tolist()


        roleInfoData = settings.PRIVROLELIST
        newroleInfoData = []
        for i in range(len(roleInfoData)):
            newroleInfoData.append([roleInfoData[i][0],roleInfoData[i][1],roleInfoData[i][2].replace("'",'').split(',')])
        roleDF = pd.DataFrame(newroleInfoData)
        roleDF.columns = ['ROLEID', 'ROLENAME','ROLEPRIVIDLIST']

                                
        newroleInfoData2 = []                        
        roleList = roleDF.values.tolist()
        for role in roleList:            
            newroleInfoData = {}
            roles = role[2]
            for roleid in roles:
                for menu in menuInfoData:
                    print(menu[0],roleid,menu[5])
                    if menu[0] == roleid:
                        
                        if menu[5] not in newroleInfoData:
                            newroleInfoData[menu[5]] = []
                        if roleid not in newroleInfoData[menu[5]]:
                            newroleInfoData[menu[5]].append(roleid)
                        break
            
            newroleInfoData2.append({'ROLEID':role[0],'ROLENAME':role[1],'ROLEPRIVIDLIST':newroleInfoData})
        
        #resultData.list = json.loads(roleDF.to_json(orient='records'))
        resultData.list = newroleInfoData2
        resultData.recordsCount = len(roleDF)
        resultData.roleInfoData1 = user_util.getAllMenuInfo2()

        

        resultData.menuInfoData = menuInfoData
        resultData.roleInfoData = roleInfoData
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def getUserRole(request):
    """
    获取用户角色信息数据
    """
    try:
        resultData = ResultData()
        getUserAccess = user_utils.getUserAccess(request)
        UserNickName = request.session.get("usernickname")
        resultData.data = {"UserAccess":getUserAccess,"UserNickName":UserNickName}
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())

@userLoginCheckDeco
def addUser(request):
    """
    添加用户
    """
    try:
        resultData = ResultData()
        email = request.POST.get('email', None)
        userRealName = request.POST.get('userRealName', None)
        userNickName = request.POST.get('userNickName', None)
        companyId = request.POST.get('companyId', None)
        # companyId = request.POST.get('companyId',None)
        # groupName = request.POST.get('groupName',None)
        roleId = request.POST.get('roleId', None)
        postEmail = request.POST.get('postEmail', None)
        if email is None:
            jsonRequest = json.loads(request.body)
            email = jsonRequest['email']
            userRealName = jsonRequest['userRealName']
            userNickName = jsonRequest['userNickName']
            companyId = jsonRequest['companyId']
            roleId = jsonRequest['roleId']
            if "postEmail" in jsonRequest:#选填参数
                postEmail = jsonRequest['postEmail']
        #print("email:", email)
        #print("userRealName:", userRealName)
        #print("userNickName:", userNickName)
        #print("roleId:", roleId)
        if email is None or userRealName is None or userNickName is None or roleId is None:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(email) == 0 or len(userRealName) == 0 or len(userNickName) == 0 or len(roleId) == 0:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if companyId is None or companyId=="":
            resultData.errorData(errorMsg='请选择公司！')
            return JsonResponse(resultData.toDict())
        check = manager_user_mode.checkEmailExists(email)
        if check is True:
            resultData.errorData(errorMsg='添加失败，用户名已经存在！')
            resultData.errorCode = '0000005'
            return JsonResponse(resultData.toDict())
        # user
        encryptPwd, pwd = user_util.createPwd(8)  # 创建密码
        newUserId = manager_user_mode.addUser(email, userRealName, userNickName, encryptPwd,roleId)
        if newUserId is None:
            resultData.errorData(errorMsg='程序出错！')
            return JsonResponse(resultData.toDict())
        #company
        check = companyMode().setCompanyUser(newUserId,companyId)
        if check is False:
            resultData.errorData(errorMsg='用户关联公司程序出错！')
            return JsonResponse(resultData.toDict())
        # role
        # check = manager_user_mode.setUserRole(newUserId, roleId)
        # if check is False:
        #     resultData.errorData(errorMsg='程序出错！')
        #     return JsonResponse(resultData.toDict())
        # group
        # groupName = str(groupName).strip().replace(' ','')
        # if groupName != '':
        #     check = manager_user_mode.setUserGroup(groupName,newUserId)
        #     if check is False:
        #         resultData.errorData(errorMsg='程序出错！')
        #         return JsonResponse(resultData.toDict())
        resultData.username = email
        resultData.password = pwd
        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()     
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        if str(postEmail)=="1":
            threading.Thread(target=acountSendEmail,args=(email,pwd,)).start()
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())

def acountSendEmail(email,pwd):
    resultData=ResultData()
    mail_host = settings.EMAIL_HOST
    send_addr = settings.EMAIL_SEND_ADDR
    send_pwd = settings.EMAIL_SEND_PWD
    to_addr = email
    text = f"""
                <p>尊敬的用户 您好，您的试用账号已开通</p>
                <p>您的试用账号已开通。以下是您的登录信息：</p>
                <p> 登录网址: <a href="https://www.intellistock.cn/#/user/login">https://www.intellistock.cn/#/user/login</a></p>
                <p>用户名: {email}</p>
                <p>密码: {pwd}</p>
                <p>您可以点击上方链接直接访问登录页面。使用提供的用户名和密码进行登录。</p>
                <p>祝您使用愉快！如果您有其他问题，请随时提问。</p>
                """
    message = MIMEMultipart()
    html = MIMEText(text, 'html', 'utf-8')
    message['From'] = send_addr
    message['To'] = to_addr
    subject = "Intelli Stock:试用账号开通成功"  # 主题
    message['Subject'] = Header(subject, 'utf-8')
    message.attach(html)
    rtdata = {"msg": ""}
    try:
        smtp = smtplib.SMTP_SSL(mail_host, 465)
        smtp.login(send_addr, send_pwd)
        smtp.sendmail(send_addr, to_addr, message.as_string())
        resultData.errorMsg = "邮件发送成功"
        rtdata['msg'] = "邮件发送成功"
        resultData.data = rtdata
        smtp.quit()
    except Exception as e:
        rtdata['msg'] = "Error: 无法发送邮件"
        resultData.data = rtdata
        resultData.errorMsg = "Error: 无法发送邮件" + str(e)
    logger.info(resultData.toDict())
    return resultData

@userLoginCheckDeco
def restUserPwd(request):
    """
    重置用户密码
    """
    try:
        resultData = ResultData()
        userId = request.POST.get('userId', None)
        newPwd = request.POST.get('newPwd', None)
        newPwdAgain = request.POST.get('newPwdAgain', None)
        if userId is None:
            jsonRequest = json.loads(request.body)
            userId = jsonRequest['userId']
            newPwd = jsonRequest['newPwd']
            newPwdAgain = jsonRequest['newPwdAgain']

        if tools_utils.isNull(userId) or tools_utils.isNull(newPwd) or tools_utils.isNull(newPwdAgain):
            resultData.errorData(errorMsg='请选择正确参数！')
            return JsonResponse(resultData.toDict())

        # 密码最少8~10位
        if len(newPwd) < 8:
            resultData.errorData(errorMsg='密码长度至少8位！')
            return JsonResponse(resultData.toDict())

        if len(newPwd) > 25:
            resultData.errorData(errorMsg='密码长度至少大于8位小于25位！')
            return JsonResponse(resultData.toDict())

        if len(newPwd) != len(newPwdAgain):
            resultData.errorData(errorMsg='两次密码输入不一致！')
            return JsonResponse(resultData.toDict())

        resultData = user_util.restPwd(userId, newPwd)

        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()     
        
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='抱歉，重置登录密码失败，请稍后重试！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def stopUser(request):
    """
    停用用户
    """
    try:
        resultData = ResultData()
        userId = request.POST.get('userId', None)
        if userId is None:
            try:
                jsonRequest = json.loads(request.body)
                userId = jsonRequest['userId']
            except Exception as err:
                userId = None
        if userId is None:
            resultData.errorData(errorMsg='停用用户的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(userId) == 0:
            resultData.errorData(errorMsg='停用用户的接口传参有误！')
            return JsonResponse(resultData.toDict())
        check = manager_user_mode.stopUser(userId)
        if check is False:
            resultData.errorData(errorMsg='停用失败！')
            return JsonResponse(resultData.toDict())
        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()     
        
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='停用用户的接口出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def startUser(request):
    """
    启用用户
    """
    try:
        resultData = ResultData()
        userId = request.POST.get('userId', None)
        if userId is None:
            try:
                jsonRequest = json.loads(request.body)
                userId = jsonRequest['userId']
            except Exception as err:
                userId = None
        if userId is None:
            resultData.errorData(errorMsg='启用用户的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(userId) == 0:
            resultData.errorData(errorMsg='启用用户的接口传参有误！')
            return JsonResponse(resultData.toDict())
        check = manager_user_mode.startUser(userId)
        if check is False:
            resultData.errorData(errorMsg='启用失败！')
            return JsonResponse(resultData.toDict())
        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()     
        
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='启用用户接口出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def editeUserRole(request):
    """
    修改用户权限
    """
    try:
        resultData = ResultData()
        editUserId = request.POST.get('editUserId', None)
        roleId = request.POST.get('roleId', None)
        editCompanyId = request.POST.get('editCompanyId', None)
        
        if editUserId is None:            
            jsonRequest = json.loads(request.body)
            try:
                editUserId = jsonRequest['editUserId']
            except Exception as err:
                editUserId = None
            try:
                roleId = jsonRequest['roleId']
            except Exception as err:
                roleId = None
            try:
                editCompanyId = jsonRequest['editCompanyId']
            except Exception as err:
                editCompanyId = None

        if tools_utils.isNull(editUserId) or tools_utils.isNull(roleId):
            resultData.errorData(errorMsg='接口参数错误，请重新输入！')
            return JsonResponse(resultData.toDict())
        # 权限检查，
        userRoleIdList = roleId.split(',')

        allPrivRoleDF = manager_menu_mode.getAllPrivRoleInfo()
        roleIdList = allPrivRoleDF['ROLEID'].values.tolist()
        for userRoleId in userRoleIdList:
            if userRoleId not in roleIdList:
                resultData.errorData(errorMsg='角色编号错误，请重新选中！')
                return JsonResponse(resultData.toDict())

        # 修改用户权限。
        isTrue = manager_user_mode.changeUserRole(editUserId, roleId,editCompanyId)
        if not isTrue:
            resultData.errorData(errorMsg="抱歉，修改用户角色失败，请稍后重试！")

        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()     
        
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        print("resultData:", resultData.toDict())
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='修改用户角色接口出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def flushUserCache(request):
    """
    刷新用户缓存
    """
    try:
        resultData = ResultData()
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        #add By Rocky 20190321
        #添加修改全局变量URLLIST与PRIVROLELIST的代码
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()

        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='刷新用户缓存接口出错！')
        return JsonResponse(resultData.toDict())


if __name__ == '__main__':
    str(None)