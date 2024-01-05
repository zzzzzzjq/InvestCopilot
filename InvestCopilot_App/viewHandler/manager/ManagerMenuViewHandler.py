from django.conf import settings
import pandas as pd
from django.http import JsonResponse, HttpResponseRedirect
from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.user.UserInfoUtil as user_util
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.manager import ManagerMenuMode as menu_mode
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
import collections
import json

from django.utils.safestring import mark_safe

logger = logger_utils.LoggerUtils()


@userLoginCheckDeco
@userMenuPrivCheckDeco('角色管理')
def managerMenuPage(request):
    """
    菜单权限管理页面
    """
    resultData = ResultData()
    roleId = request.POST.get('roleId', '')
    ############获取所有角色信息############
    allPrivRolePdData = menu_mode.getAllPrivRoleInfo()
    privRolePdData = allPrivRolePdData[['ROLEID', 'ROLENAME']]
    keyList = privRolePdData.columns.values.tolist()
    keyList = [str(item).lower() for item in keyList]
    valueList = privRolePdData.values.tolist()
    privRoleList = []
    for value in valueList:
        tmpDict = dict(zip(keyList, value))
        privRoleList.append(tmpDict)
    resultData.privRoleList = privRoleList

    ###########获取角色的菜单信息############
    if roleId == '':
        roleId = privRoleList[0]['roleid']
    # 获取所有菜单的信息
    allMenuInfoDict = collections.OrderedDict()
    allMenuInfo = menu_mode.getAllMenuInfo()
    for parentOrderId, tmpPdData in allMenuInfo.groupby('PARENTORDERID'):
        tmpPdData = tmpPdData.reset_index(drop=True)
        parentMenuName = ''
        tmpList = []
        for index, row in tmpPdData.iterrows():
            if index == 0:
                parentMenuName = row.PARENTMENUNAME
            tmpDict = {'menuId': row.MENUID, 'menuName': row.MENUNAME + ' [' + row.MENUURL + ']',
                       'order': row.SUBORDERID, 'isHave': '0'}
            tmpList.append(tmpDict)
        allMenuInfoDict[parentMenuName] = tmpList
    # 将特定角色的菜单在所有菜单信息中做好标记 以便区分 并添加到新的列表中
    privRoleInfoPdData = menu_mode.getPrivRoleInfoByRoleId(roleId)
    urserAllMenus = privRoleInfoPdData['MENUIDLIST'].values.tolist()    
    menuIdStr = urserAllMenus[0]
    menuInfoPdData = menu_mode.getMenuInfoByMenuId(menuIdStr)
    for parentMenuName, tmpPdData in menuInfoPdData.groupby('PARENTORDERID'):
        tmpPdData = tmpPdData.reset_index(drop=True)
        parentMenuName = ''
        for index, row in tmpPdData.iterrows():
            if index == 0:
                parentMenuName = row.PARENTMENUNAME
            tmpDict = {'menuId': row.MENUID, 'menuName': row.MENUNAME + ' [' + row.MENUURL + ']',
                       'order': row.SUBORDERID, 'isHave': '0'}
            # 判断是否在所有菜单内 在或不在都打一个标记
            allSubMenuList = allMenuInfoDict[parentMenuName]
            if tmpDict in allSubMenuList:
                listIndex = allSubMenuList.index(tmpDict)
                allSubMenuList[listIndex] = {'menuId': row.MENUID, 'menuName': row.MENUNAME + ' [' + row.MENUURL + ']',
                                             'order': row.SUBORDERID, 'isHave': '1'}
    privRoleMenuList = []
    for k, v in allMenuInfoDict.items():
        privRoleMenuList.append({k: v})

    resultData.privRoleMenuList = privRoleMenuList
    resultData.roleId = roleId
    returnPage = 'manager/managemenu.html'
    resultData.topMenu = '后台维护'
    resultData.subMenu = '角色管理'
    return resultData.buildRender(request, returnPage)


# 获取用户基金权限
@userLoginCheckDeco
def getUserFundRole(request):
    try:
        resultData = ResultData()
        userId = request.POST.get('userId', '')
        import FEA_app.user.UserInfoUtil as user_util
        ###########获取用户的基金权限############
        from FEA_app.toolsutils import dbutils as dbutils
        q_funds = "select fundcode as fcode ,fundname as fname,'' as fjjlb , c.fundmanager, c. department , c.begindate, c.enddate , c.startmanagedate ,c.endmanagedate \
              from FUNDNAMEDICT t left join (select 组合代码, to_char(wm_concat(投资经理)) as fundmanager , to_char(wm_concat(所属部门)) as department ,\
              min(成立时间) as begindate, decode(max(清盘时间),'20991231','',max(清盘时间)) as enddate ,  to_char(wm_concat(开始管理时间)) as startmanagedate , to_char(wm_concat(结束管理时间)) as endmanagedate \
              from fundmanager2 group by 组合代码) c \
              on t.fundcode = c.组合代码  where t.fundcode in(select distinct bk_product from PRODUCT_NETVALINCRATIO_COMP ) order by fcode "
        con = dbutils.getDBConnect()
        allFundsDF = pd.read_sql(q_funds, con)
        allFundsDF['FNAME'] = "(" + allFundsDF['FCODE'] + ")" + allFundsDF['FNAME']
        # allFundsDF = allFundsDF.sort_values(['FJJLB'])
        # allFundsDF['FJJLB'] = allFundsDF['FJJLB'].apply(lambda x: fundType(str(x)))
        q_fundsByUserId = "Select DISTINCT t.fundcode as fundcode From  Menufundpriv t where t.userid=:userId"
        userFundsDF = pd.read_sql(q_fundsByUserId, con, params={'userId': userId})
        con.close()
        userFundsList = userFundsDF['FUNDCODE'].values.tolist()
        allFundsDF['ISHAVE'] = allFundsDF['FCODE'].apply(lambda x: '启用' if x in userFundsList else '停用')
        qyFcode = allFundsDF[allFundsDF['ISHAVE'] == '启用']['FCODE']
        qyFcodeList = qyFcode.values.tolist()
        allFundsDF = allFundsDF.fillna('')
        resultData.tbColumns = ['<input id="select_all" type="hidden" name="simple_select_all"  value="1">选择',
                                "基金名称", "基金类别","基金经理","部门","成立时间","清盘时间","开始管理时间","结束管理时间","启用标志"]  # allFundsDF.columns.values.tolist()
        resultData.qyFcodeList = qyFcodeList
        resultData.tbData = allFundsDF.values.tolist()
        resultData.chooseUserId = userId
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='抱歉，获取用户基金权限失败，请稍后重试！')
        return JsonResponse(resultData.toDict())


# 修改用户基金权限
@userLoginCheckDeco
def modifyUserFundRole(request):
    try:
        resultData = ResultData()
        userId = request.POST.get('userId', '')
        fcode = request.POST.get('fcode', '')
        operaterFlag = request.POST.get('operaterFlag', '')
        import FEA_app.user.UserInfoUtil as user_util
        ###########获取用户的基金权限############
        from FEA_app.toolsutils import dbutils as dbutils
        q_funds = "select fundcode as  fcode ,fundname as fname  from FUNDNAMEDICT t where t.fundcode in(select distinct bk_product from PRODUCT_NETVALINCRATIO_COMP) order by fcode "
        con, cur = dbutils.getConnect()
        allFundsDF = pd.read_sql(q_funds, con)
        allFundsList = allFundsDF['FCODE'].values.tolist()
        if not fcode in allFundsList:
            resultData.errorData(errorMsg='基金代码[%s]不存在，请重试选择!' % (fcode))
            return JsonResponse(resultData.toDict())

        q_fundsByUserId = "Select DISTINCT t.fundcode as fundcode From  Menufundpriv t where t.userid=:userId"
        userFundsDF = pd.read_sql(q_fundsByUserId, con, params={'userId': userId})
        userFundsList = userFundsDF['FUNDCODE'].values.tolist()
        if fcode in userFundsList:
            if 'remove' == operaterFlag:  # 删除
                d_fundsByUserId = "delete from Menufundpriv t where t.userid=:userId and t.fundcode=:fcode"
                cur.execute(d_fundsByUserId, [userId, fcode])
                con.commit()
        else:
            if 'add' == operaterFlag:  # 添加
                i_funds = "insert into Menufundpriv (userid,fundcode,inserttime) values(:userId,:fcode,sysdate)"
                cur.execute(i_funds, [userId, fcode])
                con.commit()

        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='抱歉，添加用户基金权限失败，请稍后重试！')
        return JsonResponse(resultData.toDict())
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass


# 删除用户基金权限
@userLoginCheckDeco
def delUserFundRole(request):
    try:
        resultData = ResultData()
        userId = request.POST.get('userId', '')
        import FEA_app.user.UserInfoUtil as user_util
        from FEA_app.toolsutils import dbutils as dbutils
        con, cur = dbutils.getConnect()
        # 删除
        d_fundsByUserId = "delete from Menufundpriv t where t.userid=:userId"
        cur.execute(d_fundsByUserId, [userId])
        con.commit()
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='抱歉，删除用户基金权限失败，请稍后重试！')
        return JsonResponse(resultData.toDict())
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass


@userLoginCheckDeco
def managerFundRolePage(request):
    try:
        """
        基金数据访问权限管理页面
        """
        resultData = ResultData()
        ############获取所有角色信息############
        import FEA_app.user.UserInfoUtil as user_util
        allUserDF = user_util.getAllUser()
        allUserDF = allUserDF[['USERID', 'USERREALNAME']]
        allUserDF = allUserDF.fillna('')
        allUserList = []
        for row in allUserDF.itertuples():
            allUserList.append({'userId': row.USERID, 'userName': row.USERREALNAME})

        resultData.allUserList = allUserList
        ###########获取用户的基金权限############
        userId = ''
        if len(allUserList) > 0:
            userId = allUserDF['USERID'].values.tolist()[0]

        """
        基金主要负责人管理维护页面
        """        
        userDF = user_util.getAllUser()
        userList = []
        for user in userDF.itertuples():
            userId = user.USERID
            userRealName = user.USERREALNAME
            userList.append({"userId": userId, "userName": userRealName})

        resultData.userList = userList

        ##################基金列表##################
        # 权益类
        selectFund = mark_safe(
            fundposition.getFundClassMenu())
        resultData.selectFund = selectFund

        ##################公司部门##################
        departmentList = ["公司领导", "研究发展部", "养老金部", "国际业务部", "专户投资部", "量化投资部", "机构理财部", "固定收益管理总部", "指数投资部", "伦敦子公司", "宏观策略部",
            "成长投资部", "权益投资一部", "瑞元子公司", "价值投资部", "资产配置部", "策略投资部", "香港子公司", "固定收益部"]
        resultData.departmentList = departmentList

        #责任人级别
        managementTypeList=['第一负责人','第二负责人','第三负责人',]
        resultData.managementTypeList = managementTypeList
        #returnPage = 'manager/fundmanager.html'
        #return resultData.buildRender(request, returnPage)
        returnPage = 'smartset/manager/managerfundrole.html'
        resultData.activeUserId = userId
        resultData.topMenu = '后台维护'
        resultData.subMenu = '角色管理'
        return resultData.buildRender(request, returnPage)
    except:
        resultData.errorMsg = "抱歉，加载用户基金权限管理维护页面失败，请稍后重试！"
        return resultData.buildErrorRender(request)

    


@userLoginCheckDeco
def amendMenuPriv(request):
    """
    修改菜单权限的接口
    """
    try:
        resultData = ResultData()
        paraType ='POST'
        roleId = request.POST.get('roleId', None)
        subMenuId = request.POST.get('subMenuId', None)
        operate = request.POST.get('operate', None)
        if roleId is None :
            jsonRequest = json.loads(request.body.decode('utf-8'))
            if 'roleId' in jsonRequest:
                roleId = jsonRequest['roleId']
            if 'subMenuId' in jsonRequest:
                subMenuId = jsonRequest['subMenuId']
            if 'operate' in jsonRequest:
                operate = jsonRequest['operate']
            if roleId is not None:
                paraType = 'JSON'
        if roleId is None or subMenuId is None or operate is None:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(roleId) == 0 or len(subMenuId) == 0 or len(operate) == 0:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())

        #20231201 按新的逻辑修改，前端传入的是角色的所有菜单ID，直接更新即可
        if paraType == 'JSON':
            #直接解析传入的菜单ID，更新到数据库
            newMenuidlist = subMenuId.replace("'",'').split(',')
            allSubMenuid = menu_mode.getAllMenuInfo()['MENUID'].values.tolist()
            newMenuidlist2 =[]
            for i in range(len(newMenuidlist)):
                if newMenuidlist[i]  in allSubMenuid:
                    newMenuidlist2.append(newMenuidlist[i])                    
            newMenuidlist = list(map(lambda x: "'"+x+"'", newMenuidlist2))
            newMenuidstr = ','.join(newMenuidlist)
            newMenuidstr ="'nomenu',"+newMenuidstr
            menu_mode.amendPrivMenu(roleId, newMenuidstr)

        elif paraType == 'POST':
        
            privInfoPdData = menu_mode.getPrivRoleInfoByRoleId(roleId)
            if len(privInfoPdData) == 0:
                resultData.errorData(errorMsg='修改失败，角色不存在！')
                resultData.errorCode = '0000005'
                return JsonResponse(resultData.toDict())
            subMenuIdStr = privInfoPdData['MENUIDLIST'].values.tolist()[0]
            subMenuIdList = subMenuIdStr.split(',')
            allSendSubMenuIdList = subMenuId.split(',')

            #subMenuId = "'" + subMenuId + "'"
            if operate == 'add':
                for tmpSubMenuId in allSendSubMenuIdList:
                    tempSubMenuId = "'" + tmpSubMenuId + "'"
                    if tmpSubMenuId not in subMenuIdList:
                        subMenuIdList.append(tmpSubMenuId)
            elif operate == 'del':
                for tmpSubMenuId in allSendSubMenuIdList:
                    tempSubMenuId = "'" + tmpSubMenuId + "'"
                    if tmpSubMenuId in subMenuIdList:
                        if len(subMenuIdList) == 1:
                            resultData.errorData(errorMsg='修改失败，角色必须至少配置一个菜单！')
                            resultData.errorCode = '0000005'
                            return JsonResponse(resultData.toDict())
                        subMenuIdList.remove(tmpSubMenuId)            
            subMenuIdFinalStr = ','.join(subMenuIdList)
            menu_mode.amendPrivMenu(roleId, subMenuIdFinalStr)
        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()            
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def CorrectionMenuPriv(request):
    resultData = ResultData()
    menu_mode.CorrectionMenuPriv()
    return JsonResponse(ResultData().toDict())    

@userLoginCheckDeco
def addMenuPriv(request):
    """
    添加菜单权限
    """
    try:
        resultData = ResultData()
        roleName = request.POST.get("roleName", None)
        if roleName is None:
            jsonRequest = json.loads(request.body.decode('utf-8'))
            if 'roleName' in jsonRequest:
                roleName = jsonRequest['roleName']

        if roleName is None:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(roleName) == 0:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        allPrivRoleInfoPdData = menu_mode.getAllPrivRoleInfo()
        privRoleNameList = allPrivRoleInfoPdData['ROLENAME'].values.tolist()
        if roleName in privRoleNameList:
            resultData.errorData(errorMsg='添加失败，角色名字已重复！')
            resultData.errorCode = '0000005'
            return JsonResponse(resultData.toDict())
        roleId = menu_mode.addPrivRole(roleName)
        resultData.roleId = roleId
        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()     
        
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def removeMenuPriv(request):
    """
    删除菜单权限
    """
    try:
        rs = ResultData()
        roleId = request.POST.get("roleId", None)
        if roleId is None:
            jsonRequest = json.loads(request.body.decode('utf-8'))
            if 'roleId' in jsonRequest:
                roleId = jsonRequest['roleId']
        if tools_utils.isNull(roleId):
            rs.errorData(errorMsg='请选择需要删除的用户角色！')
            return JsonResponse(rs.toDict())
        # 判断角色是否存在
        notRole = True
        print(settings.PRIVROLELIST)
        for roleinfo in settings.PRIVROLELIST:
            if roleinfo[0] == roleId:
                notRole = False
                break
        if notRole:
            rs.errorData(errorMsg='角色不存在！')
            return JsonResponse(rs.toDict())
                
        rs = menu_mode.deletePrivRoleByRoleId(roleId)
        # 刷新缓存
        settings.URLLIST = user_util.getAllUrlList()
        settings.PRIVROLELIST = user_util.getAllPrivRoleList()     
        
        cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        for key in cacheList:
            cache_db.flushOneDbCache(key)
        return JsonResponse(rs.toDict())
    except Exception as ex:
        logger.errLineNo()
        rs.errorData(errorMsg='抱歉，删除角色失败，请稍后重试！！')
        return JsonResponse(rs.toDict())


# def fundType(fundType):
#     fundTypeDict = {'0': '权益类', '1': '发起式及分级', '4': 'ETF基金', '5': '固定收益类', '9': '企业年金', '51': 'QDII及专户', '11': '联接基金',
#                     '12': '专户', '18': '养老保险组合'}
#     return fundTypeDict.get(fundType)


if __name__ == '__main__':
    pass