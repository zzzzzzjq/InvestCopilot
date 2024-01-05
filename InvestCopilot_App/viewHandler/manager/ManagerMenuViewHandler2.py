import threading

from django.conf import settings
import pandas as pd
import json
from django.http import JsonResponse
from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco
from InvestCopilot_App.models.toolsutils import dbutils as dbutils


logger = logger_utils.LoggerUtils()


def getAllMenuInfo():
    """
    获取所有的菜单信息（纯数据)
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        '''

    data = pd.read_sql(selectSql, con=con)
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getAllMenuInfo0(queryStr):
    """
    按照模糊搜索获取所有的菜单信息（纯数据）
    """
    queryStr = '%' + queryStr + '%'
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        where menuid like %s or menuname like %s or menuurl like %s or parentmenuname like %s or 
        parentmenuicon like %s or parentorderid like %s
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        '''

    data = pd.read_sql(selectSql, con=con, params=[queryStr, queryStr, queryStr, queryStr, queryStr, queryStr])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getAllMenuInfo1(pageIndex, pageSize):
    """
    获取对应下标区间的菜单信息（纯数据）
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        limit %s offset %s
        '''

    data = pd.read_sql(selectSql, con=con, params=[pageSize, (pageIndex - 1) * pageSize])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getAllMenuInfo2(queryStr, pageIndex, pageSize):
    """
    按照模糊搜索获取下标区间的菜单信息（纯数据）
    """
    queryStr = '%' + queryStr + '%'
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.* 
        FROM menu t
        where menuid like %s or menuname like %s or menuurl like %s or parentmenuname like %s or 
        parentmenuicon like %s or parentorderid like %s
        ORDER BY t.parentorderid ASC, t.suborderid ASC
        limit %s offset %s
        '''

    data = pd.read_sql(selectSql, con=con, params=[queryStr, queryStr, queryStr, queryStr, queryStr, queryStr, pageSize, (pageIndex - 1) * pageSize])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getMenuInfoByMenuId(menuId):
    """
    通过menuId获取对应的菜单信息
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.*
        FROM menu t
        WHERE menuid = %s
        '''
    data = pd.read_sql(selectSql, con=con, params=[menuId])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getMenuInfoByMenuName(menuName):
    """
    通过menuName获取对应的菜单信息
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.*
        FROM menu t
        WHERE menuname = %s
        '''
    data = pd.read_sql(selectSql, con=con, params=[menuName])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getMenuInfoByMenuUrl(menuUrl):
    """
    通过menuUrl获取对应的菜单信息
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT t.*
        FROM menu t
        WHERE menuurl = %s
        '''
    data = pd.read_sql(selectSql, con=con, params=[menuUrl])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getSubMenuId(menuName):
    """
    获取当前菜单的一级子菜单
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            selectSql = '''
            SELECT t.menuid
            FROM menu t
            WHERE parentmenuname = %s and menuname != %s
            '''
        res = cur.execute(selectSql, [menuName, menuName])
        if settings.DBTYPE == 'postgresql':
            menuIdList = cur.fetchall()
        return menuIdList
    except:
        logger.errLineNo()
        return []
    finally:
        cur.close()
        con.close()



def getAllMenuInfoCount():
    """
    获取所有的菜单信息条数
    """
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT count(1) as count from menu t
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT count(1) as count from menu t
        '''
    data = pd.read_sql(selectSql, con=con)
    data.columns = data.columns.str.upper()
    con.close()
    return data


def getAllMenuInfoCount2(queryStr):
    """
    按照模糊搜索获取所有的菜单信息条数
    """
    queryStr = '%' + queryStr + '%'
    con = dbutils.getDBConnect()
    if settings.DBTYPE == 'Oracle':
        selectSql = '''
        SELECT count(1) as count from menu t
        '''
    elif settings.DBTYPE == 'postgresql':
        selectSql = '''
        SELECT count(1) as count from menu t
        where menuid like %s or menuname like %s or menuurl like %s or parentmenuname like %s or 
        parentmenuicon like %s or parentorderid like %s
        '''
    data = pd.read_sql(selectSql, con=con, params=[queryStr, queryStr, queryStr, queryStr, queryStr, queryStr])
    data.columns = data.columns.str.upper()
    con.close()
    return data


def checkMenuExist(menuId, menuName, menuUrl):
    """
    检查菜单是否存在
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            selectSql = '''
                SELECT COUNT(1)
                FROM menu
                WHERE menuname = %s or menuurl = %s or menuid = %s
                '''
        res = cur.execute(selectSql, [menuName, menuUrl, menuId])
        if settings.DBTYPE == 'postgresql':
            check = cur.fetchall()[0][0]
        if check >= 1:
            return True
        return False
    except:
        logger.errLineNo()
        return True
    finally:
        cur.close()
        con.close()


def checkMenuExist0(menuId):
    """
    检查菜单存在唯一性
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            selectSql = '''
                SELECT COUNT(1)
                FROM menu
                WHERE menuid = %s
                '''
        res = cur.execute(selectSql, [menuId])
        if settings.DBTYPE == 'postgresql':
            check = cur.fetchall()[0][0]
        if check != 1:
            return True
        return False
    except:
        logger.errLineNo()
        return True
    finally:
        cur.close()
        con.close()


def checkParentMenuExist(menuName, parentMenuName):
    """
    判断上级目录是否存在
    """
    if menuName == parentMenuName:
        return False
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            selectSql = '''
                SELECT COUNT(1)
                FROM menu
                WHERE menuname = %s
                '''
        res = cur.execute(selectSql, [parentMenuName])
        if settings.DBTYPE == 'postgresql':
            check = cur.fetchall()[0][0]
        if check == 1:
            return False
        return True
    except:
        logger.errLineNo()
        return True
    finally:
        cur.close()
        con.close()


def checkMenuIdExist(menuId):
    """
    检查菜单id是否存在
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            selectSql = '''
                SELECT COUNT(1)
                FROM menu
                WHERE menuid = %s
                '''
        res = cur.execute(selectSql, [menuId])
        if settings.DBTYPE == 'postgresql':
            check = cur.fetchall()[0][0]
        if check >= 1:
            return True
        return False
    except:
        logger.errLineNo()
        return True
    finally:
        cur.close()
        con.close()


def checkMenuNameExist(menuName):
    """
    检查菜单名称是否重复
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            selectSql = '''
                SELECT COUNT(1)
                FROM menu
                WHERE menuname = %s
                '''
        res = cur.execute(selectSql, [menuName])
        if settings.DBTYPE == 'postgresql':
            check = cur.fetchall()[0][0]
        if check >= 1:
            return True
        return False
    except:
        logger.errLineNo()
        return True
    finally:
        cur.close()
        con.close()


def checkMenuUrlExist(menuUrl):
    """
    检查菜单Url是否重复
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            selectSql = '''
                SELECT COUNT(1)
                FROM menu
                WHERE menuurl = %s
                '''
        res = cur.execute(selectSql, [menuUrl])
        if settings.DBTYPE == 'postgresql':
            check = cur.fetchall()[0][0]
        if check >= 1:
            return True
        return False
    except:
        logger.errLineNo()
        return True
    finally:
        cur.close()
        con.close()


def addMenuData(menuId, menuName, menuUrl, parentMenuName, parentMenuIcon, parentOrderId, subOrderId):
    """
    新增菜单
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            menu_insertSql = '''
            INSERT INTO menu(menuid,menuname,menuurl,parentmenuname,parentmenuicon,parentorderid,suborderid)
            VALUES(%s,%s,%s,%s,%s,%s,%s)
            '''
        res = cur.execute(menu_insertSql, [menuId, menuName, menuUrl, parentMenuName, parentMenuIcon, parentOrderId, subOrderId])
        con.commit()
        return True
    except:
        logger.errLineNo()
        return False
    finally:
        cur.close()
        con.close()


def deleteMenuData(menuId):
    """
    删除对应menuId的菜单
    """
    con, cur = dbutils.getConnect()
    try:
        # 获取对应的menu信息
        menuInfo = getMenuInfoByMenuId(menuId=menuId)

        subMenuIdList = getSubMenuId(menuName=menuInfo['MENUNAME'][0])
        for subMenuId in subMenuIdList:
            deleteMenuData(subMenuId[0])
        if settings.DBTYPE == 'postgresql':
            menu_insertSql = '''
            DELETE FROM menu
            WHERE menuid = %s
            '''
        res = cur.execute(menu_insertSql, [menuId])
        con.commit()
        return menuId, subMenuIdList
    except:
        logger.errLineNo()
        return None
    finally:
        cur.close()
        con.close()


def changeMenuData(menuId, menuName, menuUrl, parentMenuName, parentMenuIcon, parentOrderId, subOrderId):
    """
    修改menu表单的数据
    """
    con, cur = dbutils.getConnect()
    try:
        if settings.DBTYPE == 'postgresql':
            menu_updateSql = '''
            UPDATE menu t
            SET 
            '''
            format_list = []
            if menuName is not None:
                menu_updateSql = menu_updateSql + '''menuname= %s '''
                format_list.append(menuName)
            if menuUrl is not None:
                menu_updateSql = menu_updateSql + '''menuurl= %s '''
                format_list.append(menuUrl)
            if parentMenuName is not None:
                menu_updateSql = menu_updateSql + '''parentMenuName= %s '''
                format_list.append(parentMenuName)
            if parentMenuIcon is not None:
                menu_updateSql = menu_updateSql + '''parentmenuicon= %s '''
                format_list.append(parentMenuIcon)
            if parentOrderId is not None:
                menu_updateSql = menu_updateSql + '''parentorderid= %s '''
                format_list.append(parentOrderId)
            if subOrderId is not None:
                menu_updateSql = menu_updateSql + '''subirderid= %s '''
                format_list.append(subOrderId)
            menu_updateSql = menu_updateSql +'''
            WHERE t.menuid = %s
            '''
        format_list.append(menuId)
        res = cur.execute(menu_updateSql, format_list)
        con.commit()
        return True
    except:
        logger.errLineNo()
        return False
    finally:
        cur.close()
        con.close()

@userLoginCheckDeco
def getAllMenuData(request):
    """
    获取对应条件筛选的菜单数据
    """
    try:
        # 抓取request参数
        resultData = ResultData()
        pageIndex = request.POST.get('pageIndex', None)
        pageSize = request.POST.get('pageSize', None)
        queryStr = request.POST.get('queryStr', None)
        if pageIndex is None:
            jsonRequest = json.loads(request.body)

            pageIndex = jsonRequest.get('pageIndex')
            pageSize = jsonRequest.get('pageSize')
            queryStr = jsonRequest.get('queryStr')
        # 按照参数从数据库读取数据
        if queryStr is not None:
            if len(queryStr) == 0:
                queryStr = None
        if queryStr is not None and pageIndex is None:
            menuInfoPdData = getAllMenuInfo0(queryStr=queryStr)
            allMenuInfoCount = len(menuInfoPdData)
        if queryStr is None and pageIndex is not None:
            menuInfoPdData = getAllMenuInfo1(pageIndex=pageIndex, pageSize=pageSize)
            allMenuInfoCountDF = getAllMenuInfoCount()
            allMenuInfoCount = allMenuInfoCountDF['COUNT'].values.tolist()[0]
        if queryStr is not None and pageIndex is not None:
            menuInfoPdData = getAllMenuInfo2(pageIndex=pageIndex, pageSize=pageSize,queryStr=queryStr)
            allMenuInfoCountDF = getAllMenuInfoCount2(queryStr=queryStr)
            allMenuInfoCount = allMenuInfoCountDF['COUNT'].values.tolist()[0]
        if queryStr is None and pageIndex is None:
            menuInfoPdData = getAllMenuInfo()
            allMenuInfoCountDF = getAllMenuInfoCount()
            allMenuInfoCount = allMenuInfoCountDF['COUNT'].values.tolist()[0]
        # 格式规范化处理
        menuInfoPdData = menuInfoPdData.fillna('')
        dataLength = len(menuInfoPdData.values.tolist())
        tmpPdData = pd.DataFrame({'操作': [''] * dataLength})
        menuInfoPdData = pd.concat([menuInfoPdData, tmpPdData], axis=1)
        menuInfoPdData = menuInfoPdData[
            ['MENUID', 'MENUNAME', 'MENUURL', 'PARENTMENUNAME', 'PARENTMENUICON', 'PARENTORDERID', 'SUBORDERID','操作']]
        resultData.list = json.loads(menuInfoPdData.to_json(orient='records'))
        resultData.recordsCount = allMenuInfoCount
        menuInfoPdData = menuInfoPdData.rename(columns={'MENUID': '菜单ID',
                                                        'MENUNAME': '菜单名',
                                                        'MENUURL': '菜单URL',
                                                        'PARENTMENUNAME': '上级菜单名',
                                                        'PARENTMENUICON': '上级菜单图标',
                                                        'PARENTORDERID':'上级指令ID',
                                                        'SUBORDERID':'下级指令ID',})
        resultData.tbColumn = menuInfoPdData.columns.values.tolist()
        resultData.tbData = menuInfoPdData.values.tolist()
        return JsonResponse(resultData.toDict())

    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def getCertainMenuData(request):
    """
    获取指定参数的数据
    """
    try:
        # 抓取request参数
        resultData = ResultData()
        menuId = request.POST.get('menuId', None)
        menuName = request.POST.get('menuName', None)
        menuUrl = request.POST.get('menuUrl', None)
        if menuName is None:
            jsonRequest = json.loads(request.body)

            menuName = jsonRequest.get('menuName')
            menuId = jsonRequest.get('menuId')
            menuUrl = jsonRequest.get('menuUrl')
        # 判断必要参数的输入及输入格式判别
        if (menuId is None or len(menuId) == 0) and (menuName is None or len(menuName) == 0) and (menuUrl is None or len(menuUrl) == 0):
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        # if not len(menuId) and not len(menuName) and not len(menuUrl):
        #     resultData.errorData(errorMsg='接口传参有误！')
        #     return JsonResponse(resultData.toDict())
        if (menuId is None and menuName is not None and menuUrl is not None) or \
                (menuName is None and menuId is not None and menuUrl is not None) or \
                (menuUrl is None and menuId is not None and menuName is not None):
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        # 获取信息
        if menuId is not None:
            targetMenu = getMenuInfoByMenuId(menuId=menuId)
        if menuName is not None:
            targetMenu = getMenuInfoByMenuName(menuName=menuName)
        if menuUrl is not None:
            targetMenu = getMenuInfoByMenuUrl(menuUrl=menuUrl)
        resultData.targetMenu = targetMenu.to_dict()
        return JsonResponse(resultData.toDict())

    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def addMenu(request):
    """
    添加菜单
    """
    try:
        # 抓取request参数
        resultData = ResultData()
        menuId = request.POST.get('menuId', None)
        menuName = request.POST.get('menuName', None)
        menuUrl = request.POST.get('menuUrl', None)
        parentMenuName = request.POST.get('parentMenuName', None)
        parentMenuIcon = request.POST.get('parentMenuIcon', None)
        parentOrderId = request.POST.get('parentOrderId', None)
        subOrderId = request.POST.get('subOrderId', None)
        if menuName is None:
            jsonRequest = json.loads(request.body)

            menuName = jsonRequest.get('menuName')
            menuId = jsonRequest.get('menuId')
            menuUrl = jsonRequest.get('menuUrl')
            parentMenuName = jsonRequest.get('parentMenuName')
            parentMenuIcon = jsonRequest.get('parentMenuIcon')
            parentOrderId = jsonRequest.get('parentOrderId')
            subOrderId = jsonRequest.get('subOrderId')
        # 判断必要参数的输入及输入格式判别
        if menuId is None or menuName is None or menuUrl is None or parentMenuName is None or parentMenuIcon is None or parentOrderId is None or subOrderId is None:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if not len(menuId) or not len(menuName) or not len(menuUrl) or not len(parentMenuName) or not len(parentMenuIcon) or not len(parentOrderId) or not isinstance(subOrderId, int):
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        # 内容要求检查
        check = checkMenuExist(menuId=menuId, menuName=menuName, menuUrl=menuUrl)
        if check is True:
            resultData.errorData(errorMsg='添加失败，该id或菜单名或URL已经存在！')
            return JsonResponse(resultData.toDict())
        check = checkParentMenuExist(menuName, parentMenuName)
        if check is True:
            resultData.errorData(errorMsg='添加失败，上级菜单不存在！')
            return JsonResponse(resultData.toDict())
        # 插入表单
        insertStatus = addMenuData(menuId=menuId, menuName=menuName, menuUrl=menuUrl, parentMenuName=parentMenuName, parentMenuIcon=parentMenuIcon, parentOrderId=parentOrderId, subOrderId=subOrderId)
        resultData.insertStatus = insertStatus
        resultData.insertMenuData = {
            'menuId': menuId,
            'menuName': menuName,
            'menuUrl': menuUrl,
            'parentMenuName': parentMenuName,
            'parentMenuIcon': parentMenuIcon,
            'parentOrderId': parentOrderId,
            'subOrderId': subOrderId,
        }
        # 刷新缓存
        # settings.URLLIST = user_util.getAllUrlList()
        # settings.PRIVROLELIST = user_util.getAllPrivRoleList()
        # cacheList = ['SQL_userStatusDF', 'SQL_userMenuIdDF', 'SQL_menuDF', 'SQL_userConfig_dt']
        # for key in cacheList:
        #     cache_db.flushOneDbCache(key)
        return JsonResponse(resultData.toDict())

    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def deleteMenu(request):
    """
    删除菜单
    """
    try:
        # 抓取request参数
        resultData = ResultData()
        menuId = request.POST.get('menuId', None)
        # menuName = request.POST.get('menuName', None)
        # menuUrl = request.POST.get('menuUrl', None)
        if menuId is None:
            jsonRequest = json.loads(request.body)

            menuId = jsonRequest.get('menuId')
            # menuName = jsonRequest.get('menuName')
            # menuUrl = jsonRequest.get('menuUrl')
        # 判断输入参数正确性
        if menuId is None or len(menuId) == 0:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if checkMenuExist0(menuId=menuId):
            resultData.errorData(errorMsg='目标菜单不存在！')
            return JsonResponse(resultData.toDict())
        # 删除表项并处理关联信息
        deleteMenuId, deleteSubMenuId = deleteMenuData(menuId=menuId)

        resultData.DeleteMenuId = deleteMenuId
        resultData.deleteSubMenuId = deleteSubMenuId
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
def changeMenu(request):
    """
    修改菜单信息
    """
    try:
        # 抓取request参数
        resultData = ResultData()
        # 抓取request参数
        resultData = ResultData()
        menuId = request.POST.get('menuId', None)
        menuName = request.POST.get('menuName', None)
        menuUrl = request.POST.get('menuUrl', None)
        parentMenuName = request.POST.get('parentMenuName', None)
        parentMenuIcon = request.POST.get('parentMenuIcon', None)
        parentOrderId = request.POST.get('parentOrderId', None)
        subOrderId = request.POST.get('subOrderId', None)
        if menuId is None:
            jsonRequest = json.loads(request.body)

            menuName = jsonRequest.get('menuName')
            menuId = jsonRequest.get('menuId')
            menuUrl = jsonRequest.get('menuUrl')
            parentMenuName = jsonRequest.get('parentMenuName')
            parentMenuIcon = jsonRequest.get('parentMenuIcon')
            parentOrderId = jsonRequest.get('parentOrderId')
            subOrderId = jsonRequest.get('subOrderId')
        # 检查数据输入正确性
        if (menuId is None or len(menuId) == 0) or not checkMenuIdExist(menuId):
            resultData.errorData(errorMsg='接口参数错误！')
            return JsonResponse(resultData.toDict())

        if (menuName is None or len(menuName) == 0) and (menuUrl is None or len(menuUrl) == 0) and (parentMenuName is None or len(parentMenuName) == 0) and\
                (parentMenuIcon is None or len(parentMenuIcon) == 0) and (parentOrderId is None or len(parentOrderId) == 0) and (subOrderId is None or not isinstance(subOrderId, int)):
            resultData.errorData(errorMsg='接口参数错误！')
            return JsonResponse(resultData.toDict())
        if menuName is not None and checkMenuNameExist(menuName):
            resultData.errorData(errorMsg='修改内容重复，不符合要求！')
            return JsonResponse(resultData.toDict())
        if menuUrl is not None and checkMenuUrlExist(menuUrl):
            resultData.errorData(errorMsg='修改内容重复，不符合要求！')
            return JsonResponse(resultData.toDict())
        if parentMenuName is not None:
            if menuName is not None and checkParentMenuExist(menuName=menuName, parentMenuName=parentMenuName):
                resultData.errorData(errorMsg='修改内容不符合要求！')
                return JsonResponse(resultData.toDict())
            elif menuName is None:
                menuName_temp = getMenuInfoByMenuId(menuId=menuId)['MENUNAME'][0]
                if checkParentMenuExist(menuName=menuName_temp, parentMenuName=parentMenuName):
                    resultData.errorData(errorMsg='修改内容不符合要求！')
                    return JsonResponse(resultData.toDict())
        # 修改数据
        changeStatus = changeMenuData(menuId=menuId, menuName=menuName, menuUrl=menuUrl, parentMenuName=parentMenuName, parentMenuIcon=parentMenuIcon, parentOrderId=parentOrderId, subOrderId=subOrderId)
        changeMenuResult = getMenuInfoByMenuId(menuId=menuId)
        resultData.changeStatus = changeStatus
        resultData.changeMenuResult = changeMenuResult.to_dict()
        return JsonResponse(resultData.toDict())

    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错！')
        return JsonResponse(resultData.toDict())
