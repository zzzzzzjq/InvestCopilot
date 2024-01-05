import pandas as pd
from django.http import JsonResponse
from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco,userMenuPrivCheckDeco
from InvestCopilot_App.models.cache import BaseCacheUtils as base_cache
from InvestCopilot_App.models.manager import DictMode as dict_mode
from django.core.cache import cache

from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils

Logger = logger_utils.LoggerUtils()

@userLoginCheckDeco
def getCacheList(request):
    """
    获取缓存列表
    """
    try:
        resultData = ResultData()
        cacheListPdData = base_cache.getCacheListData()
        dataLength = len(cacheListPdData.values.tolist())
        tmpPdData = pd.DataFrame({'操作': [''] * dataLength})
        cacheListPdData = pd.concat([cacheListPdData, tmpPdData], axis=1)
        resultData.tbColumn = cacheListPdData.columns.values.tolist()
        resultData.tbData = cacheListPdData.values.tolist()
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='获取数据失败，请重试！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def delCacheKey(request):
    """
    删除键的操作
    """
    try:
        resultData = ResultData()
        delKey = request.POST.get('key', None)
        if delKey is None:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(delKey) == 0:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        try:
            base_cache.delKey(delKey)  # 从缓存中删除键
        except:
            Logger.errLineNo()
            resultData.errorData(errorMsg='删除键失败！')
            return JsonResponse(resultData.toDict())
        isSuccess = base_cache.delKeyFromDb(delKey)
        if isSuccess is False:
            resultData.errorData(errorMsg='删除键失败！')
            return JsonResponse(resultData.toDict())
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='程序出错，请重试！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def flushOneKey(request):
    """
    刷新单个键
    """
    try:
        resultData = ResultData()
        flushKey = request.POST.get('key', None)
        if flushKey is None:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(flushKey) == 0:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        flushKey = str(flushKey)
        if flushKey.startswith('SQL_'):
            isSuccess = cache_db.flushOneDbCache(flushKey)
        elif flushKey.startswith('TABLE_'):
            isSuccess = cache_db.flushOneTableCache(flushKey)
        if isSuccess is False:
            resultData.errorData(errorMsg='刷新失败！')
            return JsonResponse(resultData.toDict())
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='程序出错，请重试！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def managerCommPage(request):
    """
    展示缓存管理的页面
    """
    resultData = ResultData()
    returnPage = 'manager/managerdict.html'
    resultData.topMenu = '后台维护'
    resultData.subMenu = '字典管理'
    # resultData = menu_mode.buildRtPageData(resultData, returnPage)

    return resultData.buildRender(request, returnPage)


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def getDictKeyList(request):
    """
    获取字典列表
    """
    try:
        rs = ResultData()
        dictDF = dict_mode.getDictKeyList()
        _userId=request.session.get("user_id")
        if _userId in ['1795']:
            #michael可修改字典 开通
            klist=['1737','1744']
            dictKeyList = [{'keyNo': row.KEYNO, 'keyName': row.KEYDESC}
                           for row in dictDF.itertuples() if str(row.KEYNO) in klist]
        else:
            dictKeyList = [{'keyNo': row.KEYNO, 'keyName': row.KEYDESC} for row in dictDF.itertuples()]
        rs.dictKeyList = dictKeyList
        return JsonResponse(rs.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='获取字典列表的接口出错！')
        return JsonResponse(rs.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def getDictValueList(request):
    """
    获取字典值列表
    """
    try:
        rs = ResultData()
        keyNo = request.POST.get('keyNo', None)

        if tools_utils.isNull(keyNo):
            rs.errorData(errorMsg='字典编号不能为空，请重新选择！')
            return JsonResponse(rs.toDict())

        dictDF = dict_mode.getDictValueList(keyNo)
        dictValueList = [{'keyNo': row.KEYNO, 'keyName': row.KEYDESC, 'keyValue': row.KEYVALUE} for row in
                         dictDF.itertuples()]
        rs.dictValueList = dictValueList
        return JsonResponse(rs.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='获取字典列表的接口出错！')
        return JsonResponse(rs.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def delDictValueByKey(request):
    """
    删除字典值列表
    """
    try:
        rs = ResultData()
        keyNo = request.POST.get('keyNo', None)
        keyValue = request.POST.get('keyValue', None)
        if tools_utils.isNull(keyNo) or tools_utils.isNull(keyValue):
            rs.errorData(errorMsg='请选择需要删除的字典！')
            return JsonResponse(rs.toDict())

        rs = dict_mode.delDictValueByKey(keyNo, keyValue)
        # 刷新字典缓存
        cache_db.getSysDictDF(reload=True)
        return JsonResponse(rs.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='抱歉，刪除字典值异常，请稍后重试！')
        return JsonResponse(rs.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def delDictKey(request):
    """
    删除字典列表
    """
    try:
        rs = ResultData()
        keyNo = request.POST.get('keyNo', None)
        if tools_utils.isNull(keyNo):
            rs.errorData(errorMsg='字典编号不能为空，请重新选择！')
            return JsonResponse(rs.toDict())

        rs = dict_mode.delDictByKey(keyNo)
        # 刷新字典缓存
        cache_db.getSysDictDF(reload=True)
        return JsonResponse(rs.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='抱歉，刪除字典异常，请稍后重试！')
        return JsonResponse(rs.toDict())


@userLoginCheckDeco
def addDictKey(request):
    """
    新增字典列表
    """
    try:
        rs = ResultData()
        keyName = request.POST.get('keyName', None)
        if tools_utils.isNull(keyName):
            rs.errorData(errorMsg='新增字典名称不能为空！')
            return JsonResponse(rs.toDict())

        ck_rs = tools_utils.charLength(keyName, 100, checkObj="字典名称")
        if not ck_rs.errorFlag:
            return ck_rs

        rs = dict_mode.addDictKey(keyName)
        # 刷新字典缓存
        cache_db.getSysDictDF(reload=True)

        return JsonResponse(rs.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='抱歉，添加字典异常，请稍后重试！')
        return JsonResponse(rs.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def addDictValueByKey(request):
    """
    新增字典值
    """
    try:
        # print("addDictValueByKey:",request.POST)
        rs = ResultData()
        keyNo = request.POST.get('keyNo', None)
        keyDesc = request.POST.get('keyDesc', None)
        keyValue = request.POST.get('keyValue', None)
        doMethod = request.POST.get('doMethod', None)

        if tools_utils.isNull(keyNo):
            rs.errorData(errorMsg='字典编号不能为空，请重新选择！')
            return JsonResponse(rs.toDict())

        if tools_utils.isNull(keyDesc):
            rs.errorData(errorMsg='添加字典值名称不能为空！')
            return JsonResponse(rs.toDict())

        if tools_utils.isNull(keyValue):
            rs.errorData(errorMsg='添加字典值不能为空！')
            return JsonResponse(rs.toDict())

        ck_rs = tools_utils.charLength(keyDesc, 200, checkObj="字典名称")
        if not ck_rs.errorFlag:
            return JsonResponse(ck_rs.toDict())

        ck_rs = tools_utils.charLength(keyValue, 50, checkObj="字典值")
        if not ck_rs.errorFlag:
            return JsonResponse(ck_rs.toDict())
        if doMethod=='add':
            rs = dict_mode.addDictValueByKey(keyNo, keyValue, keyDesc)
        elif doMethod=='edit':
            oldKeyValue = request.POST.get('oldKeyValue', None)
            if oldKeyValue is None or oldKeyValue=='':
                rs.errorData(errorMsg='字典值不能为空！')
                return JsonResponse(rs.toDict())

            rs = dict_mode.editDictValueByKey(keyNo, oldKeyValue,keyValue,keyDesc)

        # 刷新字典缓存
        cache_db.getSysDictDF(reload=True)
        return JsonResponse(rs.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='抱歉，添加字典值异常，请稍后重试！')
        return JsonResponse(rs.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def reloadDictData(request):
    """
    刷新字典缓存
    """
    try:
        rs = ResultData()
        cache_db.getSysDictDF(reload=True)
        return JsonResponse(rs.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rs.errorData(errorMsg='抱歉，加载字典数据异常，请稍后重试！')
        return JsonResponse(rs.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def flushSysCacheKey(request):
    """
    刷新缓存
    """
    try:
        resultData = ResultData()
        cacheKey = request.POST.get('cacheKey', None)
        if cacheKey is None:
            resultData.errorData(errorMsg='刷新缓存的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(cacheKey) == 0:
            resultData.errorData(errorMsg='刷新缓存的接口传参有误！')
            return JsonResponse(resultData.toDict())

        if str(cacheKey).startswith('NEW_TABLE_GROUP_'):  # 最新表数据 财报数据
            tableName = str(cacheKey).strip().replace('NEW_TABLE_GROUP_', '')
            cache_db.getCacheTableNewDataGroupByStockCode(tableName, reload=True)
        elif str(cacheKey).startswith('NEW_TABLE_Financial_'):  # 最新表数据财务数据
            tableName = str(cacheKey).strip().replace('NEW_TABLE_Financial_', '')
            cache_db.getCacheFinancialData(tableName, reload=True)
        elif str(cacheKey).startswith('ALL_TABLE_'):  # 整表数据
            tableName = str(cacheKey).strip().replace('ALL_TABLE_', '')
            cache_db.getCacheTableAllDataDF(tableName, reload=True)
        elif str(cacheKey).startswith('NEW_TABLE_'):  # 最新表数据
            tableName = str(cacheKey).strip().replace('NEW_TABLE_', '')
            cache_db.getCacheTableNewData(tableName, reload=True)
        else:
            cache.delete(cacheKey)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='刷新缓存的接口出错！')
        return JsonResponse(resultData.toDict())


@userLoginCheckDeco
@userMenuPrivCheckDeco('字典管理')
def searchSysCache(request):
    """
    搜索缓存
    """
    try:
        resultData = ResultData()
        searchKey = request.POST.get('searchKey', None)
        if searchKey is None:
            resultData.errorData(errorMsg='搜索缓存的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(searchKey) == 0:
            resultData.tbCol = ['缓存键值', '缓存大小', '缓存数据类型', '剩余时间', '操作']
            resultData.tbData = []
            return JsonResponse(resultData.toDict())
        searchKey = str(searchKey).strip().upper()
        resultData.tbCol = ['缓存键值', '缓存大小', '缓存数据类型', '剩余时间', '操作']
        resultData.tbData = base_cache.getNewSearchCacheList(searchKey)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        Logger.errLineNo()
        resultData.errorData(errorMsg='搜索缓存的接口出错！')
        return JsonResponse(resultData.toDict())
