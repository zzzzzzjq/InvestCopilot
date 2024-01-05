import pandas as pd
from django.http import JsonResponse
from InvestCopilot_App.models.toolsutils import LoggerUtils as logger_utils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco,userMenuPrivCheckDeco
from InvestCopilot_App.models.cache import BaseCacheUtils as base_cache
from django.core.cache import cache

from InvestCopilot_App.models.toolsutils import ToolsUtils as tools_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils

logger = logger_utils.LoggerUtils()

@userLoginCheckDeco
def getCacheList(request):
    """
    获取缓存列表
    """
    try:
        resultData = ResultData()
        cacheListPdData = getCacheListData()
        dataLength = len(cacheListPdData.values.tolist())
        tmpPdData = pd.DataFrame({'操作':['']*dataLength})
        cacheListPdData = pd.concat([cacheListPdData,tmpPdData],axis=1)
        resultData.tbColumn = cacheListPdData.columns.values.tolist()
        resultData.tbData = cacheListPdData.values.tolist()
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='获取数据失败，请重试！')
        return JsonResponse(resultData.toDict())

@userLoginCheckDeco
def delCacheKey(request):
    """
    删除键的操作
    """
    try:
        resultData = ResultData()
        delKey = request.POST.get('key',None)
        if delKey is None:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(delKey) == 0:
            resultData.errorData(errorMsg='接口传参有误！')
            return JsonResponse(resultData.toDict())
        try:
            bCache = base_cache.BaseCacheUtils()
            bCache.delKey(delKey)  #从缓存中删除键
        except:
            logger.errLineNo()
            resultData.errorData(errorMsg='删除键失败！')
            return JsonResponse(resultData.toDict())
        isSuccess = multi_api.delKeyFromDb(delKey)
        if isSuccess is False:
            resultData.errorData(errorMsg='删除键失败！')
            return JsonResponse(resultData.toDict())
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错，请重试！')
        return JsonResponse(resultData.toDict())

@userLoginCheckDeco
def flushOneKey(request):
    """
    刷新单个键
    """
    try:
        resultData = ResultData()
        flushKey = request.POST.get('key',None)
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
        logger.errLineNo()
        resultData.errorData(errorMsg='程序出错，请重试！')
        return JsonResponse(resultData.toDict())

@userLoginCheckDeco
@userMenuPrivCheckDeco('系统缓存管理')
def managerCachePage(request):
    """
    展示缓存管理的页面
    """
    resultData = ResultData()
    returnPage = 'manager/managecache.html'
    resultData.topMenu = '后台维护'
    resultData.subMenu = '系统缓存管理'
    return resultData.buildRender(request,returnPage)

@userLoginCheckDeco
def getSysCacheList(request):
    """
    获取缓存列表数据
    """
    try:
        resultData = ResultData()
        cacheType = request.POST.get('cacheType',None)
        if cacheType is None:
            resultData.errorData(errorMsg='获取缓存列表的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(cacheType) == 0:
            resultData.errorData(errorMsg='获取缓存列表的接口传参有误！')
            return JsonResponse(resultData.toDict())
        resultData.tbCol = ['缓存键值','缓存大小','缓存数据类型','剩余时间','操作']
        resultData.tbData = getNewCacheList(cacheType)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='获取缓存列表的接口出错！')
        return JsonResponse(resultData.toDict())

@userLoginCheckDeco
def flushSysCacheKey(request):
    """
    刷新缓存
    """
    try:
        resultData = ResultData()
        cacheKey = request.POST.get('cacheKey',None)
        if cacheKey is None:
            resultData.errorData(errorMsg='刷新缓存的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(cacheKey) == 0:
            resultData.errorData(errorMsg='刷新缓存的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if str(cacheKey).startswith('ALL_TABLE_'):  #整表数据
            tableName = str(cacheKey).strip().replace('ALL_TABLE_','')
            cache_db.getCacheTableAllDataDF(tableName,reload=True)
        elif str(cacheKey).startswith('NEW_TABLE_'):  #最新表数据
            tableName = str(cacheKey).strip().replace('NEW_TABLE_','')
            cache_db.getCacheTableNewData(tableName,reload=True)
        else:
            cache.delete(cacheKey)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='刷新缓存的接口出错！')
        return JsonResponse(resultData.toDict())

@userLoginCheckDeco
def searchSysCache(request):
    """
    搜索缓存
    """
    try:
        resultData = ResultData()
        searchKey = request.POST.get('searchKey',None)
        if searchKey is None:
            resultData.errorData(errorMsg='搜索缓存的接口传参有误！')
            return JsonResponse(resultData.toDict())
        if len(searchKey) == 0:
            resultData.tbCol = ['缓存键值','缓存大小','缓存数据类型','剩余时间','操作']
            resultData.tbData = []
            return JsonResponse(resultData.toDict())
        searchKey = str(searchKey).strip().upper()
        resultData.tbCol = ['缓存键值','缓存大小','缓存数据类型','剩余时间','操作']
        resultData.tbData = getNewSearchCacheList(searchKey)
        return JsonResponse(resultData.toDict())
    except Exception as ex:
        logger.errLineNo()
        resultData.errorData(errorMsg='搜索缓存的接口出错！')
        return JsonResponse(resultData.toDict())


def getCacheListData():
    """
    获取系统缓存列表
    """
    con = dbutils.getDBConnect()
    selectSql1 = '''
    SELECT t.cachekey,
    t.cachesize,
    TO_CHAR(t.createtime,'yyyy-MM-dd HH24:mi:ss') AS flush_time,
    TO_CHAR(t.validtime,'yyyy-MM-dd HH24:mi:ss') AS end_time
    FROM cachelog t
    WHERE SUBSTR(t.cachekey,0,3) = 'SQL'
    OR SUBSTR(t.cachekey,0,5) = 'TABLE'
    '''
    selectSql2 = '''
    SELECT DECODE(t.cachetype,1,'基础数据',2,'指标数据') AS cache_data_type,
    'TABLE_'||t.tablename AS cachekey
    FROM cachetable t
    '''
    data1 = pd.read_sql(selectSql1,con=con)
    data2 = pd.read_sql(selectSql2,con=con)
    data = pd.merge(data1,data2,how='left',on='CACHEKEY')
    data['CACHE_DATA_TYPE'] = data['CACHE_DATA_TYPE'].apply(cacheDataType)
    data['END_TIME_DT'] = data['END_TIME'].apply(tools_utils.timeStr2Datetime)
    data['AVAILABLE_TIME'] = data['END_TIME_DT'].apply(calcCacheAvailableTime)
    data = data.drop('END_TIME_DT',axis=1)
    data = data.rename(columns = {'CACHEKEY':'缓存键值','CACHESIZE':'缓存大小','FLUSH_TIME':'更新时间',
                                  'END_TIME':'截止时间','CACHE_DATA_TYPE':'缓存数据类型','AVAILABLE_TIME':'剩余时间'})
    con.close()
    return data



def getNewCacheList(cacheType):
    """
    获取缓存列表
    """
    if cacheType == 'SQL':
        searchKey = 'SQL*'
    elif cacheType == 'TABLE':
        searchKey = 'ALL_TABLE_*'
    elif cacheType == 'TABLE_NEW':
        searchKey = 'NEW_TABLE_*'
    elif cacheType == 'DICT':
        searchKey = 'DICT*'
    elif cacheType == 'BASE_MODE':
        searchKey = 'BASEMODE*'
    cacheList = cache.keys(searchKey)
    dataList = []
    for key in cacheList:
        cacheData = cache.get(key)
        cacheSize = base_cache.getSizeOf(cacheData)
        t = cache.ttl(key)
        if t is None:
            cacheAvailableTime = '永久存在'
        else:
            cacheAvailableTime = tools_utils.changeTimeFormat(t)
        if str(key).startswith('SQL'):
            typeName = 'SQL联查结果数据'
        elif str(key).startswith('DICT'):
            typeName = '字典数据'
        elif str(key).startswith('ALL_TABLE'):
            typeName = '整表数据'
        elif str(key).startswith('NEW_TABLE'):
            typeName = '最新表数据'
        elif str(key).startswith('BASEMODE'):
            typeName = '业务缓存'

        dataList.append([key,cacheSize,typeName,cacheAvailableTime,""])
    return dataList

def getNewSearchCacheList(searchKey):
    """
    获取所有的缓存
    """
    allCacheList = cache.keys('*')
    searchCacheList = []
    for key in allCacheList:
        if searchKey in key.upper():
            cacheData = cache.get(key)
            cacheSize = base_cache.getSizeOf(cacheData)
            at = cache.ttl(key)
            if at is None:
                cacheAvailableTime = '永久存在'
            else:
                cacheAvailableTime = tools_utils.changeTimeFormat(at)
            if str(key).startswith('SQL'):
                typeName = 'SQL联查结果数据'
            elif str(key).startswith('DICT'):
                typeName = '字典数据'
            elif str(key).startswith('ALL_TABLE'):
                typeName = '整表数据'
            elif str(key).startswith('NEW_TABLE'):
                typeName = '最新表数据'
            elif str(key).startswith('BASEMODE'):
                typeName = '业务缓存'
            else:
                typeName = '其他类型缓存'
            searchCacheList.append([key,cacheSize,typeName,cacheAvailableTime,""])
    return searchCacheList
