__author__ = 'Robby'

"""
数据字典缓存
表：sysdictionary
"""

import collections
import os

import pandas as pd

from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.toolsutils.ResultData import ResultData

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 每日定时更新
# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10

KEYFIX = "copilot_DICT"
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils

Logger = logger_utils.LoggerUtils()


# 字典翻译
# 通过key获取字典列表
def getDictByKeyNo(keyNo, reload=False):
    dictDF = cache_db.getSysDictDF(reload=reload)
    # dictDF = cache_db.getSysDictDF(reload=reload)
    # keyvalue <> '#'
    dictDF = dictDF[(dictDF['KEYNO'] == str(keyNo)) & (dictDF['KEYVALUE'] != '#')]
    if dictDF.empty:
        return {}

    # 字典顺序
    dictDF = dictDF.sort_values(['KEYORDER'])  # ,ascending=[False]
    orderDict = collections.OrderedDict()
    for idx, row in dictDF.iterrows():
        key = row.KEYVALUE
        value = row.KEYDESC
        orderDict[key] = value
    return orderDict


def getSysDict(keyNo, reload=False):
    """
    数据字典，返回dict
    """
    dictInfo = getDictByKeyNo(keyNo, reload=reload)
    return dictInfo


def getDictionaryData(keyNo, option=False):
    # 数据字典以下拉菜单显示
    key = KEYFIX + "_" + keyNo + "_" + str(option)
    cacheDictData = cache.get(key)

    dictInfo = getDictByKeyNo(keyNo)
    if not option:
        return list(dictInfo.values())

    if not cacheDictData is None:
        return cacheDictData

    if len(dictInfo) == 0:
        return ["<option value=''></option>"]
    # 拼接option标签
    optionList = ["<option value='" + k + "'>" + v + "</option>" for k, v in dictInfo.items()]

    # 添加股票选择列表至缓存中
    cache.set(key, optionList, CACHEUTILS_UPDATE_SECENDS)
    return optionList


# 股票对比股票代码翻译需要
def getStockNameByStockCode(flage='A', reload=False):
    # 股票选择器缓存处理
    key = KEYFIX + "_" + flage + '_stockName'
    stockNameDict = cache.get(key)

    if stockNameDict is None:
        reload = True

    if reload:
        result = cache_db.getStockInfoCache(flage, reload=reload)
        data = result.loc[:, ['STOCKCODE', 'STOCKNAME']]
        stockNameDict = dict(data.values)
        # 添加股票选择列表至缓存中
        cache.set(key, stockNameDict, CACHEUTILS_UPDATE_SECENDS)
    return stockNameDict


# 股票对比股票代码翻译需要
def getStockNameByWindCode(flage='A', reload=False):
    # 股票选择器缓存处理

    key = KEYFIX + "_" + flage + '_windName'
    windNameDict = cache.get(key)

    if windNameDict is None:
        reload = True

    if reload:
        result = cache_db.getStockInfoCache(flage, reload=reload)
        data = result.loc[:, ['WINDCODE', 'STOCKNAME']]
        windNameDict = dict(data.values)
        # 添加股票选择列表至缓存中
        cache.set(key, windNameDict, CACHEUTILS_UPDATE_SECENDS)
        return windNameDict
    return windNameDict



# 股票对港股比股票代码翻译需要
def getStockNameByWindHKCode(reload=False):
    # 股票选择器缓存处理

    key = KEYFIX + "_HKwindName"
    windNameDict = cache.get(key)

    if windNameDict is None:
        reload = True

    if reload:
        result = cache_db.getStockInfoCache(flage='H', reload=reload)
        data = result.loc[:, ['WINDCODE', 'STOCKNAME']]
        windNameDict = dict(data.values)
        # 添加股票选择列表至缓存中
        cache.set(key, windNameDict, CACHEUTILS_UPDATE_SECENDS)
        return windNameDict
    return windNameDict


# 股票对港股比股票代码翻译需要
def getStockNameByEastHKCode(reload=False):
    # 股票选择器缓存处理

    key = KEYFIX + "_HKeastName"
    windNameDict = cache.get(key)

    if windNameDict is None:
        reload = True
    if reload:
        result = cache_db.getStockInfoCache(flage='H', reload=reload)
        data = result.loc[:, ['EASTCODE', 'STOCKNAME']]
        windNameDict = dict(data.values)
        # 添加股票选择列表至缓存中
        cache.set(key, windNameDict, CACHEUTILS_UPDATE_SECENDS)
        return windNameDict

    return windNameDict


# 股票对港股比股票代码翻译需要
def getEastHKCode(reload=False):
    # 股票选择器缓存处理

    key = KEYFIX + "_HKeastCode"
    windNameDict = cache.get(key)

    if windNameDict is None:
        reload = True
    if reload:
        result = cache_db.getStockInfoCache(flage='H',reload=reload)
        stockinfoDict={}
        for idx,row in result.iterrows():
            stockinfoDict[row.WINDCODE]=row.EASTCODE
            
        # 添加股票选择列表至缓存中
        cache.set(key, stockinfoDict, CACHEUTILS_UPDATE_SECENDS)
        return stockinfoDict
    return windNameDict


# 股票对港股比股票代码翻译需要
def getWindHKCode(reload=False):
    # 股票选择器缓存处理

    key = KEYFIX + "_HKwindCode"
    windNameDict = cache.get(key)

    if windNameDict is None:
        reload = True

    if reload:
        result = cache_db.getStockInfoCache(flage='H', reload=reload)
        stockinfoDict={}
        for idx,row in result.iterrows():
            stockinfoDict[row.EASTCODE]=row.WINDCODE
            
        # 添加股票选择列表至缓存中
        cache.set(key, stockinfoDict, CACHEUTILS_UPDATE_SECENDS)
        return stockinfoDict

    return windNameDict

# wind to east
def swtWindtoEast(reload=False):
    # 股票选择器缓存处理
    key = KEYFIX + "_WindtoEast"
    windNameDict = cache.get(key)
    if windNameDict is None:
        reload = True

    if reload:
        result =cache_db.getStockInfoCache(flage='ALL', reload=reload)
        stockinfoDict={}
        for idx,row in result.iterrows():
            stockinfoDict[row.WINDCODE]=row.EASTCODE
        cache.set(key, stockinfoDict, CACHEUTILS_UPDATE_SECENDS)
        return stockinfoDict
    return windNameDict


# east to wind
def swtEastToWind(reload=False):
    # 股票选择器缓存处理

    key = KEYFIX + "_EastToWind"
    windNameDict = cache.get(key)

    if windNameDict is None:
        reload = True

    if reload:
        result = cache_db.getStockInfoCache(flage='ALL', reload=reload)
        stockinfoDict={}
        for idx,row in result.iterrows():
            stockinfoDict[row.EASTCODE]=row.WINDCODE
        # 添加股票选择列表至缓存中
        cache.set(key, stockinfoDict, CACHEUTILS_UPDATE_SECENDS)
        return stockinfoDict

    return windNameDict


# 所有股票代码翻译，无后缀转有后缀
def getStockSuffixDic(reload=False):
    # 600619>600519.SH
    # 0700.HK >0700 >0700.HK
    key = KEYFIX + "_SuffixCode"
    suffixDict = cache.get(key)

    if suffixDict is None:
        reload = True

    if reload:
        allStockCode = cache_db.getStockInfoCache(flage='ALL', reload=reload)
        suffixDict={}
        for idx,row in allStockCode.iterrows():
            suffixDict[row.STOCKCODE]=row.WINDCODE
        # 去除港股带后缀的 0700.HK >0700
        # allStockCode["STOCKCODE"] = allStockCode["STOCKCODE"].apply(lambda x: fixCode(x))
        # allStockCode = allStockCode.loc[:, ['STOCKCODE', 'WINDCODE']]
        # suffixDict = dict(allStockCode.values)
        # 添加股票选择列表至缓存中
        cache.set(key, suffixDict, CACHEUTILS_UPDATE_SECENDS)
        return suffixDict
    return suffixDict

def translateStockCode(stockCode):
    """
    通过股票代码（A股不带后缀，H、美股带后缀）翻译获取股票名称
    """
    stockInfos = getStockNameByStockCode('ALL')
    try:
        stockName = stockInfos[stockCode]
    except Exception as ex:
        Logger.error('翻译股票代码[{}]不存在'.format(stockCode))
        return '未知代码'
    return stockName


def translateStockWindCode(windCode):
    """
    通过股票代码(带码后缀)翻译获取股票名称
    """
    stockInfos = getStockNameByWindCode('ALL')
    try:
        stockName = stockInfos[windCode]
    except Exception as ex:
        Logger.error('翻译股票代码[{}]不存在'.format(windCode))
        return '未知代码'
    return stockName


def sysDictTrans(keyNo, keyValue, reload=False):
    # 数据字典以下拉菜单显示
    key = KEYFIX + '_dictionary_' + keyNo
    cacheDictData = cache.get(key)
    if cacheDictData is None:
        reload = True

    if reload:
        cacheDictData = getSysDict(keyNo, reload=reload)
        cache.set(key, CACHEUTILS_UPDATE_SECENDS)  # 单位秒

    if cacheDictData.empty:
        return keyNo
    if pd.isnull(keyValue):
        return keyNo
    data = cacheDictData.loc[cacheDictData['KEYVALUE'] == str(keyValue)]['KEYDESC']
    if data.empty:
        return keyNo
    return data.values[0]


def getStockArea(windCode,reload=False):
    stockAreaDict =getStockAreaDict(reload=reload)
    if windCode in stockAreaDict:
        return stockAreaDict[windCode]
    return windCode

def getStockAreaDict(reload=False):
    qKey = KEYFIX + "_STOCK_AREA"
    dataDict = cache.get(qKey)
    isLock = False
    if dataDict is None:
        #_lock=threading.RLock()
        #_lock.acquire()
        isLock = True
        reload = True
        dataDict = cache.get(qKey)
        if dataDict is not None:
            reload = False
    if reload:
        if True:
            stockDF = cache_db.getStockInfoCache(flage='ALL',reload=reload)
            stockDF=stockDF[['WINDCODE','AREA']]
            dataDict={}
            for row in stockDF.itertuples():
                windCode=row.WINDCODE
                area=row.AREA
                dataDict[windCode]=area
            cache.set(qKey, dataDict, CACHEUTILS_UPDATE_SECENDS)
        if isLock:
            pass#_lock.release()
    return dataDict

def getCacheStockInfo(windCode,reload=False):
    stockInfoDict =getStockInfoDT(reload=reload)
    if windCode in stockInfoDict:
        return stockInfoDict[windCode]
    return windCode

def getStockInfoDT(flage='ALL', reload=False):
    key = KEYFIX + '_StockInfo'
    stockInfo_dt = cache.get(key)
    if stockInfo_dt is None or len(stockInfo_dt)==0:
        reload = True
    if reload:
        windDF=cache_db.getStockInfoCache(flage=flage, reload=reload)
        rtdata = windDF.rename(columns=lambda x: x.capitalize())
        stockInfo_dt = {}
        for i, wd in rtdata.iterrows():
            windCode = wd.Windcode
            stockInfo_dt[windCode] = dict(wd)
        cache.set(key, stockInfo_dt ,CACHEUTILS_UPDATE_SECENDS)  # 单位秒

    return stockInfo_dt

def getIndxInfoDT(flage='ALL', reload=False):
    #指数
    key = KEYFIX + '_IndexInfo'
    stockInfo_dt = cache.get(key)
    if stockInfo_dt is None or len(stockInfo_dt)==0:
        reload = True
    if reload:
        windDF=cache_db.getStockInfoCache(flage=flage, reload=reload)
        rtdata = windDF.rename(columns=lambda x: x.capitalize())
        stockInfo_dt = {}
        for i, wd in rtdata.iterrows():
            windCode = wd.Windcode
            if wd.Stocktype in ['idx']:
                stockInfo_dt[windCode] = dict(wd)
        cache.set(key, stockInfo_dt ,CACHEUTILS_UPDATE_SECENDS)  # 单位秒

    return stockInfo_dt

def delDictCache():
    # 数据库数据同步完成后，清空缓存，获取最新数据
    count = cache.delete_pattern(KEYFIX + "_*")
    return count


def loadDictCache():
    """
    数据库数据同步完成后,获取最新数据
    """
    resultData = ResultData()
    try:
        getStockNameByStockCode(flage='A', reload=True)
        getStockNameByStockCode(flage='ALL', reload=True)
        getStockNameByStockCode(flage='ZZETF', reload=True)

        getStockNameByWindCode(flage='A', reload=True)
        getStockNameByWindCode(flage='ALL', reload=True)
        getStockNameByWindCode(flage='ZZETF', reload=True)
        swtEastToWind(reload=True)
        swtWindtoEast(reload=True)

        getStockSuffixDic(reload=True)

    except Exception as ex:
        resultData.errorData(errorMsg="stockBaseCache刷新异常")
        Logger.error("stockBaseCache刷新异常")
        Logger.errLineNo()
    return resultData


if __name__ == '__main__':
    # sysDict1 = sysDict()
    # sysDict2 = sysDict()
    # sysDict1 = sysDict()
    # print((sysDict1._sysDict__sysDictData))
    # print(dir(sysDict1))
    # sysDict1.getDictByKeyNo('3000')

    # df=getStockNameByStockCode(flage='ALL')
    # print(df)
    # df=getStockNameByWindCode()
    # print(df)
    # cache.set('dictionary' + str("False") + '3000',None)
    # cache.set('dictionary' + str("True") + '3000',None)
    # data =getDictionaryData('3000',option=False)
    # print(data )
    # data =getSysDict('3000')
    # data =getIndustry1Dict()
    # print(data )
    # data =getIndustry2Dict()
    # print(data )
    # data =getIndustry3Dict()
    # data =getIndexRangeDict()
    # data =getStockSuffixDic()
    # data =getIndexCodeDict()
    content = "\xd5\xd2\xb2\xbb\xb5\xbd\xd6\xb8\xb6\xa8\xb5\xc4\xb3\xcc\xd0\xf2\xa1\xa3"
    content = content.encode("latin1").decode("gbk")
    content=cache_db.getStockInfoCache(flage='gzqh')
    content.loc[17]['STOCKCODE']='xxx'
    # content = getStockSuffixDic(reload=True)
    s=['']*10
    print(s)
