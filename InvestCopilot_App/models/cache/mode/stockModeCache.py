__author__ = 'Robby'

"""
个股诊断数据缓存
"""
import os
import sys

sys.path.append("../../..")
import datetime
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils

Logger = logger_utils.LoggerUtils()
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from InvestCopilot_App.models.models.stock import stockutils as stock_utils
from InvestCopilot_App.models.models.stock import stockbase as stock_base
import InvestCopilot_App.models.models.stock.stockvaluefactor as stock_valuefactor
import InvestCopilot_App.models.models.stock.stockQualityFactor as stock_qualityfactor
import InvestCopilot_App.models.models.stock.stockMomentumFactor as stock_momentumfactor
import InvestCopilot_App.models.models.stock.stockgrowthfactor as stock_growthfactor
import InvestCopilot_App.models.models.stock.stockanalysis as stock_analysis

# 缓存有效期24*6小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10

KEYFIX = "STOCKMODE"


def getBaseStockInfoCache(stockcode, reload=False):
    key = KEYFIX + '_stockbase_' + str(stockcode)
    stockBaseData = cache.get(key)
    if stockBaseData is None:
        reload = True
    if reload:
        stockBaseData = stock_base.getstockinfo(stockcode)
        # 估值表格和图表展示数据
        rankData = stock_valuefactor.getForwardFactorData(stockcode)
        stockBaseData['VALUERANKDATA'] = rankData
        cache.set(key, stockBaseData, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return stockBaseData


def getAnalySisdataCache(stockcode, reload=False):
    """
    分析师数据缓存
    """
    key = KEYFIX + '_analysisdata_' + str(stockcode)
    analysisdata = cache.get(key)
    if analysisdata is None:
        reload = True
        # logger.info('analysisdata cache in : %s', cache.get('analysisdata' + stockcode))
    if reload:
        analysisdata = stock_analysis.get_stockanalysisdata(stockcode)
        cache.set(key, analysisdata, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return analysisdata


def getAnalysisFy1Fy2CacheData(stockCode, reload=False):
    """
    分析师调整报表数据缓存
    """
    key = KEYFIX + '_analysisfy1fy2data_' + str(stockCode)
    fy1fy2Data = cache.get(key)
    if fy1fy2Data is None:
        reload = True
    if reload:
        fy1fy2Data = stock_base.getAnalysisFy1fy2CacheData(stockCode, 'NEWDATA.ANALYSTDATA')
        cache.set(key, fy1fy2Data, CACHEUTILS_UPDATE_SECENDS)
    return fy1fy2Data


def getMomentumChangeCacheData(stockCode, reload=False):
    """
    动量变化数据缓存
    """
    key = KEYFIX + '_momentumchangedata_' + str(stockCode)
    momentumData = cache.get(key)
    if momentumData is None:
        reload = True
    if reload:
        momentumData = stock_base.getMomentumChangeCacheData(stockCode,
                                                             "NEWDATA.MOMENTUMFACTOR_FINAL", "NEWDATA.MOMENTUMDATA")
        cache.set(key, momentumData, CACHEUTILS_UPDATE_SECENDS)
    return momentumData


def getValueFactorDataCache(stockcode, reload=False):
    """
    估值数据缓存
    """
    key = KEYFIX + '_valuedata_' + str(stockcode)
    valuedata = cache.get(key)
    if valuedata is None:
        reload = True
    if reload:
        valuedata = stock_valuefactor.get_valuefactor(stockcode)
        cache.set(key, valuedata, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return valuedata


def getGrowthFactorDataCache(stockcode, reload=False):
    """
    成长数据缓存
    """
    key = KEYFIX + '_growthdata_' + str(stockcode)
    growthdata = cache.get(key)
    if growthdata is None:
        reload = True
    if reload:
        growthdata = stock_growthfactor.get_stockgrowthfactor(stockcode)
        cache.set(key, growthdata, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return growthdata


def getQualityFactorDataCache(stockcode, reload=False):
    """
    盈利质量数据缓存
    """
    key = KEYFIX + '_qualitydata_' + str(stockcode)
    qualitydata = cache.get(key)
    if qualitydata is None:
        reload = True
    if reload:
        qualitydata = stock_qualityfactor.getQualityFactor(stockcode)
        cache.set(key, qualitydata, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return qualitydata


def getMomentumFactorDataCache(stockcode, reload=False):
    """
    动量数据缓存
    """
    key = KEYFIX + '_momentumdata_' + str(stockcode)
    momentumdata = cache.get(key)
    if momentumdata is None:
        reload = True
    if reload:
        momentumdata = stock_momentumfactor.getMomentumFactor(stockcode)
        cache.set(key, momentumdata, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return momentumdata


def getFactorChangeDataCache(stockcode, reload=False):
    """
    指标数据变动缓存
    """
    key = KEYFIX + '_factorchange_' + str(stockcode)
    factorchangedata = cache.get(key)
    if factorchangedata is None:
        reload = True
    if reload:
        factorchangedata = stock_utils.getFactorChange(stockcode)
        cache.set(key, factorchangedata, CACHEUTILS_UPDATE_SECENDS)  # 单位秒
    return factorchangedata


def getCNYPrice(base, reload=False):
    """
    1港元=X人民币:HKD
    """
    try:
        key = KEYFIX + '_' + str(base)
        CNY = cache.get(key)
        if CNY is None:
            reload = True

        if reload:
            CNY = 0.80406
            url = "https://api.fixer.io/latest?base=%s" % (base)
            import requests
            response = requests.get(url)
            import json
            result = json.loads(response.text)
            if "rates" in result:
                rs = result['rates']
                if "CNY" in rs:
                    CNY = (result['rates']['CNY'])
                    cache.set(key, CNY, 60 * 30)  # 30分钟

    except Exception as ex:
        Logger.errLineNo(msg="获取汇率异常：%s" % url)
        CNY = 0.80406
    return CNY


def delStockModeCache():
    # 数据库数据同步完成后，清空缓存，获取最新数据
    count = cache.delete_pattern(KEYFIX + "_*")
    return count


def loadStockModeCache(reload=False):
    """
    加载业务数据缓存
    """
    stockInfoDict = cache_dict.getStockNameByStockCode(flage='A', reload=reload)
    stocklist = stockInfoDict.keys()
    stocklist = sorted(stocklist)
    # stocklist=['600519']
    start = datetime.datetime.now()
    # 股票列表数目
    allCount = len(stocklist)
    allSize = 0
    failCount = 0
    sucessCount = 0
    for idx, sotckCode in enumerate(stocklist):
        try:
            being = datetime.datetime.now()
            data1 = getAnalySisdataCache(sotckCode, reload=reload)
            data2 = getValueFactorDataCache(sotckCode, reload=reload)
            data3 = getGrowthFactorDataCache(sotckCode, reload=reload)
            data4 = getQualityFactorDataCache(sotckCode, reload=reload)
            data5 = getMomentumFactorDataCache(sotckCode, reload=reload)
            data6 = getAnalysisFy1Fy2CacheData(sotckCode, reload=reload)
            data7 = getMomentumChangeCacheData(sotckCode, reload=reload)

            size = sys.getsizeof(data1) + sys.getsizeof(data2) + sys.getsizeof(data3) + sys.getsizeof(
                data4) + sys.getsizeof(data5) + sys.getsizeof(data6) + sys.getsizeof(data7)
            allSize += size
            # print('loadStockModeCache: %s,%s,%s/byte,%s/s' % (idx, sotckCode, str(size), (datetime.datetime.now() - being).seconds * 1000))
            Logger.info('loadStockModeCache: %s,%s,%s/byte,%s/s' % (
            idx, sotckCode, str(size), (datetime.datetime.now() - being).seconds * 1000))
            sucessCount += 1
        except Exception as ex:
            failCount += 1
            Logger.error("[%s]个股诊断数据加载失败" % (sotckCode))
            Logger.errLineNo()

    total = sucessCount + failCount
    isTrue = False
    if total == allCount:
        isTrue = True

    # 处理状态，总条数据，成功数据，失败数据，大小，耗时（秒）
    Logger.info('加载业务数据缓存完毕，状态[%s]，总条数据[%d]，成功[%d]，失败[%d]，大小[%s]，耗时[%s]' %
                (str(isTrue), allCount, sucessCount, failCount, str(int(allSize / (1024 * 1024))) + "M",
                 str((datetime.datetime.now() - start).seconds) + "S"))

    title = '个股诊断缓存[%s]总数[%d]成功[%d]失败[%d]大小[%s]耗时[%s]' % \
            (str(isTrue), allCount, sucessCount, failCount, str(int(allSize / (1024 * 1024))) + "M",
             str((datetime.datetime.now() - start).seconds) + "S")
    print(title)
    try:
        Logger.info(title)
        import InvestCopilot_App.sendemail as sendemail
        #sendemail.sendSimpleEmail(sendemail.InvestCopilotList, 'robby.xia', title, "")
    except Exception as ex:
        Logger.error("个股诊断缓存数据刷新异常")
        Logger.errLineNo()

    return [isTrue, allCount, sucessCount, failCount, str(int(allSize / (1024 * 1024))) + "M",
            str((datetime.datetime.now() - start).seconds) + "S"]


if __name__ == '__main__':
    # 个股代码缓存数据
    # delStockModeCache()
    # print(getCNYPrice("HKD"))
    loadStockModeCache(reload=True)
    pass
