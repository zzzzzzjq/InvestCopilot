__author__ = 'Robby'

"""
股票信息数据缓存
"""
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils

Logger = logger_utils.LoggerUtils()

from InvestCopilot_App.models.cache import cacheDB as cache_db
from InvestCopilot_App.models.cache import buildCacheData as cache_build
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.models.manager import ManagerSysIndexMode as manager_index_mode
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")
from django.core.cache import cache

# 每日定时更新
# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10
KEYFIX = "BASEMODE"


# 股票下拉菜单需要
def getStockCodeListCache(flage='A',reload=False):
    # 股票选择器缓存处理
    key = KEYFIX + "_" + flage + '_stockoption'
    optionList = cache.get(key)
    # logging.info('cachestockcodes in cache: %s', cachestockcodes)

    if optionList is None:
        reload=True

    if reload:
        dataset = cache_db.getStockInfoCache(flage,reload=reload)
        dataset = dataset.loc[:, ['STOCKCODE', 'NAME']].values
        stockcode = dataset[:, 0].tolist()
        tip = []
        for info in dataset:
            tip.append('{}({})'.format(info[0], info[1]))
        result = {'stockcode': stockcode, 'tip': tip}

        # 拼接option标签
        optionList = ["<option value='" + k + "'>" + v + "</option>" for k, v in zip(result['stockcode'], result['tip'])]
        # optionList = ["<option value='%s'>%s</option>" % (k,v) for k, v in zip(result['stockcode'], result['tip'])]
        # optionList = ["<option value='{}'>{}</option>".format(k,v) for k, v in zip(result['stockcode'], result['tip'])]

        # 添加股票选择列表至缓存中
        cache.set(key, optionList, CACHEUTILS_UPDATE_SECENDS)
    return optionList


def getStockRangeMenu(parentId="#",reload=False):
    """
    股票范围展示树形菜单
    """
    key = KEYFIX + "_"+parentId+"_stockRangeMenu"
    factorMenus = cache.get(key)
    if factorMenus is None:
        reload=True
    if reload:
        factorMenus = cache_build.buildStockRangeMenu(parentId)
        cache.set(key, factorMenus, CACHEUTILS_UPDATE_SECENDS)
    return factorMenus


def getStockRangeMenuv2(parentId="#",reload=False):
    """
    股票范围展示树形菜单
    """
    key = KEYFIX + "_"+parentId+"_stockRangeMenuv2"
    factorMenus = cache.get(key)
    if factorMenus is None:
        reload=True
    if reload:
        factorMenus = cache_build.buildStockRangeMenuv2(parentId)
        cache.set(key, factorMenus, CACHEUTILS_UPDATE_SECENDS)
    return factorMenus

def getFactorMenus(menuType="1",reload=False):
    """
    指标展示树形菜单
    menuType #指标类型：1：旧指标；2：新指标
    """
    key = KEYFIX + "_factorMenus_"+menuType
    factorMenus = cache.get(key)
    if factorMenus is None:
        reload=True
    if reload:
        factorMenus = cache_build.getFactorMenus(menuType)
        cache.set(key, factorMenus, CACHEUTILS_UPDATE_SECENDS)
    return factorMenus


def getIndexTreeCache(indexType,reload=False):
    """
    获取指标树数据
    """
    key = KEYFIX + '_indexTree_' + str(indexType)
    allCategoryIndexList = cache.get(key)
    if allCategoryIndexList is None:
        reload=True
    if reload:
        topIndexCategoryPdData = manager_index_mode.getCategorySubIndex(parentId='#', indexType=indexType)
        topIndexCategoryList = [{'id': row.MENUID, 'text': row.MENUNAME} for idx, row in topIndexCategoryPdData.iterrows()]
        allCategoryIndexList = manager_index_mode.getAllCategoryIndexList(topIndexCategoryList, indexType)
        # allCategoryIndexList = sorted(allCategoryIndexList, key=lambda x: x['id'])
        cache.set(key,allCategoryIndexList,CACHEUTILS_UPDATE_SECENDS)
    return allCategoryIndexList

def flushFactorCellCache(reload=True):
    """
    刷新指标缓存
    """
    cache_db.getCacheTableAllDataDF(schemaTableName='FACTORCELL',reload=reload)


def flushIndexTreeCache(indexType,reload=True):
    """
    刷新指标树数据缓存
    """
    # key = KEYFIX + "_indexTree_" + str(indexType)
    # cache.delete(key)
    getIndexTreeCache(indexType,reload=reload)

def delBaseModeCache():
    # 数据库数据同步完成后，清空缓存
    count = cache.delete_pattern(KEYFIX + "_*")
    return count


def loadBaseModeCache():
    """
    数据库数据同步完成后,获取最新数据
    """
    resultData = ResultData()
    try:
        getStockCodeListCache(flage='A',reload=True)
        getStockCodeListCache(flage='ALL',reload=True)
        getStockRangeMenu(reload=True)
        getStockRangeMenuv2(reload=True)
        getFactorMenus(menuType="1",reload=True)
        getFactorMenus(menuType="2",reload=True)
        getIndexTreeCache(indexType="1",reload=True)
        getIndexTreeCache(indexType="2",reload=True)
    except Exception as ex:
        resultData.errorData(errorMsg="stockBaseCache刷新异常")
        Logger.error("stockBaseCache刷新异常")
        Logger.errLineNo()
    return resultData


if __name__ == '__main__':
    # loadBaseModeCache()


    df = getIndexTreeCache(indexType=1,reload=True)
    print(df)
    #计算工作日
    #''2018

    # import datetime
    # begin = datetime.datetime.today()+datetime.timedelta(days=-4)
    #
    # addDay=6
    # for i in range(1,1):
    #     end=begin+datetime.timedelta(days=addDay)
    #     # print(1461+i,datetime.datetime.strftime(begin,'%Y%m%d'),datetime.datetime.strftime(end,'%Y%m%d'))
    #     if i==1:
    #         weekid="201753"
    #     else:
    #         weekid=str(begin.year)+str(i-1).zfill(2)
    #
    #     weekno=1461+i
    #     print("insert into workweek(weekno,weekid,weekbegin,weekend) values(%d,'%s','%s','%s')" % (weekno,weekid,datetime.datetime.strftime(begin,'%Y%m%d'),datetime.datetime.strftime(end,'%Y%m%d')))
    #     end=end+datetime.timedelta(days=1)
    #     begin=end
    #     if begin.year > 2018:
    #         break
    # pass
