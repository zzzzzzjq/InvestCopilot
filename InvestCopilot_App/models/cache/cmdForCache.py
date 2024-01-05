#coding=utf-8
__author__ = 'Robby'
"""
    数据库表数据缓存
"""

import sys

sys.path.append("../../")

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import InvestCopilot_App.models.cache.BaseCacheUtils as base_utils

Logger = logger_utils.LoggerUtils()
base_cache = base_utils.BaseCacheUtils()

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
from InvestCopilot_App.models.cache import cacheDB as cache_db
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
import InvestCopilot_App.models.cache.mode.stockModeCache as cache_mode_base
import InvestCopilot_App.models.cache.mode.stockBaseCache as cache_stock_base
import InvestCopilot_App.models.cache.BaseCacheUtils as base_utils

def delAllCache():
    resultData=ResultData()
    #1）清除表缓存
    print("1）清除表缓存")
    resultData.delTableCacheCount = base_utils.delTableCache()
    print("..清除表缓存[%s][OK]" % resultData.delTableCacheCount)
    #1）清除表缓存
    print("2）清除字典缓存")
    resultData.delDictCacheCount = cache_dict.delDictCache()
    print("..清除字典缓存[%s][OK]" % resultData.delDictCacheCount)
    #2）清除表sql缓存
    print("3）清除表sql缓存")
    resultData.delSQLCacheCount=cache_db.delSQLCache()
    print("..加载表缓存[%s][OK]" % resultData.delSQLCacheCount)
    #3）清空股票信息数据缓存数据
    print("4）清除股票信息数据缓存数据")
    count=cache_stock_base.delBaseModeCache()
    resultData.delBaseModeCacheCount=count
    print("..加载表缓存[%s][OK]" % count)
    #4）清除个股诊断缓存数据
    print("5）清除个股诊断缓存数据")
    count=cache_mode_base.delStockModeCache()
    resultData.delStockModeCache=count
    print("..加载表缓存[%s][OK]" % count)

def loadCacheData():
   #刷新表缓存
    print("1）准备加载表缓存")
    resultData = base_cache.flushCacheData()
    print("..加载表缓存[OK]")
    # 刷新字典缓存
    print("2）加载字段缓存")
    cache_dict.loadDictCache()
    print("..加载字段缓存[OK]")
    #刷新sql查询缓存
    print("3）准备加载SQL缓存")
    cache_db.flushDBCache()
    print("..加载SQL缓存[OK]")
    #加载股票信息数据缓存数据
    print("4）准备加载股票信息数据缓存")
    lbmcRS = cache_stock_base.loadBaseModeCache()
    print("..加载股票信息数据缓存[OK]")
    resultData.lbmcRS=lbmcRS.toDict()

    #重新加载个股诊断缓存数据
    print("5）准备加载个股诊断缓存数据(加载时间会比较长，预计耗时10分钟)")
    cache_mode_base.loadStockModeCache()
    print("..加载个股诊断缓存数据[OK]")

def cmdConsole():

    isDelCache= input("是否要清除表缓存(y/n):")
    if str(isDelCache).lower() in ['y','yes']:
        delAllCache()
        print('清除缓存数据成功[OK]')

    print('准备重新加载缓存数据...')
    loadCacheData()
    print('完成所有缓存数据[OK]')

if __name__ == '__main__':
    cmdConsole()
    # import datetime
    # t=datetime.datetime.now()
    # print("begin:",datetime.datetime.now())
    # rs=cache_db.getStockPriceDF(reload=True)
    # print("size:",base_utils.getSizeOf(rs))
    # print("end:",t)
    # print("20150101 股价：",(datetime.datetime.now()-t).seconds,"s")
