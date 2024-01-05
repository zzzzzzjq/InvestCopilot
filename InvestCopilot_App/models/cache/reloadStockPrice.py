# coding=utf-8
__author__ = 'Robby'
"""
    加载股价历史数据至缓存中
"""

import sys

sys.path.append("../../")

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import InvestCopilot_App.models.cache.BaseCacheUtils as base_utils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
import pandas as pd
import datetime
import time

Logger = logger_utils.LoggerUtils()
base_cache = base_utils.BaseCacheUtils()

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "InvestCopilot.settings")

from InvestCopilot_App.models.cache import cacheDB as cache_db


def loadStockHistPriceHandler():
    q_maxDay = "select max(tradedate ) as tradedate from newdata.ashareeodprices_his"
    while True:
        allStockPriceDF = cache_db.getStockPriceDF()
        # allStockPriceDF=allStockPriceDF[allStockPriceDF['TRADEDATE']<'20181019']
        cacheMaxDay = ''
        if not allStockPriceDF.empty:
            cacheMaxDay = allStockPriceDF['TRADEDATE'].max()

        con = dbutils.getDBConnect()
        maxDayDF = pd.read_sql(q_maxDay, con)
        con.close()
        if maxDayDF.empty:
            time.sleep(1000 * 5)
            continue

        pricesHisMaxDay = maxDayDF['TRADEDATE'].values.tolist()[0]
        print("ashareeodprices_his maxday:%s,SQL_stockPriceDF maxday: %s " % (pricesHisMaxDay, cacheMaxDay))
        if cacheMaxDay == pricesHisMaxDay:
            break

        if cacheMaxDay < pricesHisMaxDay:
            # 重新加载股价行情
            print("loadStockHistPrice 加载股价行情开始")
            b = datetime.datetime.now()
            cache_db.getStockPriceDF(reload=True)
            e = datetime.datetime.now()
            print("loadStockHistPrice 加载股价行情完成，耗时:%ss" % ((e - b).seconds))
            break

        time.sleep(1000 * 5)


if __name__ == '__main__':
    loadStockHistPriceHandler()
