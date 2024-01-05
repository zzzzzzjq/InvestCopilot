# -*- coding:utf-8 -*-
import sys

import datetime
import time
import socket
sys.path.append("../../../..")

from InvestCopilot_App.models.market.snapRedis import cacheTools
from InvestCopilot_App.models.market.spider import eastUtis as east_utils
from InvestCopilot_App.models.toolsutils import sendmsg as sendmsg
import pandas as pd
import pickle

"""
凌晨获取美股行业，主要用于组合中包含的美股收益计算
"""

def task_dayMarketData():
    sendmsg.send_wx_msg_operation("每日东财高频行情获取开始")
    redis = cacheTools(decode_responses=False)
    # 非工作日，获取全量行情
    WorkTime = time.strftime('%H%M%S', time.localtime())
    bt = datetime.datetime.now()
    baseDF = pd.DataFrame([], columns=['STOCKCODE', 'F2', 'F3',  'F18', 'TRADEDATE', 'WORKTIME', 'AREA', 'STATUS'])
    usaDF = east_utils.getStockRealMarket(WorkTime, area='USA')
    redis.set(east_utils.usa_market, pickle.dumps(usaDF))
    hostName = socket.gethostname()
    # if hostName != "iZ2vcc0k0a629n6e2al2udZ":  # 阿里云Linux
    cnDF = east_utils.getStockRealMarket(WorkTime, area='CN')
    redis.set(east_utils.cn_market, pickle.dumps(cnDF))
    hkDF = east_utils.getStockRealMarket(WorkTime, area='HK')
    redis.set(east_utils.hk_market, pickle.dumps(hkDF))
    # otherDF = japan.copyOtherMarket(WorkTime)
    # redis.set(mqdata.other_market, pickle.dumps(otherDF))
    # windDF = japan_indexcode.copyOtherMarket(WorkTime)
    # redis.set(mqdata.wind_market, pickle.dumps(windDF))
    bt_1 = datetime.datetime.now()
    # soutLog("get market , 耗时:%ss" % ((bt_1 - bt).seconds))
    baseDF = pd.concat([baseDF, usaDF, cnDF, hkDF ], ignore_index=True)
    # print("baseDF:",len(baseDF),"cnDF:",len(cnDF),"usaDF:",len(usaDF),"otherDF:",len(otherDF))
    # 将结果放入缓存
    redis.set(east_utils.market_key, pickle.dumps(baseDF))
    bt_3 = datetime.datetime.now()
    # soutLog("放入缓存, 耗时:%ss" % ((bt_3 - bt_2).seconds))
    # soutLog("非工作日全部获取行情数据, 耗时:%ss" % ((bt_3 - bt).seconds))
    sendmsg.send_wx_msg_operation("[%s]每日东财高频行情获取完毕"%hostName)
if __name__ == '__main__':
    task_dayMarketData()
