# -*- coding:utf-8 -*-
# 东财高频行情获取，美股行情获取，主要是获取分时数据，每隔1分钟一个点
import random
import sys
import traceback
import datetime
import time
sys.path.append("../../../..")
from InvestCopilot_App.models.market.snapRedis import cacheTools
from InvestCopilot_App.models.toolsutils import dateUtils as dateUtils
from InvestCopilot_App.models.market.spider import eastUtis as east_utils
from InvestCopilot_App.models.market import marketUtils as market_utils
from InvestCopilot_App.models.toolsutils import sendmsg as sendmsg
import pandas as pd
import pickle
import os
import socket

# def getFundPositionCode():
#     import common.aly_dbutils as aly_dbutils
#     #新基金交易个股
#     q_codes="select distinct eastcode  from spider.ib_contracts ic  where symbol in (select symbol  from  portfolio.sg_position sp where sectype  in ('STK'))"
#     ibCodeDF = aly_dbutils.getPDQuery(q_codes)
#     return ibCodeDF

# def getCopilotCodes():
#     usaSymbols=[]
#     try:
#         from  process.spider.seekingalpha.seekingalpha import seekUtil
#         fsymbols, trackSymbols = seekUtil().getAlyStockPool()
#         #只需要美股
#         allSymbols=list(set(fsymbols+trackSymbols))
#         for  x  in allSymbols:
#             if str(x)[-2:] in ['.A','.O','.N']:
#                 usaSymbols.append(str(x).split(".")[0])
#         #检查列表是否已配置
#         con,cur = db_utils.getConnect()
#         q_sql = "select stockcode mqstockcode , eastcode,AREA from spider.usa_stockcode where eastCode in (:eastCodes)".replace(':eastCodes',db_utils.getQueryInParam(usaSymbols))
#         # q_sql = "select stockcode mqstockcode , eastcode from spider.usa_stockcode where eastCode in (%s)"
#         # q_codes=dbutils.getQueryInParam(eastCodes)
#         eastAMCodeDF=pd.read_sql(q_sql,con)
#         feastCodes=eastAMCodeDF['EASTCODE'].values.tolist()
#         difEastCodes = list(set(usaSymbols)-set(feastCodes))
#         addCodes=[]
#         i_id="insert inTO  spider.usa_stockcode  (stockcode,eastcode,stockname,area) values(%s,%s,%s,%s)"
#         for ecode in  difEastCodes:
#             q_id="select stockcode||'.'||areaname as stockcode,stockcode eastcode,stockname,areaname from spider.east_stock_area where stockcode=%s"
#             eastCodeID=pd.read_sql(q_id,con,params=[ecode])
#             if not eastCodeID.empty:
#                 addCodes.append(eastCodeID.values.tolist()[0])
#         if len(addCodes)>0:
#             cur.executemany(i_id,addCodes)
#             # print("addCodes:",addCodes)
#             con.commit()
#             cur.close()
#             con.close()
#     except:
#         print(traceback.format_exc())
#     return usaSymbols

def spiderData(isCombHold=False):
    try:
        pid = os.getpid()
        amTradeTime =dateUtils.amTradeSeason()
        #晚上9点~10点启动
        weekday = datetime.datetime.now().weekday()
        if weekday>4:
            return
        exeDate = datetime.datetime.now().strftime("%Y%m%d")
        isAMWork = market_utils.isUSAWorkDay(tradeDay=exeDate)
        if not isAMWork:
            sendmsg.send_wx_msg_operation("[%s][%s][USA_COMB]非美股交易日，不启动[%s]isCombHold[%s]"%(socket.gethostname(),amTradeTime,exeDate,isCombHold))
            return
        redis = cacheTools(decode_responses=False)
        sendmsg.send_wx_msg_operation("pid[%s][%s][%s][USA]东财高频行情启动isCombHold[%s]"%(pid,socket.gethostname(),amTradeTime,isCombHold))
        # 非工作日，获取全量行情
        WorkTime = time.strftime('%H%M%S', time.localtime())
        bt = datetime.datetime.now()
        baseDF = pd.DataFrame([], columns=['STOCKCODE', 'F2', 'F3', 'F18','TRADEDATE', 'WORKTIME', 'AREA','STATUS'])
        usaDF = east_utils.getStockRealMarket(WorkTime, area='USA')
        redis.set(east_utils.usa_market, pickle.dumps(usaDF))
        cnDF = east_utils.getStockRealMarket(WorkTime, area='CN')
        redis.set(east_utils.cn_market, pickle.dumps(cnDF))
        hkDF = east_utils.getStockRealMarket(WorkTime, area='HK')
        redis.set(east_utils.hk_market, pickle.dumps(hkDF))
        #A股和港股不更新
        # cn_df_bytes_from_redis = redis.get(east_utils.cn_market)
        # cnDF = pickle.loads(cn_df_bytes_from_redis)
        # hk_df_bytes_from_redis = redis.get(east_utils.hk_market)
        # hkDF = pickle.loads(hk_df_bytes_from_redis)
        # otherDF = japan.copyOtherMarket(WorkTime)
        # redis.set(east_utils.other_market, pickle.dumps(otherDF))
        #wind
        # windDF = japan_index.copyOtherMarket(WorkTime)
        # redis.set(east_utils.wind_market, pickle.dumps(windDF))
        bt_1 = datetime.datetime.now()
        east_utils.soutLog("get market , 耗时:%ss" % ((bt_1 - bt).seconds))
        baseDF = pd.concat([baseDF, usaDF, cnDF, hkDF], ignore_index=True)#, otherDF,windDF
        # print("baseDF:",len(baseDF),"cnDF:",len(cnDF),"usaDF:",len(usaDF),"otherDF:",len(otherDF))
        # baseDF.to_excel("/Users/xiaxuhong/work/temp/out/baseDF1.xlsx")
        # 将结果放入缓存
        redis.set(east_utils.market_key, pickle.dumps(baseDF))
        bt_3 = datetime.datetime.now()
        # soutLog("放入缓存, 耗时:%ss" % ((bt_3 - bt_2).seconds))
        east_utils.soutLog("[%s]获取全部行情数据, 耗时:%ss" % (amTradeTime,(bt_3 - bt).seconds))
        time.sleep(10)
        while True:
            try:
                WorkTime = time.strftime('%H%M', time.localtime())
                if  amTradeTime=="summer" and ((WorkTime >= '2130' and WorkTime <= '2400') or (WorkTime >= '0000' and WorkTime <= '0410')):
                    # if (WorkTime >= '0401' and WorkTime <= '0405'):
                    #     行情波动大，不获取
                        # time.sleep(55)
                        # continue
                    getData=True
                elif amTradeTime == "winter" and ((WorkTime >= '2230' and WorkTime <= '2400')
                    or (WorkTime > '0000' and WorkTime <= '0510')):
                    # if (WorkTime >= '0501' and WorkTime <= '0505'):
                        # 行情波动大，不获取
                        # time.sleep(55)
                        # continue
                    getData = True
                else:
                    getData=False
                if  getData:
                    bt_1 = datetime.datetime.now()
                    findStockList = []
                    if isCombHold and False:
                        # q_am = "select distinct eastcode eastcode  from config.stockinfo where windcode in ( select distinct windcode from portfolio.combhold) and area=%s"
                        # amDF = dbutils.getPDQueryByParams(q_am, params=['AM'])
                        # eastCodes = amDF['EASTCODE'].values.tolist()
                        # # print("eastCodes:",len(eastCodes),eastCodes)
                        # positionCodeDF = getFundPositionCode()
                        # positionEastCodes = positionCodeDF['EASTCODE'].values.tolist()
                        # copilotCodes =getCopilotCodes()#新闻股票池列表
                        # # print("positionEastCodes:", len(positionEastCodes), positionEastCodes)
                        # eastCodes = list(set(eastCodes + positionEastCodes + copilotCodes))
                        # # print("end eastCodes:",len(eastCodes),eastCodes)
                        # findStockList = eastCodes
                        pass
                    if len(findStockList)>0:
                        #缓存中剔除 组合数据 只更新组合行情
                        t_am_df_bytes_from_redis = redis.get(east_utils.usa_market)
                        t_amDF = pickle.loads(t_am_df_bytes_from_redis)
                        t_amDF = t_amDF[~t_amDF['STOCKCODE'].isin(findStockList)]
                        combUsaDF = east_utils.getStockRealMarket(WorkTime, findStockList=findStockList, area='USA')
                        usaDF = t_amDF.append(combUsaDF)
                    else:
                        #所有的
                        usaDF = east_utils.getStockRealMarket(WorkTime,area='USA')
                    # 更新单个地区个股最新行情
                    redis.set(east_utils.usa_market, pickle.dumps(usaDF))
                    # 所有个股行情
                    #HK
                    hk_df_bytes_from_redis = redis.get(east_utils.hk_market)
                    hkDF = pickle.loads(hk_df_bytes_from_redis)
                    #CN
                    cn_df_bytes_from_redis = redis.get(east_utils.cn_market)
                    cnDF = pickle.loads(cn_df_bytes_from_redis)
                    #JPAN
                    # other_df_bytes_from_redis = redis.get(east_utils.other_market)
                    # otherDF = pickle.loads(other_df_bytes_from_redis)
                    #index
                    # wind_df_bytes_from_redis = redis.get(wind_market)
                    # windDF = pickle.loads(wind_df_bytes_from_redis)
                    # windDF = japan_index.copyOtherMarket(WorkTime)
                    # redis.set(wind_market, pickle.dumps(windDF))
                    baseDF = pd.concat([cnDF, hkDF, usaDF], ignore_index=True)#, otherDF,windDF
                    # 更新缓存中数据
                    redis.set(east_utils.market_key, pickle.dumps(baseDF))
                    # syncMarketByTime()  # save 分时数据
                    bt_2 = datetime.datetime.now()
                    east_utils.soutLog("get HK stock, 耗时:%ss" % ((bt_2 - bt_1).seconds))
                    # print("step end;")
                    # if len(findStockList)>0:
                    #     #copy his行情
                    #     mqdata.syncMarketByTime(areas=['USA','other'])#other 包含期货行情
                    #     _st=1
                    # else:
                    #     _st=random.randint(15,25)
                    time.sleep(60)
                else:
                    time.sleep(60)
                    # sendmsg.send_wx_msg_operation("[USA]东财高频行情退出")
                    # break
                if "summer"== amTradeTime:
                    if WorkTime >= '0420'and WorkTime <= '0425':
                        sendmsg.send_wx_msg_operation("pid[%s][%s][%s][USA]东财高频行情退出isCombHold[%s]"%(pid,socket.gethostname(),amTradeTime,isCombHold))
                        break
                if WorkTime >= '0510'and WorkTime <= '0525':
                    sendmsg.send_wx_msg_operation("pid[%s][%s][%s][USA]东财高频行情退出isCombHold[%s]"%(pid,socket.gethostname(),amTradeTime,isCombHold))
                    break
            except:
                error = traceback.format_exc()
                sendmsg.send_wx_msg_operation("pid[%s][%s]isCombHold[%s][USA]东财高频行情获取异常:[%s]",(pid,socket.gethostname(),isCombHold,error))
                east_utils.soutLog("get uas stock market error:%s" % error)
                time.sleep(60)
    except:
        error = traceback.format_exc()
        sendmsg.send_wx_msg_operation("[USA]东财高频行情启动异常:"+error)
        east_utils.soutLog("get uas stock market error:%s" % error)
        time.sleep(60)


if __name__ == '__main__':
    # 核心调用方法weekday
    print("sys.argv:",sys.argv)
    isCombHold=False
    if len(sys.argv)>1:
        argv1 = sys.argv[1]
        if argv1=="1":
            isCombHold=True
    spiderData(isCombHold)