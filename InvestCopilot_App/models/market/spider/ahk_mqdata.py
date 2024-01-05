# -*- coding:utf-8 -*-
# 东财高频行情获取，按地区分表存储，主要是获取分时数据，每隔1分钟一个点


import sys
import os
import requests
import traceback
import datetime
import time
import json
import collections
import socket

sys.path.append("../../../..")

from InvestCopilot_App.models.toolsutils import ToolsUtils as toolutils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.market.snapRedis import cacheTools
# import process.spider.other.japan as japan
# import process.spider.other.japan_indexcode as japan_index
from InvestCopilot_App.models.market.spider import eastUtis as east_utils
from InvestCopilot_App.models.market import marketUtils as market_utils
from InvestCopilot_App.models.toolsutils import sendmsg as sendmsg
import pandas as pd
import pickle

#import zlib
#r.set("key", zlib.compress( pickle.dumps(df)))
#df=pickle.loads(zlib.decompress(r.get("key")))

def addFutSettlementPrice(mkDF):
    """
    指数期货结算行情
    :param con:
    :param cur:
    :return:
    """
    try:
        if not mkDF.empty:
            tdate = str(mkDF.iloc[0].TRADEDATE)
            tcodes = mkDF['STOCKCODE'].values.tolist()
            d_his="delete from spider.futures_market where tradedate=%s and eastcode in ({})".format(dbutils.getQueryInParam(tcodes))
            i_data="insert into  spider.futures_market (eastcode,closeprice,preSettlementprice,tradedate,area,updatetime) values (%s,%s,%s,%s,%s,current_timestamp) "
            con, cur=dbutils.getConnect()
            cur.execute(d_his,[tdate])
            cur.executemany(i_data,mkDF.values.tolist())
            con.commit()
            cur.close()
            con.close()
    except:
        print(traceback.format_exc())
        sendmsg.send_wx_msg_operation('HK fut 结算行情数据保存异常[%s]'%(traceback.format_exc()))
    finally:
        try:
            if cur is not None:
                cur.close()
            if con is not None:
                con.close()
        except:pass



def copyToIndexMarket(tradeDate,indexCode='000300.SH'):
    """
    index 指数归档
    :param con:
    :param cur:
    :return:
    """
    try:
        con, cur=dbutils.getConnect()
        q_sql="select count(1) from spider.emminhq_index where stockcode=%s AND TRADEDATE=%s"
        cur.execute(q_sql,[indexCode,tradeDate])
        findCount=cur.fetchall()[0][0]
        if findCount>0:
            d_sql="delete from SPIDER.INDEXMARKET where stockcode=%s AND TRADEDATE=%s "
            cur.execute(d_sql,[indexCode,tradeDate])
            copy_sql="insert into SPIDER.INDEXMARKET  (stockcode, tradedate, tradetime, nowprice, pctchange, high, low, open, preclose, roundlot, change, volume, amount)" \
                     " select  stockcode, tradedate, tradetime, nowprice, pctchange, high, low, open, preclose, roundlot, change, volume, amount from  spider.emminhq_index  where stockcode=%s AND TRADEDATE=%s"
            cur.execute(copy_sql,[indexCode,tradeDate])
            con.commit()
    except:
        print(traceback.format_exc())
        sendmsg.send_wx_msg_operation('indexMarket [%s][%s]归档异常[%s]'%(indexCode,tradeDate,traceback.format_exc()))
    finally:
        try:
            cur.close()
            con.close()
        except:pass

def insertDB2(con, cur, dataList, flushData=False):
    sqlstr1 = "insert into spider.emminhq_index " \
              "(stockcode, tradedate, tradetime, nowprice, pctchange, high, low, open, preclose, roundlot, change, volume, amount,area)" \
              " values (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # 全量更新
    if flushData:
        d_sql = "delete from  spider.emminhq_index  where tradedate=%s "
        tradeDate = dataList[0][1]
        cur.execute(d_sql, [tradeDate])
        # print("del:",cur.rowcount)
    cur.executemany(sqlstr1, dataList)
    # print("add:", cur.rowcount)
    con.commit()



def insertDB(con, cur, dataList, area='CN'):
    eminhq_table_dict = {
        "CN": "spider.eminhq_cn",
        "HK": "spider.eminhq_hk",
        "USA": "spider.eminhq_usa",
        "OTHER": "spider.eminhq_other",
    }

    aa_table = eminhq_table_dict[area]
    sqlstr1 = "insert into {} " \
              "(stockcode, tradedate, tradetime, nowprice, pctchange)" \
              " values (%s,to_char(current_date ,'YYYYMMDD'),%s,%s, %s)".format(aa_table)

    cur.executemany(sqlstr1, dataList)
    con.commit()


def insertMarketByTime(con, cur, dataList):
    sqlstr1 = "insert into spider.emminhq_time " \
              "(stockcode, tradedate, tradetime, nowprice, pctchange,area,status)" \
              " values (%s,%s,%s,%s,%s,%s,%s)"
    cur.executemany(sqlstr1, dataList)
    con.commit()



def insertCHstockmarket(con, cur, dataList):
    sqlstr1 = "insert into spider.CHstockmarket " \
              "(stockcode, tradedate, s_dq_close, s_dq_preclose)" \
              " values (%s,%s,%s,%s)"
    cur.executemany(sqlstr1, dataList)
    con.commit()


def _test_():
    mpi = "2000"  # 2秒刷新一次
    # 股票列表，0：深市，1：沪市
    secids = "0.002274,1.600864,1.603019,0.300297,0.300077,0.300188,1.603986,0.000938,0.002600"
    # fields 获取指列
    fields = "f12,f13,f19,f14,f139,f2,f4,f1,f3,f152,f5,f30,f31,f18,f32,f6,f8,f7,f10,f22,f9,f112,f100"
    url = "https://88.push2.eastmoney.com/api/qt/ulist/sse?invt=3&pi=0&pz=9&mpi={}&" \
          "secids={}&ut=6d2ffaa6a585d612eda28417681d58fb&fields={}&po=1".format(mpi, secids, fields)
    url = "https://88.push2.eastmoney.com/api/qt/ulist/sse?" \
          "invt=3&" \
          "pi=0&" \
          "pz=9&" \
          "mpi=2000&" \
          "secids=0.002274,1.600864,1.603019,0.300297,0.300077,0.300188,1.603986,0.000938,0.002600&" \
          "ut=6d2ffaa6a585d612eda28417681d58fb&" \
          "fields=f12,f13,f19,f14,f139,f2,f4,f1,f3,f152,f5,f30,f31,f18,f32,f6,f8,f7,f10,f22,f9,f112,f100&" \
          "po=1"
    # url="http://push2.eastmoney.com/api/qt/ulist.np/get?secids=1.000001,0.399001&fields=f104,f105,f106&ut=6d2ffaa6a585d612eda28417681d58fb&cb=jQuery33103276047253713332_1556156446679&_=1556156446719"
    # url=url+"&cb=jQuery33103276047253713332_1556156446679&_=1556156446719"
    url = "http://push2.eastmoney.com/api/qt/pkyd/sse?lmt=9&fields=f1,f2,f3,f4,f5,f6,f7&ut=6d2ffaa6a585d612eda28417681d58fb"
    url = "http://push2.eastmoney.com/api/qt/pkyd/sse?lmt=9&fields=f1,f2,f3,f4,f5,f6,f7&ut=6d2ffaa6a585d612eda28417681d58fb&secids=0.002274,1.600864,1.603019,0.300297,0.300077,0.300188,1.603986,0.000938,0.002600"
    # 指数实时接口 开始成功
    # "1.000001,0.399001,0.399006,0.399005,8.040120,104.CN00Y,100.HSI,100.N225,100.FTSE,100.DJIA,100.NDX,100.GDAXI,102.CL00Y,101.GC00Y,100.UDI,133.USDCNH,120.USDCNYC,142.scm"
    secids = "1.000001,0.399001,0.399006,0.399005"
    fields = "f13,f12,f14,f2,f3,f4,f15,f16,f17,f6,f30,f5,f10,f51,f52,f53,f54,f55,f56,f57,f58"
    # odl http://push2.eastmoney.com/api/qt/ulist.np/get?secids={}&fields=f1,f2,f3,f4,f12,f13,f14,f107&ut=6d2ffaa6a585d612eda28417681d58fb&cb=jQuery3310018081068693506452_1556158723273&_=1556168723430
    url = "http://push2.eastmoney.com/api/qt/ulist.np/get?secids={}&fields={}&fields2=f51,f52,f53,f54,f55,f56,f57,f58&" \
          "ut=6d2ffaa6a585d612eda28417681d58fb" \
          "&_=1556168723430".format(
        secids, fields)
    # 指数实时接口 结束，成功
    s = east_utils.toRequest(url)
    print("s:", s)
    s = east_utils.findRsData(s)
    s = east_utils.parseFactorData(s, fields)
    print(s)
    return
    """
    f1:地区：2:大陆;3:香港
    f2:最新价
    f3:涨跌幅
    f4:涨跌额
    f5:总手
    f30:现手
    f31:买入价
    f32:卖出价
    f18:昨收
    f6:成交额
    f8:换手率
    f7:振幅
    f10:量比
    f22:涨速
    f9:市盈率
    f112:每股收益
    f100:所属行业板块
    f15:最高价,
    f16：最低价,
    f17：开盘价
    """
    fieldsUnits = {"f2": -100, "f3": -100, "f4": -100}
    # f12:股票代码,f13:市场,f14:股票名称
    fields = "f12,f13,f19,f14,f139,f2,f4,f1,f3,f152,f5,f30,f31,f18,f32,f6,f8,f7,f10,f22,f9,f112,f100"
    # fields = "f12,f14,f2,f3,f4"
    secids = "0.002274,1.600864,116.00700,105.JD"
    # stockList = getStockList()
    # pageCodeList = pageStockList(stockList)
    # print(len(pageCodeList))
    # secids = stockList[0:100]
    # secids=stockList
    # secids = ",".join(secids)
    url = "http://push2.eastmoney.com/api/qt/ulist.np/get?" \
          "secids={}&fields={}&" \
          "ut=6d2ffaa6a585d612eda28417681d58fb&_=1556168723430".format(secids, fields)

    bt = datetime.datetime.now()
    rs = toRequest(url)
    et = datetime.datetime.now()
    print("耗时：", (et - bt).seconds)
    import json
    rs = json.loads(rs)
    # print(type(rs), rs)
    rsDict = rs['data']
    total = rsDict['total']
    diff = rsDict['diff']
    rows = []
    for d in diff:
        row = []
        dt = sorted(d.items(), key=lambda item: int(str(item[0])[1:]))
        print("dt:", dt)
        odDict = collections.OrderedDict()
        for d in dt:
            odDict[d[0]] = d[1]
        d = odDict
        for f in fields.split(","):
            if f in fieldsUnits:
                unit = fieldsUnits[f]
            else:
                unit = 0
            nd = d[f]
            if unit < 0:
                nd = nd / abs(unit)
                row.append(nd)
            elif unit > 0:
                nd = nd * unit
                row.append(nd)
            else:
                row.append(nd)
        rows.append(row)

    print(len(rows), rows)
    dataDF = pd.DataFrame(diff, columns=fields.split(","))
    dataDF.to_excel("d:/test/eastfactor.xlsx")


def parserFactor():
    # http://quote.eastmoney.com/zixuan/?from=home
    import bs4
    text = '<select class="zbsselect sscroll" multiple="" id="zbssel2"><option value="f2">最新价</option><option value="f3">涨跌幅</option><option value="f4">涨跌额</option><option value="f5">总手</option><option value="f30">现手</option><option value="f31">买入价</option><option value="f32">卖出价</option><option value="f18">昨收</option><option value="f6">成交额</option><option value="f8">换手率</option><option value="f7">振幅</option><option value="f10">量比</option><option value="f22">涨速</option><option value="f9">市盈率</option><option value="f112">每股收益</option><option value="f100">所属行业板块</option><option value="f15">最高价</option><option value="f16">最低价</option><option value="f17">开盘价</option><option value="f102">所属地区板块</option><option value="f103">所属概念板块</option><option value="f62">主力净流入</option><option value="f63">集合竞价</option><option value="f64">超大单流入</option><option value="f66">超大单净额</option><option value="f65">超大单流出</option><option value="f69">超大单净占比</option><option value="f70">大单流入</option><option value="f71">大单流出</option><option value="f72">大单净额</option><option value="f75">大单净占比</option><option value="f76">中单流入</option><option value="f77">中单流出</option><option value="f78">中单净额</option><option value="f81">中单净占比</option><option value="f82">小单流入</option><option value="f83">小单流出</option><option value="f84">小单净额</option><option value="f87">小单净占比</option><option value="f88">当日DDX</option><option value="f89">当日DDY</option><option value="f90">当日DDZ</option><option value="f91">5日DDX</option><option value="f92">5日DDY</option><option value="f94">10日DDX</option><option value="f95">10日DDY</option><option value="f97">DDX飘红天数(连续)</option><option value="f98">DDX飘红天数(5日)</option><option value="f99">DDX飘红天数(10日)</option><option value="f38">总股本</option><option value="f39">流通股</option><option value="f36">人均持股数</option><option value="f113">每股净资产</option><option value="f37">净资产收益率(加权)</option><option value="f40">营业收入</option><option value="f41">营业收入同比</option><option value="f42">营业利润</option><option value="f43">投资收益</option><option value="f44">利润总额</option><option value="f45">净利润</option><option value="f46">净利润同比</option><option value="f47">未分配利润</option><option value="f48">每股未分配利润</option><option value="f49">毛利率</option><option value="f50">总资产</option><option value="f51">流动资产</option><option value="f52">固定资产</option><option value="f53">无形资产</option><option value="f54">总负债</option><option value="f55">流动负债</option><option value="f56">长期负债</option><option value="f57">资产负债比率</option><option value="f58">股东权益</option><option value="f59">股东权益比</option><option value="f60">公积金</option><option value="f61">每股公积金</option><option value="f26">上市日期</option></select>'
    text = toolutils.strEncode(text)
    sl = bs4.BeautifulSoup(text, "html.parser")
    options = sl.findAll("option")
    factorIndex = {}
    for o in options:
        factorIndex[o.attrs['value']] = o.text
        print(o.attrs['value'] + ":" + o.text)
    print(factorIndex)
    return factorIndex


def getStockTradeStatus():
    # 获取股票交易状态，停牌
    pass

market_key = "east_all_realMarketDF"
usa_market = "east_usa_realMarketDF"
cn_market = "east_cn_realMarketDF"
hk_market = "east_hk_realMarketDF"
other_market = "east_other_realMarketDF"
wind_market = "wind_other_realMarketDF"
index_market = "east_index_realMarketDF"
cn_suspension = "east_cn_suspensionDF"#A股停牌

def modifyStockMarket(modifyDF, redis):
    # 更新缓存中数据
    # mdDF = cnDF.append(hkDF)
    df_bytes_from_redis = redis.get(market_key)
    baseDF = pickle.loads(df_bytes_from_redis)
    md_dict = {}
    for idx, row in modifyDF.iterrows():
        stockCode = row.STOCKCODE
        md_dict[stockCode] = row
    for idx, row in baseDF.iterrows():
        stockCode = row.STOCKCODE
        if stockCode in md_dict:
            baseDF.iloc[idx] = md_dict[stockCode]
    newDF = baseDF[['STOCKCODE', 'F2', 'F3', 'TRADEDATE', 'WORKTIME', 'AREA','STATUS']]
    newDF = newDF.reset_index(drop=True)
    redis.set(market_key, newDF.to_msgpack())

def ckSaveOrUpdateData(_redis,rtDF, area='USA'):
    """
    检查获取的高频数据是否完整
    """
    #将数据写入缓存
    if not rtDF.empty:
        if area in ['USA']:
            _redis.set(usa_market, pickle.dumps(rtDF))
        elif area in ['CN']:
            _redis.set(cn_market, pickle.dumps(rtDF))
        elif area in ['HK']:
            _redis.set(hk_market, pickle.dumps(rtDF))
        return rtDF
    #从缓存获取
    if area in ['USA']:
        usa_df_bytes_from_redis = _redis.get(usa_market)
        usaDF = pickle.loads(usa_df_bytes_from_redis)
        return usaDF
    elif area in ['CN']:
        cn_df_bytes_from_redis = _redis.get(cn_market)
        cnDF = pickle.loads(cn_df_bytes_from_redis)
        _redis.set(cn_market, pickle.dumps(cnDF))
        return cnDF
    elif area in ['HK']:
        hk_df_bytes_from_redis = _redis.get(hk_market)
        hkDF = pickle.loads(hk_df_bytes_from_redis)
        _redis.set(hk_market, pickle.dumps(hkDF))
        return hkDF
    return rtDF

def spiderData(host):
    pid = os.getpid()
    vhostname=socket.gethostname()
    exeDate = datetime.datetime.now().strftime("%Y%m%d")
    workDay = market_utils.getWorkDate()
    isATrade = False
    # A股交易日
    if exeDate == workDay:
        isATrade = True

    # 港股交易日
    isHKTrade = market_utils.isHkWorkDay(exeDate)
    redis = cacheTools(decode_responses=False,host=host)

    # 停牌股票检查
    tp_dict=east_utils.getStopOrTradingStocks()
    redis.set(cn_suspension, json.dumps(tp_dict))
    if not isATrade and not isHKTrade:
        sendmsg.send_wx_msg_operation("[%s]非工作日东财高频行情爬虫不启动"%(vhostname))
        # 非工作日，获取全量行情
        WorkTime = time.strftime('%H%M%S', time.localtime())
        bt = datetime.datetime.now()
        baseDF = pd.DataFrame([], columns=['STOCKCODE', 'F2', 'F3', 'F18','TRADEDATE', 'WORKTIME', 'AREA','STATUS'])
        usaDF = east_utils.getStockRealMarket(WorkTime, area='USA')
        usaDF=ckSaveOrUpdateData(redis,usaDF,area='USA')
        cnDF = east_utils.getStockRealMarket(WorkTime, area='CN',suspensionDict=tp_dict)
        cnDF=ckSaveOrUpdateData(redis,cnDF,area='CN')
        hkDF = east_utils.getStockRealMarket(WorkTime, area='HK')
        hkDF=ckSaveOrUpdateData(redis,hkDF,area='HK')
        # otherDF = japan.copyOtherMarket(WorkTime)
        # redis.set(other_market,otherDF.dumps(otherDF))
        #wind
        # windDF = japan_index.copyOtherMarket(WorkTime)
        # redis.set(wind_market, pickle.dumps(windDF))
        bt_1 = datetime.datetime.now()
        east_utils.soutLog("get market , 耗时:%ss" % ((bt_1 - bt).seconds))
        baseDF = pd.concat([baseDF, usaDF, cnDF, hkDF], ignore_index=True)#, otherDF,windDF
        # print("baseDF:",len(baseDF),"cnDF:",len(cnDF),"usaDF:",len(usaDF),"otherDF:",len(otherDF))
        # baseDF.to_excel("/Users/xiaxuhong/work/temp/out/baseDF1.xlsx")
        # 将结果放入缓存
        redis.set(market_key, pickle.dumps(baseDF))
        bt_3 = datetime.datetime.now()
        # soutLog("放入缓存, 耗时:%ss" % ((bt_3 - bt_2).seconds))
        east_utils.soutLog("[%s]非工作日全部获取行情数据, 耗时:%ss" % ((bt_3 - bt).seconds,vhostname))
        return
    else:
        sendmsg.send_wx_msg_operation("pid[%s][%s]启动东财高频行情爬虫,isATrade[%s],isHKTrade[%s]" % (pid,vhostname,isATrade, isHKTrade))
    isUSA = True

    isIndexFlag=True
    while True:
        try:
            WorkTime = time.strftime('%H%M%S', time.localtime())
            if WorkTime > "163500":
                break
            # print("WorkTime:",WorkTime)
            if WorkTime > "161500":
                # sendmsg.send_wx_msg("关闭东财高频行情爬虫")
                #获取全部指数
                if isATrade and isIndexFlag:
                    # indexDF = east_utils.getIndexChartData2(area='CN',indexCodeStr = "1.000001,0.399001,0.399006,1.000300,0.399905")  # 指数
                    # if not indexDF.empty:
                        # 放入缓存
                        # redis.set(index_market, pickle.dumps(indexDF))
                        # isIndexFlag=False
                    if isHKTrade:
                        #港股期货结算价格
                        # stockList = east_utils.getStockList(area='HK', dataType='fut')
                        # futCodes = [str(s).split(".")[1] for s in stockList]
                        # cacheHKDF = pickle.loads(redis.get(hk_market))
                        # futhkDF = cacheHKDF[cacheHKDF['STOCKCODE'].isin(futCodes)]
                        # futhkDF = futhkDF[['STOCKCODE', 'F2', 'F18', 'TRADEDATE','AREA']]
                        # addFutSettlementPrice(futhkDF)
                        pass
            if isUSA:
                # 触发一次  保证 emminhq表有数据，取最新同步到emminhq2表中，再更新至缓存。
                bt = datetime.datetime.now()  # ['STOCKCODE', 'F2', 'F3', 'F18', 'TRADEDATE', 'WORKTIME', 'AREA', 'STATUS']
                baseDF = pd.DataFrame([], columns=['STOCKCODE', 'F2', 'F3', 'F18', 'TRADEDATE', 'WORKTIME', 'AREA',
                                                   'STATUS'])
                usaDF = east_utils.getStockRealMarket(WorkTime, area='USA')
                usaDF=ckSaveOrUpdateData(redis,usaDF,area='USA')
                cnDF = east_utils.getStockRealMarket(WorkTime, area='CN', suspensionDict=tp_dict)
                cnDF=ckSaveOrUpdateData(redis,cnDF,area='CN')
                hkDF = east_utils.getStockRealMarket(WorkTime, area='HK')
                hkDF = ckSaveOrUpdateData(redis, hkDF, area='HK')
                # otherDF = japan.copyOtherMarket(WorkTime)
                # redis.set(other_market, pickle.dumps(otherDF))
                # wind
                # windDF = japan_index.copyOtherMarket(WorkTime)
                # redis.set(wind_market, pickle.dumps(windDF))
                bt_1 = datetime.datetime.now()
                east_utils.soutLog("get market , 耗时:%ss" % ((bt_1 - bt).seconds))
                baseDF = pd.concat([baseDF, usaDF, cnDF, hkDF], ignore_index=True)#, otherDF, windDF
                # print("baseDF:",len(baseDF),"cnDF:",len(cnDF),"usaDF:",len(usaDF),"otherDF:",len(otherDF))
                # baseDF.to_excel("/Users/xiaxuhong/work/temp/out/baseDF1.xlsx")
                # 将结果放入缓存
                redis.set(market_key, pickle.dumps(baseDF))
                bt_3 = datetime.datetime.now()
                # soutLog("放入缓存, 耗时:%ss" % ((bt_3 - bt_2).seconds))
                east_utils.soutLog("首次全部获取行情数据, 耗时:%ss" % ((bt_3 - bt).seconds))
                isUSA = False
                time.sleep(50)
                continue

            # 所有股票
            # 港股交易日  港股会临时停牌，上午或下午停牌，手动开关
            #delete from config.hkworkday t where t.workday='20201013';insert into config.hkworkday values('20201013');
            isHKTrade = market_utils.isHkWorkDay(exeDate)
            if ((WorkTime >= '092500' and WorkTime <= '113200') or (WorkTime >= '130000' and WorkTime <= '150200')):
                t_sleep = 50
                #A股停牌股票
                bt_1 = datetime.datetime.now()
                if str(WorkTime)[2:4] in ['00']:#每隔60分钟更新一次
                    tp_dict = east_utils.getStopOrTradingStocks()
                    redis.set(cn_suspension, json.dumps(tp_dict))
                if isATrade and isHKTrade:
                    cnDF = east_utils.getStockRealMarket(WorkTime, area='CN',suspensionDict=tp_dict)
                    hkDF = east_utils.getStockRealMarket(WorkTime, area='HK')
                    # otherDF = japan.copyOtherMarket(WorkTime)
                    # wind
                    # windDF = japan_index.copyOtherMarket(WorkTime)

                    # 更新单个地区个股最新行情
                    cnDF = ckSaveOrUpdateData(redis, cnDF, area='CN')
                    hkDF = ckSaveOrUpdateData(redis, hkDF, area='HK')

                    # redis.set(other_market, pickle.dumps(otherDF))
                    # redis.set(wind_market, pickle.dumps(windDF))

                    # 所有个股行情
                    usa_df_bytes_from_redis = redis.get(usa_market)
                    usaDF = pickle.loads(usa_df_bytes_from_redis)
                    baseDF = pd.concat([cnDF, hkDF, usaDF], ignore_index=True)#, otherDF,windDF
                    # print("baseDF:",len(baseDF),"cnDF:",len(cnDF),"usaDF:",len(usaDF),"otherDF:",len(otherDF))
                    # baseDF.to_excel("/Users/xiaxuhong/work/temp/out/baseDF2.xlsx")
                    redis.set(market_key, pickle.dumps(baseDF))

                    # syncMarketByTime()  # save 分时数据
                    bt_2 = datetime.datetime.now()
                    indexDF = east_utils.getIndexChartData2(area='CN')  # 指数
                    # 放入缓存
                    if not indexDF.empty:
                        redis.set(index_market, pickle.dumps(indexDF))
                    bt_3 = datetime.datetime.now()
                    east_utils.soutLog("getIndexChartData2, 耗时:%ss" % ((bt_3 - bt_2).seconds))
                    east_utils.soutLog("总耗时:%ss" % ((bt_3 - bt_1).seconds))
                    # japan.japanMarketHandler() #获取日本行情个股（linxu 无法访问）

                    # 后台线程处理
                    # import threading
                    # threading.Thread(target=syncMarketByTime)
                    t_sleep = 45
                elif isATrade:
                    cnDF = east_utils.getStockRealMarket(WorkTime, area='CN',suspensionDict=tp_dict)
                    # otherDF = japan.copyOtherMarket(WorkTime)
                    # wind
                    # windDF = japan_index.copyOtherMarket(WorkTime)

                    # 更新单个地区个股最新行情
                    cnDF = ckSaveOrUpdateData(redis, cnDF, area='CN')

                    # redis.set(other_market, pickle.dumps(otherDF))
                    # redis.set(wind_market, pickle.dumps(windDF))

                    # 所有个股行情
                    usa_df_bytes_from_redis = redis.get(usa_market)
                    usaDF = pickle.loads(usa_df_bytes_from_redis)
                    hk_df_bytes_from_redis = redis.get(hk_market)
                    hkDF = pickle.loads(hk_df_bytes_from_redis)

                    baseDF = pd.concat([cnDF, hkDF, usaDF], ignore_index=True)#, otherDF,windDF
                    # 更新缓存中数据
                    redis.set(market_key, pickle.dumps(baseDF))

                    # syncMarketByTime()  # save 分时数据
                    bt_2 = datetime.datetime.now()
                    indexDF = east_utils.getIndexChartData2(area='CN')  # 指数
                    # 放入缓存
                    if not indexDF.empty:
                        redis.set(index_market, pickle.dumps(indexDF))
                    bt_3 = datetime.datetime.now()
                    east_utils.soutLog("getIndexChartData2, 耗时:%ss" % ((bt_3 - bt_2).seconds))
                    east_utils.soutLog("总耗时:%ss" % ((bt_2 - bt_1).seconds))

                    t_sleep = 50
                elif isHKTrade:
                    hkDF = east_utils.getStockRealMarket(WorkTime, area='HK')
                    # otherDF = japan.copyOtherMarket(WorkTime)
                    # # wind
                    # windDF = japan_index.copyOtherMarket(WorkTime)
                    # 更新单个地区个股最新行情
                    hkDF = ckSaveOrUpdateData(redis, hkDF, area='HK')
                    # redis.set(other_market, pickle.dumps(otherDF))
                    # redis.set(wind_market, pickle.dumps(windDF))
                    # 所有个股行情
                    usa_df_bytes_from_redis = redis.get(usa_market)
                    usaDF = pickle.loads(usa_df_bytes_from_redis)
                    cn_df_bytes_from_redis = redis.get(cn_market)
                    cnDF = pickle.loads(cn_df_bytes_from_redis)
                    baseDF = pd.concat([cnDF, hkDF, usaDF], ignore_index=True)#, otherDF,windDF
                    # 更新缓存中数据
                    redis.set(market_key, pickle.dumps(baseDF))
                    bt_2 = datetime.datetime.now()
                    # syncMarketByTime()  # save 分时数据
                    east_utils.soutLog("get HK stock, 耗时:%ss" % ((bt_2 - bt_1).seconds))
                    t_sleep = 55
                # bt_5 = datetime.datetime.now()
                # soutLog("function copy all emminhq2 to emminhq, 耗时:%ss" % ((bt_5 - bt_4).seconds))
                # soutLog("fetchall 耗时:%ss" % ((bt_5 - bt_1).seconds))
                time.sleep(t_sleep)
                # print("sleep 55s")
            elif (WorkTime >= '113300' and WorkTime <= '120300') or (WorkTime >= '150300' and WorkTime <= '163500'):
                if (WorkTime >= '160100' and WorkTime <= '160600'):
                    #行情波动大，不获取
                    time.sleep(55)
                    continue
                # 港股
                if isHKTrade:
                    bt_1 = datetime.datetime.now()
                    hkDF = east_utils.getStockRealMarket(WorkTime, area='HK')
                    # otherDF = japan.copyOtherMarket(WorkTime)
                    # windDF = japan_index.copyOtherMarket(WorkTime)
                    # 更新单个地区个股最新行情
                    hkDF = ckSaveOrUpdateData(redis, hkDF, area='HK')
                    # redis.set(other_market, pickle.dumps(otherDF))
                    # redis.set(wind_market, pickle.dumps(windDF))
                    # 所有个股行情
                    usa_df_bytes_from_redis = redis.get(usa_market)
                    usaDF = pickle.loads(usa_df_bytes_from_redis)
                    cn_df_bytes_from_redis = redis.get(cn_market)
                    cnDF = pickle.loads(cn_df_bytes_from_redis)
                    baseDF = pd.concat([cnDF, hkDF, usaDF], ignore_index=True)#, otherDF,windDF
                    # 更新缓存中数据
                    redis.set(market_key, pickle.dumps(baseDF))
                    bt_2 = datetime.datetime.now()
                    east_utils.soutLog("get HK stock, 耗时:%ss" % ((bt_2 - bt_1).seconds))
                    time.sleep(55)
                else:
                    time.sleep(50)
            else:
                time.sleep(55)
        except:
            error = traceback.format_exc()
            print(error)
            sendmsg.send_wx_msg_operation("pid[%s][%s][east]高频行情获取异常:"%(pid,vhostname))
            east_utils.soutLog("[%s]get stock market error:%s" % (datetime.datetime.now(),error))
            time.sleep(60)
    # 删除历史数据
    sendmsg.send_wx_msg_operation("pid[%s][%s][east]高频行情退出"%(pid,vhostname))
    east_utils.delEmminhqHis()

def syncMarketByTime(areas=[]):  # isFirst=False,isATrade=False,isHkTrade=False
    # 将行情按分钟进行处理，所有个股每隔1分钟一个行情，有用后面组合绩效的分时收益计算
    dateStr = datetime.datetime.now().strftime("%Y%m%d")
    timeStr = datetime.datetime.now().strftime("%H%M")
    timeStr = timeStr + "00"
    redis = cacheTools(decode_responses=False)
    try:
        redis.get(market_key)
        df_bytes_from_redis = redis.get(market_key)
        baseDF = pickle.loads(df_bytes_from_redis)
        baseDF['WORKTIME'] = timeStr
        baseDF['TRADEDATE'] = dateStr
        addDF = baseDF[['STOCKCODE', 'TRADEDATE', 'WORKTIME', 'F2', 'F3','AREA',  'STATUS']]
        if len(areas)>0:
            addDF=addDF[baseDF['AREA'].isin(areas)]
        # addDF = pd.DataFrame()
        # if isFirst:
        #     #首次，全部都插入
        #     addDF=baseDF
        # elif  isATrade and isHkTrade:
        #     #A股和港股都是交易日
        #     addDF=baseDF[(baseDF['AREA']=='CN')&(baseDF['AREA']=='HK')]
        # elif isATrade:
        #     addDF = baseDF[(baseDF['AREA'] == 'CN')]
        # elif isHkTrade:
        #     addDF = baseDF[(baseDF['AREA'] == 'HK')]
        #
        # if addDF.empty:
        #     return

        # stockcode, tradedate, tradetime, nowprice, pctchange,area
        snapshotTimes = ['113100', '150100']
        df_bytes_from_redis = redis.get(index_market)
        indexDF = pickle.loads(df_bytes_from_redis)
        # index指数数据保存
        if timeStr in snapshotTimes:
            # 全部更新
            flushMarket = True
            addIndexDF = indexDF
        else:
            # 增量更新
            flushMarket = False
            addIndexDF = indexDF[(indexDF['TRADEDATE'] == dateStr)
                                 & (indexDF['TRADETIME'] == timeStr)]

        con, cur = dbutils.getConnect()
        # insert stock market
        insertMarketByTime(con, cur, addDF.values.tolist())

        # index index last indexMarket
        if 'CN' in areas:
            insertDB2(con, cur, addIndexDF.values.tolist(), flushData=flushMarket)

    except:
        print(traceback.format_exc())
        try:
            cur.close()
            con.close()
        except:
            pass

if __name__ == '__main__':
    # 核心调用方法

    hostName = socket.gethostname()
    print("hostName:", hostName)
    spiderData(host="127.0.0.1")
    pass