import pandas as pd
import traceback
import sys
import requests
import datetime
import os
import json
import time
sys.path.append("../../../..")
from InvestCopilot_App.models.toolsutils import ToolsUtils as toolutils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.toolsutils import sendmsg as sendmsg
from InvestCopilot_App.models.market import marketUtils as market_utils

# from process.spider.east.efinance.stock.getter import get_realtime_quotes

market_key = "east_all_realMarketDF"
usa_market = "east_usa_realMarketDF"
cn_market = "east_cn_realMarketDF"
hk_market = "east_hk_realMarketDF"
other_market = "east_other_realMarketDF"
wind_market = "wind_other_realMarketDF"
index_market = "east_index_realMarketDF"
cn_suspension = "east_cn_suspensionDF"#A股停牌


def delEmminhqHis():
    try:
        hisDay = market_utils.getWorkDate(num=-2)
        d2 = "delete from spider.emminhq_time where tradedate<=%s"
        d3 = "delete from spider.emminhq_index where tradedate<=%s"
        con, cur = dbutils.getConnect()
        cur.execute(d2, [hisDay])
        cur.execute(d3, [hisDay])
        # print('delete: ', hisDay, cur.rowcount)
        con.commit()
    except:
        errorMsg = traceback.format_exc()
        # print(errorMsg)
        soutLog(errorMsg)
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass


def stockFix(row,area):
    area_id = str(row.f13)
    scode = str(row.f12)
    if area_id == '0':
        # return scode + '.SZ'
        #深圳和北京交易所
        return market_utils.getstock_windcode(row.f12)
    elif area_id == '1':#上海交易所
        return scode + '.SH'
    elif area_id == '116':
        return scode + ".HK"
    elif area_id == '100':
        #港股指数
        return area_id + "."+scode
        if area=='HK':
            return scode + ".HK"
        else:
            return scode
    #期货
    return scode



def etfFix(row):
    area_id = str(row.F13)
    scode = str(row.F12)
    if area_id == '0':
        return scode + '.SZ'
    elif area_id == '1':#上海交易所
        return scode + '.SH'

    return scode


def stockFixApi(row,area):
    area_id = str(row.f13)
    scode = str(row.f12)
    if area_id == '0':
        # return scode + '.SZ'
        #深圳和北京交易所
        return toolutils.getstock_windcode(row.f12)
    elif area_id == '1':#上海交易所
        return scode + '.SH'
    elif area_id == '128':
        return scode + ".HK"
    elif area_id == '100':
        if area=='HK':
            return scode + ".HK"
        else:
            return scode
    return scode


def getStockList(area="CN",findStockList=[],dataType=''):
    # 美股独立分开，只获取一次
    rtStockList = []
    rtEastStock_dt = {}
    if str(area).upper() == 'USA':
        # 美股分地区，需要构建股票代码
        # 105: NASDAQ 纳斯达克  106:NYSE 纽约证券交易所  107:AMEX 美国证券交易所()
        # 指标 f13 地区 areaDict = {0: 'SZ', 1: 'SH', 116: 'HK',
        # 105: 'NASDAQ', 106: 'NYSE', 107: 'AMEX'}
        #103 :NQ00Y
        con = dbutils.getDBConnect()
        if len(findStockList)>0:
            q_sql = "select eastcode,area,null stocktype from spider.usa_stockcode where eastcode in (%s)  union" \
                    " select eastcode,null area,stocktype from config.stockinfo where  area='AM' and stocktype in ('idx','fut')"%(dbutils.getQueryInParam(findStockList))
            sDF = pd.read_sql(q_sql, con)
        else:
            q_sql = "select eastcode,area,null stocktype  from spider.usa_stockcode where  windcode is not null" \
                    "  union select eastcode ,null area,stocktype from config.stockinfo where  area='AM' and stocktype in ('idx','fut')"
            sDF = pd.read_sql(q_sql, con)
        con.close()
        areaDict = {'O': "105", 'N': "106", 'A': "107", 'I': "100", 'PS': "153"}#153：美國粉单市场
        sDF=toolutils.dfColumUpper(sDF)
        for r in sDF.itertuples():
            eastcode = r.EASTCODE
            area = r.AREA
            if pd.isnull(area):
                wdcode =eastcode
            else:
                fix = areaDict[area]
                wdcode = fix + "." + eastcode
            rtStockList.append(wdcode)
            rtEastStock_dt[wdcode]=eastcode
        return rtStockList,rtEastStock_dt
    elif area=='CN':
        con = dbutils.getDBConnect()
        #A股B股
        if len(findStockList) > 0:
            q_sql = "select  * from (select S_INFO_WINDCODE from newdata.asharedescription t where substr(s_info_code,1,1)<>'A'  and S_INFO_DELISTDATE is null" \
                    " union select eastcode as S_INFO_WINDCODE from spider.eaststockcode where enable is null ) as x where x.S_INFO_WINDCODE in  (%s)" % (
                        dbutils.getQueryInParam(findStockList))
            Adata = pd.read_sql(q_sql, con)
        else:
            q_sql = "select S_INFO_WINDCODE,null stocktype from newdata.asharedescription t where substr(s_info_code,1,1)<>'A'  and S_INFO_DELISTDATE is null" \
                    " union select eastcode as S_INFO_WINDCODE,stocktype from config.stockinfo where  area='CH' and stocktype in ('idx','fut')  "#期货 或 B股
            Adata = pd.read_sql(q_sql, con)
        con.close()
        AnewStockList = []
        Adata=toolutils.dfColumUpper(Adata)
        for adt in Adata.itertuples():
        # AstockList = Adata['S_INFO_WINDCODE'].values.tolist()
            wdcode=adt.S_INFO_WINDCODE
            stockType=adt.STOCKTYPE    #只有 spider.eaststockcode 表才会有数据
            # for wdcode in AstockList:
            _suffix=wdcode[-3:]
            if _suffix in [".SH"]:
                wdcode = "1." + wdcode[0:6]
            elif _suffix in [".BJ",".SZ"]:
                wdcode = "0." + wdcode[0:6]
            # if not pd.isnull(stockType):
                # if str(stockType) in ['fut']:
                #     A股期货
                    # wdcode = "8." + wdcode[0:6]#延迟数据
            AnewStockList.append(wdcode)
            rtEastStock_dt[wdcode] = adt.S_INFO_WINDCODE
            # 添加指数：
            # if area == 'CN':
            #     AnewStockList.append('1.000300')  # 沪深300指数
            # print('1.000300')
        return AnewStockList,rtEastStock_dt
    elif area=='HK':
        con = dbutils.getDBConnect()
        if len(findStockList) > 0:
            q_hk = "select windcode  as S_INFO_WINDCODE ,eastcode as eastcode,stocktype from config.stockinfo where area ='HK' and eastcode in (%s)"%(dbutils.getQueryInParam(findStockList))
            if dataType=='fut':
                q_hk=q_hk+" and stocktype='fut'"
            Hdata = pd.read_sql_query(q_hk, con)
        else:
            q_hk = "select windcode as S_INFO_WINDCODE ,eastcode as eastcode,stocktype from config.stockinfo where area ='HK' "
            if dataType=='fut':
                q_hk=q_hk+" and stocktype='fut'"

            Hdata = pd.read_sql(q_hk, con)
        con.close()

        Hdata=toolutils.dfColumUpper(Hdata)
        # idx_DF=pd.DataFrame([['HSI.HK','idx'],['06862.HK','hk']],columns=['S_INFO_WINDCODE','STOCKTYPE'])
        # Hdata=Hdata.append(idx_DF)
        # Hdata=idx_DF
        # HstockList = Hdata['S_INFO_WINDCODE'].values.tolist()
        HnewStockList = []
        for hk_if in Hdata.itertuples():#HstockList:
            hkCode=hk_if.S_INFO_WINDCODE
            hkEastCode=hk_if.EASTCODE
            if ".HK" in hkCode:
                hkcode = hkCode.replace(".HK", '')
                hkcode = "116." + str(hkcode).zfill(5)
                HnewStockList.append(hkcode)
                rtEastStock_dt[hkcode] = hkEastCode
            else:
                if str(hk_if.STOCKTYPE)=='idx':
                    #指数
                    # hkcode = hkCode.replace(".HK", '')
                    # hkcode = "100." + str(hkcode)
                    HnewStockList.append(hkEastCode)
                    rtEastStock_dt[hkEastCode] = hkEastCode
                elif str(hk_if.STOCKTYPE)=='fut':
                    #期货
                    # hkcode = hkEastCode.replace(".HK", '')
                    # hkcode = "134." + str(hkcode)
                    HnewStockList.append(hkEastCode)
                    rtEastStock_dt[hkEastCode] = hkEastCode
                else:
                    continue
        return  HnewStockList ,rtEastStock_dt
    return rtStockList ,rtEastStock_dt


def pageStockList(stockList, pageSize=300):
    allSize = len(stockList)
    if allSize % pageSize > 0:
        page = allSize // pageSize + 1
    else:
        page = allSize // pageSize

    pageSList = []
    for page in range(page):
        pageSList.append(stockList[page * pageSize:pageSize * (page + 1)])
    # print(pageSList)
    return pageSList



def toRequest(url):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        # "Host": "push2.eastmoney.com",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36"}
    # print(url)
    rsp = requests.get(url, headers=headers, timeout=(20, 30))
    # rsp=requests.get(url)
    # print(rsp)
    # print(rsp.text)
    return rsp.text



def parseFactorData(data, fields):
    # 解析指标数据
    # f13 地区 areaDict = {0: 'SZ', 1: 'SH', 116: 'HK',
    # USA: 105: NASDAQ 纳斯达克  106:NYSE 纽约证券交易所  107:AMEX 美国证券交易所()
    # f2:最新价    f3:涨跌幅    f4:涨跌额
    # 港股股价要/1000 ,A股都/1000
    fieldsUnits = {0: {"f2": -100, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -100},
                   1: {"f2": -100, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -100},
                   116: {"f2": -1000, "f3": -10000, "f4": -1000, "f15": -1000, "f16": -1000, "f17": -1000, "f18": -1000},
                   105: {"f2": -100, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -100},
                   153: {"f2": -100, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -100},
                   106: {"f2": -100, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -100},
                   107: {"f2": -100, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -100},
                   100: {"f2": -100, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -100},#港股指数
                   134: {"f2": 1, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": 1},#港股期货
                   8: {"f2": -10, "f3": -10000, "f4": -100, "f15": -100, "f16": -100, "f17": -100, "f18": -10},#A股期货
                   }
    rows = []
    fetchFields = fields.split(",")
    if not 'f13' in fetchFields:
        raise Exception("获取指标必须包含f13")
    for d in data:
        row = []
        # print("d:", d)
        for f in fetchFields:
            area = d['f13']
            if area in fieldsUnits:
                formatUnitis = fieldsUnits[area]
                if f in formatUnitis:
                    unit = formatUnitis[f]
                else:
                    unit = 0
                nd = d[f]
                if unit < 0:
                    nd = nd / abs(unit)
                    row.append(str(nd))
                elif unit > 0:
                    nd = nd * unit
                    row.append(str(nd))
                else:
                    row.append(str(nd))
            else:
                row = d

        rows.append(row)
    return rows


def parseFactorDataNewApi(dataDF):
    # 解析指标数据
    # f13 地区 areaDict = {0: 'SZ', 1: 'SH', 116: 'HK',
    # USA: 105: NASDAQ 纳斯达克  106:NYSE 纽约证券交易所  107:AMEX 美国证券交易所()
    # f2:最新价    f3:涨跌幅    f4:涨跌额
    # 港股股价要/1000 ,A股都/1000
    fieldsUnits = {0: {  "f3": -100, },
                   1: {  "f3": -100, },
                   128: {  "f3": -100, },
                   #128: { "f2": -10, "f3": -10000,"f18": -10 },
                   105: {  "f3": -100, },
                   153: {  "f3": -100, },
                   106: {  "f3": -100, },
                   107: {  "f3": -100, },
                   100: {  "f3": -100, },
                   }
    rows = []
    data=[dict(x) for i,x in dataDF.iterrows()]
    fetchFields = dataDF.columns.values.tolist()
    if not 'f13' in fetchFields:
        raise Exception("获取指标必须包含f13")

    for d in data:
        row = []
        if str(d['f2'])=='-':
            continue

        # rows.append(d)
        # continue
        for f in fetchFields:
            area = d['f13']
            if area in fieldsUnits:
                formatUnitis = fieldsUnits[area]
                if f in formatUnitis:
                    unit = formatUnitis[f]
                else:
                    unit = 0
                nd = d[f]
                if unit < 0:
                    nd = nd / abs(unit)
                    row.append(str(nd))
                elif unit > 0:
                    nd = nd * unit
                    row.append(str(nd))
                else:
                    row.append(str(nd))
            else:
                row = d

        rows.append(row)
    return rows

def findRsData(data):
    # 查找需要解析的参数
    rsData = []
    if len(data) > 0:
        rs = json.loads(data)
        # print(type(rs), rs)
        if "data" in rs:
            rsDict = rs['data']
            # total = rsDict['total']
            if "diff" in rsDict:
                rsData = rsDict['diff']
    return rsData


def getFactorRealData(fields, stockList, pageSize=500):
    # f12:股票代码,f13:市场,f14:股票名称
    # fields = "f12,f13,f19,f14,f139,f2,f4,f1,f3,f152,f5,f30,f31,f18,f32,f6,f8,f7,f10,f22,f9,f112,f100"
    # secids = "0.002274,1.600864,1.600084"
    # fields = "f13,f12,f14,f2,f3,f4"  #指标 f13 地区 areaDict = {0: 'SZ', 1: 'SH', 116: 'HK', 105: 'USA'}
    # stockList = getStockList()
    # pageSize = 300
    pageCodeList = pageStockList(stockList, pageSize)
    # pageCodeList=[['0.002274','1.600864','116.00700','105.JD']]#获取股票 test
    t = toolutils.curtimestamp()  # 时间戳
    allData = []
    for idx, pageList in enumerate(pageCodeList):
        # secids = stockList[0:300]
        # secids=stockList
        secids = ",".join(pageList)
        url = "http://push2.eastmoney.com/api/qt/ulist.np/get?" \
              "secids={}&fields={}&" \
              "ut=6d2ffaa6a585d612eda28417681d58fb&_={}".format(secids, fields, t)

        bt = datetime.datetime.now()
        rspData = toRequest(url)
        et = datetime.datetime.now()
        # print("耗时：", (et - bt).microseconds / 1000)
        findData = findRsData(rspData)
        factorData = parseFactorData(findData, fields)
        # print(idx + 1, len(factorData), datetime.datetime.now())
        for pageData in factorData:
            allData.append(pageData)

    return allData

def getArea():
    areaDict = {'0': 'SZ', '1': 'SH', '116': 'HK', '105': 'USA', '153': 'USA'}
    return areaDict

def stockStatus(row):
    status = str(row.STATUS)
    if status == '停牌':
        return row.F18
    return row.F2

def soutLog(msg):
    # 记录日志
    try:
        filePath = sys.path[0]
        fileCtx = os.path.join(filePath, "logs")
        if not os.path.exists(fileCtx):
            os.makedirs(fileCtx)
        exeDate = datetime.datetime.now().strftime("%Y%m%d")
        fileName = os.path.join(fileCtx, "east_runlog_" + exeDate + ".log")
        with open(fileName, mode='a') as wf:
            wf.write(msg + "\n")
    except:
        print(traceback.format_exc())

def getStopOrTradingStocks(page=1, pageSize=100):
    """
    停牌 停复牌一览
    # https://data.eastmoney.com/tfpxx/
    :return:
    """
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=UTF-8",
        # "Host": "datacenter-web.eastmoney.com",
        # "Referer": "https://data.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
    }

    tp_dict = {}
    q_bdate = datetime.datetime.now().strftime('%Y-%m-%d')
    try:
        t = toolutils.curtimestamp()
        cb = "jQuery112305909854975742028_%s" % t
        """
        callback: jQuery112305909854975742028_1655868919850
        sortColumns: SUSPEND_START_DATE
        sortTypes: -1
        pageSize: 500
        pageNumber: 1
        reportName: RPT_CUSTOM_SUSPEND_DATA_INTERFACE
        columns: ALL
        source: WEB
        client: WEB
        filter: (MARKET="全部")(DATETIME='2022-06-22')
        """
        # print("q_bdate:",q_bdate)
        url=" https://datacenter-web.eastmoney.com/api/data/v1/get?" \
            "callback={cb}" \
            "&sortColumns=SUSPEND_START_DATE&sortTypes=-1" \
            "&pageSize={pageSize}&pageNumber={pageNumber}&reportName=RPT_CUSTOM_SUSPEND_DATA_INTERFACE&columns=ALL&source=WEB&client=WEB" \
            "&filter=(MARKET%3D%22%E5%85%A8%E9%83%A8%22)(DATETIME%3D%27{q_bdate}%27)".format(**{'cb':cb,'pageSize':pageSize,'pageNumber':page,'q_bdate':q_bdate})
        # print("url:",url)
        resp = requests.get(url, headers=headers)
        resp.encoding = 'UTF-8'
        if resp.status_code == 200:
            dataStr = resp.text
            if dataStr != '':
                data = dataStr.replace(cb, "")
                data = data.replace("false", "\"false\"")
                data = data.replace("true", "\"true\"")
                dataStr = data[1:-2]
                # print("dataStr:",dataStr)
                data = json.loads(dataStr)
                # print("pages:", data['pages'])
                result = data['result']
                result = result['data']
                # needColumns=['SECURITY_CODE', 'SECURITY_NAME_ABBR', 'SUSPEND_START_TIME','SUSPEND_END_TIME', 'SUSPEND_EXPIRE', 'SUSPEND_REASON', 'TRADE_MARKET','SUSPEND_START_DATE', 'PREDICT_RESUME_DATE', 'TRADE_MARKET_CODE','SECURITY_TYPE_CODE']
                pdDF = pd.DataFrame(result)
                for r in pdDF.itertuples():
                    SECURITY_CODE=r.SECURITY_CODE
                    # SUSPEND_START_DATE=datetime.datetime.strptime(str(r.SUSPEND_START_DATE),'%Y-%m-%d %H:%M:%S')
                    # if pd.isnull(r.SUSPEND_END_TIME):
                    #     SUSPEND_END_TIME=nowTime
                    # else:
                    #     SUSPEND_END_TIME=datetime.datetime.strptime(str(r.SUSPEND_END_TIME),'%Y-%m-%d %H:%M:%S')
                    # SUSPEND_EXPIRE=r.SUSPEND_EXPIRE
                    # SUSPEND_REASON=r.SUSPEND_REASON
                    #
                    # if nowTime>=SUSPEND_START_TIME and nowTime<=SUSPEND_END_TIME:
                    #     _status='停牌'
                    #     windCode  = toolutils.getstock_windcode(SECURITY_CODE)
                    #     tp_dict[windCode]={'status':_status,'SUSPEND_START_TIME':SUSPEND_START_TIME,'SUSPEND_END_TIME':SUSPEND_END_TIME,'SUSPEND_EXPIRE':SUSPEND_EXPIRE,'SUSPEND_REASON':SUSPEND_REASON}
                    # elif nowTime>=SUSPEND_START_TIME:
                    _status='停牌'
                    windCode  = toolutils.getstock_windcode(SECURITY_CODE)
                    tp_dict[windCode]={'status':_status,'SUSPEND_START_TIME':r.SUSPEND_START_TIME,'SUSPEND_END_TIME':r.SUSPEND_END_TIME,'SUSPEND_EXPIRE':r.SUSPEND_EXPIRE,'SUSPEND_REASON':r.SUSPEND_REASON}

                # for k,v in tp_dict.items():
                #     print(k,v)
                # print("tp_dict:",len(tp_dict))
                return tp_dict
        else:
            sendmsg.send_wx_msg_operation('[%s]停复牌数据查询失败'%(q_bdate))
            return tp_dict
    except:
        soutLog('[%s]停复牌数据查询失败'%(q_bdate)+ ":" + str(datetime.datetime.now()))

        return tp_dict
    finally:
        pass
    return tp_dict

def getStockRealMarket(workTime, area='CN',findStockList=[],suspensionDict={},dataType=''):
    #suspensionDict: 当日停牌A股
    #dataType: 期货类型 fut
    # f2:最新价    f3:涨跌幅    f4:涨跌额  f12：股票代码  f14：股票名称 f18：昨收盘价，停牌股票
    fields = "f13,f12,f14,f2,f3,f4,f18"  # 指标 f13 地区 areaDict = {0: 'SZ', 1: 'SH', 116: 'HK', 105: 'USA 再细分地区'}
    fetchCount = 0
    stockList = []
    dataDF=pd.DataFrame()
    try:
        workTime = workTime[0:4] + '00'  # 取整数
        # stockList = stockList[0:310]
        # print(stockList)
        # exeDate = datetime.datetime.now().strftime("%Y%m%d")
        # workDay = toolutils.getWorkDate()
        # if exeDate != workDay:
        #     return
        bgTime = datetime.datetime.now()
        stockList,rtEastStock_dt = getStockList(area=area,findStockList=findStockList,dataType=dataType)
        # print("stockList:",len(stockList))
        allData = getFactorRealData(fields, stockList)
        dataDF = pd.DataFrame(allData, columns=fields.split(","))
        # dataDF['f3'] = dataDF['f3'].astype(float) / 100
        dataDF['WORKTIME'] = workTime
        # dataDF['STOCKCODE'] = dataDF[['f13', 'f12']].apply(lambda x: stockFix(x, area,rtEastStock_dt), axis=1)
        dataDF['STOCKCODE'] = (dataDF["f13"].astype(str)+"."+dataDF['f12'].astype(str)).apply(lambda x: rtEastStock_dt[x] if x in rtEastStock_dt else x)
        # print(dataDF['STOCKCODE'].values.tolist()[0:10])
        dataDF['AREA'] = area
        dataDF['TRADEDATE'] = datetime.datetime.now().strftime("%Y%m%d")
        # con, cur = dbutils.getConnect()
        dataDF = dataDF[['STOCKCODE', 'f2', 'f3', 'f18', 'TRADEDATE', 'WORKTIME', 'AREA']]
        dataDF = dataDF.rename(columns={'f2': 'F2', 'f3': 'F3', 'f18': 'F18'})
        if area in ['CN']:
            dataDF['STATUS'] = dataDF['STOCKCODE'].apply(lambda x: '停牌' if x in suspensionDict else '交易')
        else:
            dataDF['STATUS'] = '交易'
        # 停牌状态股票 当前股价为昨收盘价
        dataDF['F2'] = dataDF[['F2', 'F18', 'STATUS']].apply(lambda x: stockStatus(x), axis=1)
        dataDF['F2']=dataDF['F2'].astype(float).round(2)
        # dataDF['STOCKTYPE'] = 'cs'
        #单独获取ETF行情
        if area in ['CN']:
            # ETFDF = get_realtime_quotes('ETF', **{'quote': 'b'})
            # ETFDF['AREA']=area
            # ETFDF['STATUS']='交易'
            # ETFDF['TRADEDATE']=datetime.datetime.now().strftime("%Y%m%d")
            # ETFDF['WORKTIME']=workTime
            # ETFDF = ETFDF.rename(columns={'股票代码': 'F12', '市场编号': 'F13', '最新价': 'F2', '涨跌幅': 'F3', '昨日收盘': 'F18'})
            # def _fun_s(row):
            #     if str(row.F2)=='-':
            #         return row.F18
            #     return row.F2
            # ETFDF['F2'] = ETFDF[['F2', 'F18']].apply(lambda x: _fun_s(x), axis=1)
            # ETFDF['F3']=ETFDF['F3'].astype(str).replace({'-':0})
            # ETFDF['F3']=ETFDF['F3'].astype(float)/100
            # #最新行情为空，用上次的价格替换，
            # ETFDF['STOCKCODE'] = ETFDF[['F13', 'F12']].apply(lambda x: etfFix(x), axis=1)
            # ETFDF = ETFDF[['STOCKCODE', 'F2', 'F3','F18', 'TRADEDATE', 'WORKTIME', 'AREA','STATUS']]
            # ETFDF['F2'] = ETFDF['F2'].astype(float).round(3)
            # ETFDF['F18'] = ETFDF['F18'].astype(float).round(3)
            # # ETFDF['STOCKTYPE']='etf'
            # dataDF=dataDF.append(ETFDF,ignore_index=True)
            pass
        edTime = datetime.datetime.now()
        print("[%s]总耗时：%s [秒][%s]" % (area, (edTime - bgTime).seconds,bgTime))
        # print("dataDF:",dataDF)
        # stockcode, tradedate, tradetime, nowprice, pctchange
        # fetchCount = len(dataDF.values.tolist())
        # columns =(dataDF.columns.tolist())
        # print(columns)
        # insertDB(con, cur, dataDF.values.tolist(),area=area)
        return dataDF
    except:
        errorMsg = traceback.format_exc()
        print("errorMsg:",errorMsg)
        soutLog(errorMsg)
        sendmsg.send_wx_msg_operation(errorMsg)
    return dataDF
def getStockRealMarketNewApi(workTime, area='CN',suspensionDict={}):
    fields = {'f12': '股票代码', 'f3': '涨跌幅', 'f2': '最新价', 'f4': '涨跌额',
              'f18': '昨日收盘', 'f13': '市场编号', 'f124': '更新时间戳', 'f297': '最新交易日'}
    _fields={v:k for k,v in fields.items()}
    fetchCount = 0
    stockList = []
    try:
        bgTime = datetime.datetime.now()
        workTime = workTime[0:4] + '00'  # 取整数

        if area in ['CN']:
            dataDF = get_realtime_quotes('沪深A股', **{'quote': 'b'})
        elif area in ['HK']:
            dataDF = get_realtime_quotes('港股', **{'quote': 'b'})
        elif area in ['USA']:
            dataDF = get_realtime_quotes('美股', **{'quote': 'b'})
        else:
            raise Exception('请填写area参数[CN,HK,USA]')
            # dataDF.to_excel("/Users/xiaxuhong/Downloads/dataDFxx.xlsx")
            # print("dataDF 1:",dataDF.columns)
        # print("dataDF:",dataDF.iloc[0])
        dataDF=dataDF.rename(columns=_fields)
        allData=parseFactorDataNewApi(dataDF)
        dataDF = pd.DataFrame(allData, columns=dataDF.columns.values.tolist())
        # print("dataDF 2:",dataDF.columns)
        dataDF=dataDF[['f2', 'f3','f4', 'f18','f12','f13']]
        # dataDF = pd.DataFrame(allData, columns=fields.split(","))
        # dataDF['f3'] = dataDF['f3'].astype(float) / 100
        dataDF['WORKTIME'] = workTime
        dataDF['STOCKCODE'] = dataDF[['f13', 'f12']].apply(lambda x: stockFixApi(x,area), axis=1)
        dataDF['AREA'] = area
        dataDF['TRADEDATE'] = datetime.datetime.now().strftime("%Y%m%d")
        # con, cur = dbutils.getConnect()
        dataDF = dataDF[['STOCKCODE', 'f2', 'f3','f18', 'TRADEDATE', 'WORKTIME', 'AREA']]
        dataDF = dataDF.rename(columns={'f2': 'F2', 'f3': 'F3', 'f18': 'F18'})
        if area in ['CN']:
            dataDF['STATUS'] = dataDF['STOCKCODE'].apply(lambda x: '停牌' if x in suspensionDict else '交易')
        else:
            dataDF['STATUS'] = '交易'
        #停牌状态股票 当前股价为昨收盘价
        dataDF['F2'] = dataDF[['F2','F18','STATUS']].apply(lambda x: stockStatus(x), axis=1)
        edTime = datetime.datetime.now()
        print("[%s]总耗时：%s [秒]" % (area, (edTime - bgTime).seconds))

        return dataDF
    except:
        errorMsg = traceback.format_exc()
        print("errorMsg:",errorMsg)
        soutLog(errorMsg)
        sendmsg.send_wx_msg_operation(errorMsg)
    finally:
        try:
            # cur.close()
            # con.close()
            # soutLog(area + ":" + str(len(stockList)) + ":" + str(fetchCount) + ":" + str(datetime.datetime.now()))
            pass
        except:
            pass
            # rcount=0
            # getCode=[]
            # for rc in allData:
            #     rcount=rcount+len(rc)
            #     for r in rc:
            #         getCode.append(r[1])
            # print(len(stockList),rcount)
            # cstockList=[x[str(x).find(".")+1:] for x in stockList]
            # print("误差",len(list(set(cstockList)-set(getCode))))

def getIndexChartData(area="CN",indexCodeStr = "0000011,3990012,3990062,0003001,3999052"):
    fetchCount = 0
    stockList = []
    columns = ["STOCKCODE", "TRADEDATE", "TRADETIME", "NOWPRICE", "PCTCHANGE",
               "HIGH", "LOW", "OPEN", "PRECLOSE", "ROUNDLOT", "CHANGE", "VOLUME", "AMOUNT", "AREA"]
    rsDF = pd.DataFrame([], columns=columns)
    try:
        # 000001：上证指数，399001：深圳成指,399006：创业版指,399005：中小板指,000300：沪深300，399101：中小板综，399102：创业板综 399905：中证500
        # indexCodeStr = "0000011,3990012,3990062,3990052,0003001,3991012,3991022,3999052"
        # indexCodeStr = "0000011,3990012,3990062,0003001,3999052"
        stockList = indexCodeStr.split(",")
        # http://pdfm.eastmoney.com/EM_UBG_PDTI_Fast/api/js?rtntype=5&cb=jQuery17206007141995963381_1556245673436&id=0000011&type=r&iscr=false&_=1556246093567
        dtList = []

        #pos 取多少个分时数据
        #http://push2.eastmoney.com/api/qt/stock/details/get?secid=1.000001&ut=bd1d9ddb04089700cf9c27f6f7426281&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&pos=-23&invt=2&cb=jQuery112407697237110487369_1623029926524&_=1623029926579
        for indexCode in stockList:
            t = toolutils.curtimestamp()
            url = "http://pdfm.eastmoney.com/EM_UBG_PDTI_Fast/api/js?rtntype=5&id={}&type=r&_={}".format(indexCode, t)
            # print("url:",url)
            data = toRequest(url)
            data = data.replace("false", "\"false\"")
            data = data.replace("true", "\"true\"")
            dataDict = eval(data)
            # print("dataDict:",dataDict)
            info = dataDict['info']
            cp = info['c']  # 当前价
            h = info['h']  # 最高价
            l = info['l']  # 最低价
            o = info['o']  # 开盘价
            yc = info['yc']  # 昨收盘价
            data_list_time = dataDict['data']
            code = dataDict['code']
            name = dataDict['name']
            # print("c:",cp,"h:",h,"l:",l,"o:",o)
            # data = data.pop()

            # "2019-04-26 10:58,3110.18：价格,67547600：成交量  ,3108.683,0
            for data in data_list_time:
                dl = str(data).split(",")
                tradeTimes = dl[0]
                tradeTimes = str(tradeTimes).split(" ")
                ddate = tradeTimes[0]
                dtime = tradeTimes[1]
                cp = dl[1]  # 当前价
                vl = dl[2]
                # vl= dl[3]
                dif = float(cp) - float(yc)
                chg = dif / float(yc)
                # print(code,"chg:",chg,cv,dtime)
                fixCode = indexCode[-1:]
                windCode = code
                if str(fixCode) == "1":
                    windCode = code + ".SH"
                if str(fixCode) == "2":
                    windCode = code + ".SZ"
                ddate = str(ddate).replace("-", '')
                dtime = str(dtime).replace(":", '') + "00"
                dt = [windCode, ddate, dtime, cp, chg, h, l, o, yc, vl, dif, vl, vl, area]
                # f2:最新价    f3:涨跌幅    f4:涨跌额  f12：股票代码  f14：股票名称
                # f15,f16,f17,f18 最高价，最低价，开盘价,昨收,  f6:成交额 f30:现手
                # 现手 ROUNDLOT
                # 成交量 VOLUME
                # 成交额 AMOUNT
                dtList.append(dt)

            # time.sleep(1)
                # dataDF = dataDF[['stockCode', 'workTime', 'f2', 'f3', 'f15', 'f16', 'f17', 'f18', 'f30', 'f4', 'f6', 'f6']]
        # stockcode, tradedate, tradetime, nowprice, pctchange, high, low, open, preclose, roundlot, change, volume, amount, area
        columns = ["STOCKCODE", "TRADEDATE", "TRADETIME", "NOWPRICE", "PCTCHANGE",
                   "HIGH", "LOW", "OPEN", "PRECLOSE", "ROUNDLOT", "CHANGE", "VOLUME", "AMOUNT", "AREA"]
        rsDF = pd.DataFrame(dtList, columns=columns)
        workDay = datetime.datetime.now().strftime("%Y%m%d")
        rsDF = rsDF[rsDF['TRADEDATE'] == workDay]

        fetchCount = len(rsDF)
        # print("dtList：", fetchCount)
        # con,cur =dbutils.getConnect()
        # insertDB2(con, cur, dtList)
        return rsDF
    except:
        errorMsg = traceback.format_exc()
        print(errorMsg)
        # sendmsg.send_wx_msg(errorMsg)
        soutLog("[%s]errorMsg:[%s]"%(indexCode,errorMsg))
    finally:
        try:
            # cur.close()
            # con.close()
            soutLog("indexCode:" + str(len(stockList)) + ":" + str(fetchCount) + ":" + str(datetime.datetime.now()))
        except:
            pass
            # rcount=0
            # getCode=[]
            # for rc in allData:
            #     rcount=rcount+len(rc)
            #     for r in rc:
            #         getCode.append(r[1])
            # print(len(stockList),rcount)
            # cstockList=[x[str(x).find(".")+1:] for x in stockList]
            # print("误差",len(list(set(cstockList)-set(getCode))))
    return rsDF
def getIndexChartData2(area="CN",indexCodeStr='1.000001'):
    fetchCount = 0
    stockList = []
    try:
        #indexCodeStr='1.000001,0.399001,0.399006,1.000300,0.399905'
        # 000001：上证指数，399001：深圳成指,399006：创业版指,399005：中小板指,000300：沪深300，399101：中小板综，399102：创业板综 399905：中证500
        # indexCodeStr = "0000011,3990012,3990062,3990052,0003001,3991012,3991022,3999052"
        stockList = indexCodeStr.split(",")
        # stockList=['1.000001']
        # http://pdfm.eastmoney.com/EM_UBG_PDTI_Fast/api/js?rtntype=5&cb=jQuery17206007141995963381_1556245673436&id=0000011&type=r&iscr=false&_=1556246093567
        dtList = []

        #pos 取多少个分时数据
        #http://push2.eastmoney.com/api/qt/stock/details/get?secid=1.000001&ut=bd1d9ddb04089700cf9c27f6f7426281&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&pos=-23&invt=2&cb=jQuery112407697237110487369_1623029926524&_=1623029926579
        q_url="http://push2his.eastmoney.com/api/qt/stock/trends2/get?cb={cb}" \
              "&secid={secid}" \
              "&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6%2Cf7%2Cf8%2Cf9%2Cf10%2Cf11&fields2=f51%2Cf53%2Cf56%2Cf58&iscr=0&ndays=1" \
              "&_={_}"
        for indexCode in stockList:
            t = toolutils.curtimestamp()
            cb="jQuery112407697237110487369_%s"%t
            url = q_url.format(**{"secid":indexCode,"cb":cb,"_":t})
            # print("url:",url)
            data = toRequest(url)
            data = data.replace(cb, "")
            data = data.replace("false", "\"false\"")
            data = data.replace("true", "\"true\"")
            data=data[1:-2]
            # print("data:",data)
            dataDict = eval(data)
            # print("dataDict:",dataDict)
            info = dataDict['data']
            code = info['code']
            market = info['market']
            name = info['name']
            yc = info['preClose']  # 昨收盘价
            trends = info['trends']
            dt=[]
            mk0=trends[0]
            openPrice=str(mk0).split(",")[1]
            for data in trends:
                dl = str(data).split(",")
                tradeTimes = dl[0]
                tradeTimes = str(tradeTimes).split(" ")
                ddate = tradeTimes[0]
                dtime = tradeTimes[1]
                cp = dl[1]  # 当前价
                vl = dl[2]
                vp= dl[3]#均价
                dif = float(cp) - float(yc)
                chg = dif / float(yc)
                # print(code,"chg:",chg,cv,dtime)
                windCode = code
                if str(market) == "1":
                    windCode = code + ".SH"
                if str(market) == "0":
                    windCode = code + ".SZ"
                ddate = str(ddate).replace("-", '')
                dtime = str(dtime).replace(":", '') + "00"
                dt = [windCode, ddate, dtime, cp, chg, vp, vp, vp, yc, vl, dif, vl, vl, area]
                dtList.append(dt)
        columns = ["STOCKCODE", "TRADEDATE", "TRADETIME", "NOWPRICE", "PCTCHANGE",
                   "HIGH", "LOW", "OPEN", "PRECLOSE", "ROUNDLOT", "CHANGE", "VOLUME", "AMOUNT", "AREA"]
        rsDF = pd.DataFrame(dtList, columns=columns)
        workDay = datetime.datetime.now().strftime("%Y%m%d")
        rsDF = rsDF[rsDF['TRADEDATE'] == workDay]

        fetchCount = len(rsDF)
        # print("dtList：", fetchCount)
        # con,cur =dbutils.getConnect()
        # insertDB2(con, cur, dtList)
        return rsDF
    except:
        errorMsg = traceback.format_exc()
        print(errorMsg)
        # sendmsg.send_wx_msg(errorMsg)
    finally:
        try:
            # cur.close()
            # con.close()
            soutLog("indexCode:", str(len(stockList)) + ":" + str(fetchCount) + ":" + str(datetime.datetime.now()))
        except:
            pass
            # rcount=0
            # getCode=[]
            # for rc in allData:
            #     rcount=rcount+len(rc)
            #     for r in rc:
            #         getCode.append(r[1])
            # print(len(stockList),rcount)
            # cstockList=[x[str(x).find(".")+1:] for x in stockList]
            # print("误差",len(list(set(cstockList)-set(getCode))))

    """
    #http://quote.eastmoney.com/zs000001.html##fullScreenChart
    #http://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&ut=bd1d9ddb04089700cf9c27f6f7426281&fields=f118,f107,f57,f58,f59,f152,f43,f169,f170,f46,f60,f44,f45,f168,f50,f47,f48,f49,f46,f169,f161,f117,f85,f47,f48,f163,f171,f113,f114,f115,f86,f117,f85,f119,f120,f121,f122,f292&invt=2&cb=jQuery112407678758039059816_1623031902187&_=1623031902312
    #http://push2.eastmoney.com/api/qt/stock/get?secid=100.HSI&ut=bd1d9ddb04089700cf9c27f6f7426281&fields=f118,f107,f57,f58,f59,f152,f43,f169,f170,f46,f60,f44,f45,f168,f50,f47,f48,f49,f46,f169,f161,f117,f85,f47,f48,f163,f171,f113,f114,f115,f86,f117,f85,f119,f120,f121,f122,f292&invt=2&cb=jQuery112407678758039059816_1623031902187&_=1623031902312
    上证指数 000001 3593.75 +1.91 +0.05%
今开：3597.14
昨收：3591.84
最高：3600.38
最低：3587.69
涨跌幅：+0.05%
涨跌额：+1.91
换手：0.32%
振幅：0.35%
成交量：1.23亿
成交额：1800亿
内盘：6457万
外盘：5832万


{
	"rc": 0,
	"rt": 4,
	"svr": 182994642,
	"lt": 1,
	"full": 1,
	"data": {
		"f43": 359375,
		"f44": 360038,#最高
		"f45": 358769,#最低
		"f46": 359714,
		"f46": 359714,#今开
		"f47": 123101417,#成交量
		"f47": 123101417,
		"f48": 179998121984.0,#成交额
		"f48": 179998121984.0,
		"f49": 58315259,
		"f50": 187,
		"f57": "000001",
		"f58": "上证指数",
		"f59": 2,
		"f60": 359184,#昨收
		"f85": 3850529577705.0,
		"f85": 3850529577705.0,
		"f86": 1623032132,
		"f107": 1,
		"f113": 1024,
		"f114": 827,
		"f115": 93,
		"f117": 40739504433087.0,
		"f117": 40739504433087.0,
		"f118": 2,
		"f119": -60,
		"f120": 484,
		"f121": 698,
		"f122": 347,
		"f152": 2,
		"f161": 64786158,
		"f163": "-",
		"f168": 32,#换手
		"f169": 191,#涨跌额
		"f169": 191,
		"f170": 5,#涨跌幅
		"f171": 35,#振幅
		"f292": 2
	}
}

    """


all_indexs = [
            {'scode': '000016', 's_info_windcode': '000016.SH', 's_info_name': '上证50', 'market': '012001','sec_id': 'S10442',
             'orgid': 'jysh0000002'},
            {'scode': '399102', 's_info_windcode': '399102.SZ', 's_info_name': '创业板综', 'market': '012002','sec_id': 'S3816168',
             'orgid': '9900001261'},
            {'scode': '399005', 's_info_windcode': '399005.SZ', 's_info_name': '中小企业100', 'market': '012002','sec_id': 'S24126',
             'orgid': '9900001261'},
            {'scode': '000010', 's_info_windcode': '000010.SZ', 's_info_name': '上证180', 'market': '012001','sec_id': '1B0007',
             'orgid': 'jysh0000002'},
            {'scode': '000300', 's_info_windcode': '000300.SH', 's_info_name': '沪深300', 'market': '012001','sec_id': 'S12425',
             'orgid': '9900003101'},
            {'scode': '399101', 's_info_windcode': '399101.SZ', 's_info_name': '中小综指', 'market': '012002','sec_id': 'S13237',
             'orgid': '9900001261'},
            {'scode': '399006', 's_info_windcode': '399006.SZ', 's_info_name': '创业板指',  'market': '012002','sec_id': 'S3805767',
             'orgid': '9900001261'},
            {'scode': '000001', 's_info_windcode': '000001.SH', 's_info_name': '上证指数', 'market': '012001','sec_id': '1A0001',
             'orgid': 'jysh0000002'},
            {'scode': '399905', 's_info_windcode': '399905.SZ', 's_info_name': '中证500', 'market': '012002','sec_id': 'S24126',
             'orgid': '9900003101'},
            {'scode': '399001', 's_info_windcode': '399001.SZ', 's_info_name': '深证成指', 'market': '012002','sec_id': '2A01',
             'orgid': 'jysz0000001'},
        ]

def getIndexMarketData(area="CN"):
    """
    替代wind指数行情 newdata.AIndexEODPrices 表
    :param area:
    :return:
    """
    fetchCount = 0
    stockList = []
    try:
        t = toolutils.curtimestamp()
        # 000001：上证指数，399001：深圳成指,399006：创业版指,399005：中小板指,000300：沪深300，399101：中小板综，399102：创业板综 399905：中证500
        #'000010.SZ 上证180
        indexCodeStr = "1.000001,0.399001,0.399006,0.399005,1.000300,0.399101,0.399102,0.399905,1.000010"
        stockList = indexCodeStr.split(",")
        # stockList=['1.000001']
        # http://pdfm.eastmoney.com/EM_UBG_PDTI_Fast/api/js?rtntype=5&cb=jQuery17206007141995963381_1556245673436&id=0000011&type=r&iscr=false&_=1556246093567
        dtList = []
        #pos 取多少个分时数据
        #http://push2.eastmoney.com/api/qt/stock/details/get?secid=1.000001&ut=bd1d9ddb04089700cf9c27f6f7426281&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&pos=-23&invt=2&cb=jQuery112407697237110487369_1623029926524&_=1623029926579
        q_url="http://push2his.eastmoney.com/api/qt/stock/trends2/get?cb=jQuery112407697237110487369_1623029926516" \
              "&secid={secid}" \
              "&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6%2Cf7%2Cf8%2Cf9%2Cf10%2Cf11&fields2=f51%2Cf53%2Cf56%2Cf58&iscr=0&ndays=1" \
              "&_={_}"
        cb="jQuery112407678758039059816_%s"%t
        q_url="http://push2.eastmoney.com/api/qt/stock/get?" \
              "secid={secid}" \
              "&ut=bd1d9ddb04089700cf9c27f6f7426281" \
              "&fields=f118,f107,f57,f58,f59,f152,f43,f169,f170,f46,f60,f44,f45,f168,f50,f47,f48,f49,f46,f169,f161,f117,f85,f47,f48,f163,f171,f113,f114,f115,f86,f117,f85,f119,f120,f121,f122,f292" \
              "&invt=2" \
              "&cb={cb}" \
              "&_={_}"

        rows = []
        for indexCode in stockList:
            url = q_url.format(**{"secid":indexCode,"cb":cb,"_":t})
            # print("url:",url)
            data = toRequest(url)
            data = data.replace(cb, "")
            data = data.replace("false", "\"false\"")
            data = data.replace("true", "\"true\"")
            data=data[1:-2]
            # print("data:",data)
            dataDict = eval(data)
            # print("dataDict:",dataDict)
            data=dataDict['data']
            # for k,v in data.items():
            #     print(k,v)
            # print("data:",data)
            _decimal=float(data['f59'])#小数位
            _divisor=10**_decimal

            s_dq_close=float(data['f43'])/_divisor#收盘价
            s_dq_open=float(data['f46'])/_divisor#收盘价
            s_dq_high=float(data['f44'])/_divisor#最高价
            s_dq_low=float(data['f45'])/_divisor#最低指数
            s_dq_preclose=float(data['f60'])/_divisor#昨日收市指数
            indexcode=data['f57']#指数代码
            s_dq_volume=float(data['f47'])#成交数量w
            s_dq_amount=float(data['f48'])/_divisor/10#成交金额
            s_dq_change=float(data['f169'])/_divisor#涨跌额
            s_dq_pctchange=float(data['f170'])/_divisor#涨跌幅
            # print("s_dq_close:",s_dq_close)
            crncy_code = 'CNY'
            #511917230.7
            #511917232
            s_info_windcode = ''
            for x in all_indexs:
                scode = x['scode']
                if indexcode == scode:
                    s_info_windcode = x['s_info_windcode']
                    sec_id = x['sec_id']
                    break
            trade_dt=datetime.datetime.now().strftime("%Y%m%d")
            # trade_dt="20210714"  #
            opmode = '0'
            opdate = datetime.datetime.now()
            tradedate = trade_dt
            d_ids = [indexcode, trade_dt, str(s_dq_close)]
            object_id = toolutils.md5("".join(d_ids))

            rows.append(
                [object_id, s_info_windcode, trade_dt, crncy_code, s_dq_preclose, s_dq_open, s_dq_high, s_dq_low,
                 s_dq_close,
                 s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, sec_id, opdate, opmode, indexcode, tradedate
                 ])
            print("rows：",len(rows[-1]),rows[-1])
        i_sql = "INSERT INTO newdata.aindexeodprices (object_id, s_info_windcode, trade_dt, crncy_code, s_dq_preclose, s_dq_open, s_dq_high, s_dq_low, s_dq_close," \
                " s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, sec_id, opdate, opmode, indexcode, tradedate) " \
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        con,cur = dbutils.getConnect()
        cur.executemany(i_sql,rows)
        con.commit()
        cur.close()
        con.close()

        return
    except:
        errorMsg = traceback.format_exc()
        print(errorMsg)
        sendmsg.send_wx_msg("替代wind指数行情"+errorMsg)
    finally:
        try:
            # cur.close()
            # con.close()
            soutLog("indexCode:", str(len(stockList)) + ":" + str(fetchCount) + ":" + str(datetime.datetime.now()))
        except:
            pass
            # rcount=0
            # getCode=[]
            # for rc in allData:
            #     rcount=rcount+len(rc)
            #     for r in rc:
            #         getCode.append(r[1])
            # print(len(stockList),rcount)
            # cstockList=[x[str(x).find(".")+1:] for x in stockList]
            # print("误差",len(list(set(cstockList)-set(getCode))))

    """
    #http://quote.eastmoney.com/zs000001.html##fullScreenChart
    上证指数 000001 3593.75 +1.91 +0.05%
今开：3597.14
昨收：3591.84
最高：3600.38
最低：3587.69
涨跌幅：+0.05%
涨跌额：+1.91
换手：0.32%
振幅：0.35%
成交量：1.23亿
成交额：1800亿
内盘：6457万
外盘：5832万


{
	"rc": 0,
	"rt": 4,
	"svr": 182994642,
	"lt": 1,
	"full": 1,
	"data": {
		"f43": 359375,
		"f44": 360038,#最高
		"f45": 358769,#最低
		"f46": 359714,
		"f46": 359714,#今开
		"f47": 123101417,#成交量
		"f47": 123101417,
		"f48": 179998121984.0,#成交额
		"f48": 179998121984.0,
		"f49": 58315259,
		"f50": 187,
		"f57": "000001",
		"f58": "上证指数",
		"f59": 2,
		"f60": 359184,#昨收
		"f85": 3850529577705.0,
		"f85": 3850529577705.0,
		"f86": 1623032132,
		"f107": 1,
		"f113": 1024,
		"f114": 827,
		"f115": 93,
		"f117": 40739504433087.0,
		"f117": 40739504433087.0,
		"f118": 2,
		"f119": -60,
		"f120": 484,
		"f121": 698,
		"f122": 347,
		"f152": 2,
		"f161": 64786158,
		"f163": "-",
		"f168": 32,#换手
		"f169": 191,#涨跌额
		"f169": 191,
		"f170": 5,#涨跌幅
		"f171": 35,#振幅
		"f292": 2 #交易状态
	}
}
参考 
 /Users/xiaxuhong/work/project/web/main/login/static/js
 http://quote.eastmoney.com/newstatic/build/globalindex2.js

    """

def hkWorkDayChange(workDay,mode=''):
    """
    港股交易日动态变动
    """
    try:
        _count=0
        if mode in ['add']:
            con, cur = dbutils.getConnect()
            d_sql="delete from config.hkworkday where workday=%s"
            i_sql="insert into config.hkworkday(workday) values (%s)"
            cur.execute(d_sql,[workDay])
            cur.execute(i_sql,[workDay])
            _count = cur.rowcount
            con.commit()
            cur.close()
            con.close()
        elif mode in ['remove']:
            con, cur = dbutils.getConnect()
            d_sql="delete from config.hkworkday where workday=%s"
            cur.execute(d_sql,[workDay])
            _count = cur.rowcount
            con.commit()
            cur.close()
            con.close()
        else:
            return
        sendmsg.send_wx_msg_operation('港股交易日变动[%s],[%s]成功,rowcount[%s]'%(workDay,mode,str(_count)))
    except:
        # logging.error('港股交易日动态变动异常')
        # logging.error(traceback.format_exc())
        sendmsg.send_wx_msg_operation('港股交易日动态变动异常[%s],[%s]'%(workDay,mode))
    finally:
        pass


def marketStatus(indexCode="100.HSI",isSendMsg=False):
    try:
        _tradeStatus={}
        _tradeStatus['1']='开盘竞价'
        _tradeStatus['2']='交易中'
        _tradeStatus['3']='盘中休市'
        _tradeStatus['4']='收盘竞价'
        _tradeStatus['5']='已收盘'
        _tradeStatus['6']='停牌'
        _tradeStatus['7']='退市'
        _tradeStatus['8']='暂停上市'
        _tradeStatus['9']='未上市'
        _tradeStatus['10']='未开盘'
        _tradeStatus['11']='盘前'
        _tradeStatus['12']='盘后'
        _tradeStatus['13']='休市'
        _tradeStatus['14']='盘中停牌'
        _tradeStatus['15']='非交易代码'
        _tradeStatus['16']='波动性中断'
        _tradeStatus['17']='盘后交易启动'
        _tradeStatus['18']='盘后集中撮合交易'
        _tradeStatus['19']='盘后固定价格交易'
        t = toolutils.curtimestamp()
        cb = "jQuery112407678758039059816_%s" % t

        _url="http://push2.eastmoney.com/api/qt/stock/get?secid={_indexCode}&ut=bd1d9ddb04089700cf9c27f6f7426281&fields=f118,f107,f57,f58,f59,f152,f43,f169,f170,f46,f60,f44,f45,f168,f50,f47,f48,f49,f46,f169,f161,f117,f85,f47,f48,f163,f171,f113,f114,f115,f86,f117,f85,f119,f120,f121,f122,f292" \
             "&invt=2&cb={_cb}&_={_t}".format(**{'_indexCode':indexCode,"_cb":cb,'_t':t})
        # print("_url:",_url)
        data = toRequest(_url)
        data = data.replace(cb, "")
        data = data.replace("false", "\"false\"")
        data = data.replace("true", "\"true\"")
        data = data[1:-2]
        dataDict = eval(data)
        # print("dataDict:",dataDict)
        data = dataDict['data']
        f58 = data['f58']#指数名称
        f292 = str(data['f292'])#交易状态
        if isSendMsg:
            tradeStatus = _tradeStatus[f292]
            #if f292 in ['3']:#盘中休市，不推送
            #    return
            if indexCode.endswith('.HSI'):
                daoheIsWorkDay = toolutils.isHkWorkDay()
            elif indexCode.endswith('.NDX'):
                daoheIsWorkDay = toolutils.isUSAWorkDay()
            else:
                daoheIsWorkDay = toolutils.isAWorkDay()
            if indexCode.endswith('.HSI') and tradeStatus in ['交易中'] and not daoheIsWorkDay:
                #添加交易日数据 hkworkday
                hkWorkDay = toolutils.getHkWorkDay()
                hkWorkDayChange(hkWorkDay,mode='add')
            # _msg = "%s %s %s [daohe:%s]" % (f58, datetime.datetime.now().strftime("%Y%m%d"), tradeStatus,str(daoheIsWorkDay))
            _msg = "%s %s %s [eastMarket:%s] [daoheDB:%s]" % (f58, datetime.datetime.now().strftime("%Y%m%d"),indexCode, tradeStatus, str(daoheIsWorkDay))
            sendmsg.send_wx_msg_operation(_msg)
            sendmsg.send_wx_msg_operation(_msg, wxId='trade')
        else:
            if f292 not in ['1','2','3']:
                tradeStatus=_tradeStatus[f292]
                if indexCode.endswith('.HSI'):
                    daoheIsWorkDay = toolutils.isHkWorkDay()
                elif indexCode.endswith('.NDX'):
                    daoheIsWorkDay = toolutils.isUSAWorkDay()
                else:
                    daoheIsWorkDay = toolutils.isAWorkDay()
                _msg = "%s %s %s [eastMarket:%s] [daoheDB:%s]" % (
                f58, datetime.datetime.now().strftime("%Y%m%d"),indexCode, tradeStatus, str(daoheIsWorkDay))
                sendmsg.send_wx_msg_operation(_msg)
                sendmsg.send_wx_msg_operation(_msg,wxId='trade')
    except:

        sendmsg.send_wx_msg_operation("获取市场状态异常")
        sendmsg.send_wx_msg_operation(traceback.format_exc())


def futsseapixx(f_code):
        """
        test
        获取昨收盘，如果遇到提前收盘，sql查询会有误差，最好是用 cme_group_mk.py 中获取结算行情。
        """
        try:
            t = toolutils.curtimestamp()
            callbackName="jQuery35104539555147704015_%s"%t
            q_code=str(f_code).replace(".",'_')
            rt=t
            url="http://futsseapi.eastmoney.com/static/{q_code}_qt?callbackName={callbackName}&_={rt}".format(**{'q_code':q_code,'callbackName':callbackName,'rt':rt})
            resp = requests.get(url)
            if resp.status_code==200:
                resp_txt=str(resp.text).replace(callbackName,'')[1:-1]
                rp=json.loads(resp_txt)
                qt=rp['qt']
                # print('rp:',qt)
                zjsj=qt['zjsj']
                kpsj=qt['kpsj']
                spsj=qt['spsj']
                utime=qt['utime']
                dm=qt['dm']
                name=qt['name']
                _kpsj = datetime.datetime.fromtimestamp(kpsj)
                _spsj = datetime.datetime.fromtimestamp(spsj)
                _utime = datetime.datetime.fromtimestamp(utime)
                futureDate=(str(_kpsj)[0:10]).replace('-','')
                print("_kpsj",_kpsj)
                print("_spsj",_spsj)
                print("_utime",_utime)
                # print("preClose",preClose)
                symbol=str(f_code).split(".")[-1]

                selltData = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%m%d%Y")
                _kpsj = (_utime + datetime.timedelta(days=-1)).strftime("%m%d%Y")
                if selltData==_kpsj:
                    stockCodeDict={'NQ00Y':'NQZ2'}
                    stockCode=dm
                    if dm in stockCodeDict:
                        stockCode=stockCodeDict[dm]

                    q_his="select s_dq_close from  spider.wdstockmarket" \
                          " where stockcode = and tradedate=(select max(tradedate) from  spider.wdstockmarket where stockcode = and tradedate<=%(tradeDate)s)"
                    #add pass
                    d_sql = "delete from spider.wdstockmarket where stockcode =%s"
                    # 历史行情
                    i_sql = "INSERT INTO spider.wdstockmarket (stockcode, stockname, s_dq_close, s_dq_preclose, s_dq_change, s_dq_pctchange, tradedate)" \
                            " VALUES(%s,%s,%s,%s,%s,%s,%s)"
                    con, cur = dbutils.getConnect()
                    cur.execute(d_sql ,stockCode)
                    cur.executemany(i_sql, [stockCode,name,zjsj,])
                    con.commit()


        except:
            print(traceback.format_exc())
            sendmsg.send_wx_msg_operation('future Settlement Market Get Error!')
            sendmsg.send_wx_msg_operation(traceback.format_exc())


"""
east 获取个股信息：
https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_USF10_INFO_ORGPROFILE&columns=SECUCODE%2CSECURITY_CODE%2CORG_CODE%2CSECURITY_INNER_CODE%2CORG_NAME%2CORG_EN_ABBR%2CBELONG_INDUSTRY%2CFOUND_DATE%2CCHAIRMAN%2CREG_PLACE%2CADDRESS%2CEMP_NUM%2CORG_TEL%2CORG_FAX%2CORG_EMAIL%2CORG_WEB%2CORG_PROFILE&quoteColumns=&filter=(SECURITY_CODE%3D%22MU%22)&pageNumber=1&pageSize=200&sortTypes=&sortColumns=&source=SECURITIES&client=PC&v=019264191352860038


reportName: RPT_USF10_INFO_ORGPROFILE
columns: SECUCODE,SECURITY_CODE,ORG_CODE,SECURITY_INNER_CODE,ORG_NAME,ORG_EN_ABBR,BELONG_INDUSTRY,FOUND_DATE,CHAIRMAN,REG_PLACE,ADDRESS,EMP_NUM,ORG_TEL,ORG_FAX,ORG_EMAIL,ORG_WEB,ORG_PROFILE
quoteColumns: 
filter: (SECURITY_CODE="MU")
pageNumber: 1
pageSize: 200
sortTypes: 
sortColumns: 
source: SECURITIES
client: PC
v: 019264191352860038

https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_USF10_INFO_ORGPROFILE&columns=SECUCODE%2CSECURITY_CODE%2CORG_CODE%2CSECURITY_INNER_CODE%2CORG_NAME%2CORG_EN_ABBR%2CBELONG_INDUSTRY%2CFOUND_DATE%2CCHAIRMAN%2CREG_PLACE%2CADDRESS%2CEMP_NUM%2CORG_TEL%2CORG_FAX%2CORG_EMAIL%2CORG_WEB%2CORG_PROFILE&quoteColumns=&filter=(SECURITY_CODE%3D%22MU%22)&pageNumber=1&pageSize=200&sortTypes=&sortColumns=&source=SECURITIES&client=PC&v=019264191352860038

#重大事件提醒：
https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_F10_US_DETAIL&params=MU.O&p=1&source=SECURITIES&client=SW&v=04703713781895009

#派息查询：
https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_USF10_INFO_DIVIDEND&columns=SECUCODE%2CSECURITY_CODE%2CSECURITY_NAME_ABBR%2CSECURITY_INNER_CODE%2CNOTICE_DATE%2CASSIGN_TYPE%2CPLAN_EXPLAIN%2CEQUITY_RECORD_DATE%2CBONUS_PAY_DATE%2CEX_DIVIDEND_DATE%2CASSIGN_PERIOD&quoteColumns=&filter=(SECUCODE%3D%22MU.O%22)&pageNumber=1&pageSize=200&sortTypes=-1&sortColumns=EX_DIVIDEND_DATE&source=SECURITIES&client=PC&v=07849953928007192
reportName: RPT_USF10_INFO_DIVIDEND
columns: SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,SECURITY_INNER_CODE,NOTICE_DATE,ASSIGN_TYPE,PLAN_EXPLAIN,EQUITY_RECORD_DATE,BONUS_PAY_DATE,EX_DIVIDEND_DATE,ASSIGN_PERIOD
quoteColumns: 
filter: (SECUCODE="MU.O")
pageNumber: 1
pageSize: 200
sortTypes: -1
sortColumns: EX_DIVIDEND_DATE
source: SECURITIES
client: PC
v: 07849953928007192

"""
if __name__ == '__main__':
    # indexDF = getIndexChartData(area='CN')  # 指数
    # indexDF = getIndexChartData2(area='CN')  # 指数
    # indexDF = getIndexMarketData(area='CN')  # 指数
    con,cur = dbutils.getConnect()
    q_sql=" select windcode ,gicssubindustry ,companyname  from newdata.a_reuters_basic arb  where windcode  in ('AAPL.O','MSFT.O','GOOG.O','GOOGL.O','AMZN.O','NVDA.O','TSLA.O','META.O','TSM.N','DEO.N','V.N','LLY.N','JNJ.N','WMT.N','NVO.N','MA.N','AVGO.O','PG.N','ORCL.N','HD.N','ASML.O','MRK.N','KO.N','PEP.O','ABBV.N','COST.O','AZN.O','BABA.N','PFE.N','ADBE.O','NVS.N','MCD.N','CRM.N','AMD.O','CSCO.O','TMO.N','NFLX.O','DHR.N','DIS.N','NKE.N','SAP.N','TXN.O','RTX.N','PM.N','INTC.O','QCOM.O','UL.N','SONY.N','AMAT.O','AMGN.O','SBUX.O','MDT.N','NOW.N','PDD.O','MDLZ.O','BKNG.O','ADI.O','TJX.N','UBER.N','LRCX.O','SHOP.N','ABNB.O','ZTS.N','MU.O','RACE.N','BTI.N','PYPL.O','GSK.N','PANW.O','SNPS.O','KLAC.O','EL.N','CDNS.O','CL.N','ARM.O','MCO.N','CART.O','MNST.O','NTES.O','TGT.N','JD.O','CMG.N','SNOW.N','MAR.O','FTNT.O','MRVL.O','ANET.N','HSY.N','NXPI.O','GIS.N','BIDU.O','KMB.N','MCHP.O','TEAM.O','LULU.O','TDG.N','ON.O','SQ.N','HLT.N','TTD.O','SE.N','ROST.O','CRWD.O','DELL.N','DG.N','PLTR.N','ILMN.O','LI.O','FIS.N','DDOG.O','BF_A.N','DLTR.O','SPOT.N','DASH.N','MDB.O','HUBS.N','TCOM.O','YUMC.N','ENPH.O','ALGN.O','RBLX.N','ZS.O','BGNE.O','SYM.O','ZTO.N','NET.N','ULTA.O','BEKE.N','HPE.N','FSLR.O','ZM.O','GRMN.N','SWKS.O','TER.O','ENTG.O','PINS.N','EXPE.O','SNAP.N','CHWY.N','HTHT.O','U.N','SMCI.O','NIO.N','TME.N','DECK.N','BILL.N','LSCC.O','OKTA.O','COIN.O','LEGN.O','ETSY.O','CFLT.O','XPEV.N','CDAY.N','ROKU.O','JNPR.N','QRVO.O','VIPS.N','ONON.N','HOOD.O','MNDY.O','NATI.O','GTLB.O','BZ.O','EDU.N','YMM.N','ESTC.N','WOLF.N','BILI.O','DUOL.O','CYBR.O','NOV.N','FUTU.O','COHR.N','ELF.N','MNSO.N','ONTO.N','WK.N','AFRM.O','SMAR.N','NEWR.N','TOELY.OO','FRSH.O','FN.N','S.N','MTSI.O','LYFT.O','LITE.O','MSTR.O','EXTR.O','ESMT.N','NCNO.O','SPT.O','FROG.O','STAA.O','JAMF.O','ATAT.O','PD.N','RIOT.O','IFNNY.OO','LKNCY.OO','CAMT.O','LMND.N','DNOW.N','AAOI.O','MSCI.N','TISI.N')"
    rt=pd.read_sql(q_sql,con)
    rt.to_excel("d:/work/temp/us_gics.xlsx",index=False)
    # indexDF.to_excel('/Users/xiaxuhong/Downloads/dataDF_east.xlsx')

    # print(indexDF.columns)

    # marketStatus(indexCode='100.NDX',isSendMsg=True)
    # marketStatus(indexCode='1.000001',isSendMsg=True)
    # # marketStatus(isSendMsg=True)
    # rs = getStockRealMarketNewApi('172600',area='USA')
    # rs = getStockRealMarket('172600',area='USA')
    # print(len(rs))
    # x=rs[rs['STOCKCODE']=='LKNCY']
    # print(x)

    # stockList = getStockList(area='CN')
    # print(len(stockList),stockList)

    # hkDF = getStockRealMarket('094900', area='HK',dataType='fut')
    # hkDF = getStockRealMarket('094900', area='HK')
    # print("hkDF:",hkDF)
    #
    # stockList = getStockList(area='HK',dataType='fut')
    # futCodes = [str(s).split(".")[1] for s in stockList]
    #
    # print(len(stockList),stockList,futCodes)
    # futhkDF=hkDF[hkDF['STOCKCODE'].isin(futCodes)]
    # print("futhkDF:",futhkDF)
    # stockList = getStockList(area='USA')
    # print(len(stockList),stockList)

    # marketStatus(indexCode="100.HSI", isSendMsg=True)  # 港股市场状态检查
    # marketStatus(indexCode="1.000001", isSendMsg=True)  # A股市场状态检查

    # x=getIndexChartData2(area='CN',indexCodeStr = "1.000001,0.399001,0.399006,1.000300,0.399905")

    # x.to_excel("d:/work/temp/a.xlsx")

    # fields = "f13,f12,f14,f2,f3,f4,f18"  # 指标 f13 地区 areaDict = {0: 'SZ', 1: 'SH', 116: 'HK', 105: 'USA 再细分地区'}
    # secids = ",".join(["103.NQ23M","134.HTIH3"])
    # t=toolutils.curtimestamp()
    # url = "http://push2.eastmoney.com/api/qt/ulist.np/get?" \
    #       "secids={}&fields={}&" \
    #       "ut=6d2ffaa6a585d612eda28417681d58fb&_={}".format(secids, fields, t)
    #
    # bt = datetime.datetime.now()
    # rspData = toRequest(url)
    # print("rspData:",rspData)

    pass