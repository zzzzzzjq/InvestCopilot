###
###获取美股收盘行情
###
import random
import requests
import logging
import traceback
import pandas as pd
import sys, os
import time
import numpy as np
import datetime

sys.path.append("../../../../..")
# sys.setdefaultencoding('utf8')
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'

logger = logging.getLogger('')

from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
import InvestCopilot_App.models.toolsutils.sendmsg as wx_send
from InvestCopilot_App.models.market import marketUtils as market_utils

token = '7bc05d0d4c3c22ef9fca8c2a912d779c'

def formatFloat(data):
    try:
        return float(data)
    except Exception as ex:
        return 0

insert_east_us_daymarket= """
   insert into spider.east_us_daymarket
   (windcode,stockcode,ric, stockname, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate)
 values
   (%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s,%s )
"""

def getusMarktet(con, cur):
    """
    美股实时行情同步
    """
    try:
        #当前工作日
        workDay = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")
        if not market_utils.isUSAWorkDay(workDay):
            return
        del_usstockmarket = "delete from spider.east_us_daymarket t where t.tradeDate=%s"
        usdaymarkDF = get_realtime_quotes('美股')
        #	最新价	涨跌幅	涨跌额	成交量	成交额	换手率	动态市盈率	量比	股票代码	市场编号	股票名称	最高	最低	今开	昨日收盘	总市值	流通市值	更新时间戳	最新交易日
        usdaymarkDF = usdaymarkDF.replace({"-": None,np.NAN:None})
        # of="d:/work/temp/美股3.xlsx"
        usdaymarkDF = usdaymarkDF[usdaymarkDF['总市值'].astype(float) > 0]
        # usdaymarkDF.to_excel("d:/work/temp/us.xlsx")
        # return
        # (stockcode, stockname, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate)

        quscodedf = pd.read_sql("select  *  from spider.usa_stockcode  where  windcode is not null ",con)
        qcodes=quscodedf[["stockcode",'windcode']]
        codes_dt={r.stockcode : r.windcode for r in qcodes.itertuples()}
        usMarketList=[]
        for _idx, usdt in usdaymarkDF.iterrows():
            stockCode = str(usdt.股票代码)
            if stockCode in codes_dt:
                windCode = codes_dt[stockCode]
            else:
                continue
            rid =str(usdt.市场编号)+"."+str(usdt.股票代码)
            stockname = usdt.股票名称
            s_dq_close = usdt.最新价
            if pd.isnull(s_dq_close) or s_dq_close is None:
                continue  # 停牌
            s_dq_change = usdt.涨跌额
            s_dq_pctchange = usdt.涨跌幅
            s_dq_volume = usdt.成交量
            s_dq_amount = usdt.成交额
            s_dq_open = usdt.今开
            s_dq_high = usdt.最高
            s_dq_low = usdt.最低
            s_dq_preclose = usdt.昨日收盘
            tradedate = workDay
            usMarketList.append([windCode,stockCode,rid, stockname, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate])
            # print(usMarketList[-1])
            # cur.execute(insert_east_us_daymarket, usMarketList[-1])
            # print('row:', row)
        cur.execute(del_usstockmarket, [workDay])
        cur.executemany(insert_east_us_daymarket, usMarketList)
        con.commit()
    except Exception as ex:
        print(traceback.format_exc())
        logging.error("通过美股行情数据异常:%s" % ex)
        return "同步通过美股行情数据异常!"

    return "同步美股行情数据成功!"


def synusMarket(stocktype='stocks'):
    try:
        workDay = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")
        if not market_utils.isUSAWorkDay(workDay):
            return
        con, cur = dbutils.getConnect()
        msg="获取美股行情数据成功"
        # 获取美股行情数据
        getusMarktet(con, cur)
        # 检查最新行情数据，发送邮件
        if stocktype in ['stocks']:
            q_usMarket = 'select count(1) allnum,tradedate from spider.east_us_daymarket t where t.tradedate =(select max(tradedate) from spider.east_us_daymarket) group by tradedate'
        elif stocktype in ['idx']:
            q_usMarket = 'select count(1) allnum,tradedate from spider.east_us_idxmarket t where t.tradedate =(select max(tradedate) from spider.east_us_idxmarket) group by tradedate'
        else:
            return
        # mkDF = pd.read_sql(q_usMarket, con)
        cur.execute(q_usMarket)
        rsData = cur.fetchall()
        if len(rsData)==0:#mkDF.empty:
            title = "获取美股行情数据为空，请手动触发！"
        else:
            allNum = rsData[0][0]
            tradeDate = rsData[0][1]
            copynum=0
            if stocktype in ['stocks']:
                #copy
                d_his="delete  from  spider.east_us_stockprice  where tradedate=%s"
                copy="insert into spider.east_us_stockprice  select  windcode,stockcode, ric, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate from spider.east_us_daymarket" \
                     " where tradedate= (select max(tradedate) from spider.east_us_daymarket) and windcode in (select windcode from spider.east_us_stockprice  group by windcode )"
            else:
                d_his = "delete  from  spider.east_us_idxprice  where tradedate=%s"
                copy = "insert into spider.east_us_idxprice  select  windcode,stockcode, ric, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate from spider.east_hk_daymarket" \
                       " where tradedate= (select max(tradedate) from spider.east_us_idxmarket) and windcode in (select windcode from spider.east_us_idxprice  group by windcode )"
            cur.execute(d_his,[tradeDate])
            cur.execute(copy)
            con.commit()
            copynum=cur.rowcount
            title = "生产环境美股[%s]行情抓取：[%s]日获取数据[%d]条,copy[%s]条[%s]" % (stocktype,tradeDate, allNum ,copynum,datetime.datetime.now())
            first_hisdata(stocktype='idx', beg=tradeDate)
        wx_send.send_wx_msg_operation(title)
        print("sync usMarktet" , title)
    except:
        error = traceback.format_exc()
        title="获取美股行情数据失败"
        logger.error(title)
        logger.error(error)
        msg=error
        wx_send.send_wx_msg_operation(title+msg)
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass

insert_eaststockmarket = """
   insert into spider.east_us_stockprice
   (windcode,stockcode, ric, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate)
 values
   (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
"""
insert_eastidxkmarket = """
   insert into spider.east_us_idxprice
   (windcode,stockcode, ric, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate)
 values
   (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
"""

def getStockHisMarket(windCode,stockCode,eastCode='1.510050',beg="",end="",stocktype="stocks"):
    col_dict = {'f51': '交易日期', 'f52': '今开', 'f53': '收盘', 'f54': '最高', 'f55': '最低', "f56": '成交量', "f57": "成交额",
                'f58': '振幅', 'f59': '涨跌幅', 'f60': '涨跌额', "f61": "换手率", "f18": "昨收"}
    nowDate = datetime.datetime.now().strftime("%Y%m%d")
    try:
        if beg=='':
            beg=nowDate
        if end=='':
            end=nowDate
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get" \
              "?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13" \
              "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61" \
              "&beg={}" \
              "&end={}" \
              "&ut=fa5fd1943c7b386f172d6893dbfba10b" \
              "&rtntype=6" \
              "&secid={}" \
              "&klt=101" \
              "&fqt=1" \
              "".format(beg,end,eastCode)  # &cb=jsonp1599125260092  1.510050 上证50etf
        # sz 0. sh 1.
        # fqt=1 前权价格
        # fqt=2 后复权价格
        # fqt=3 不权价格
        headers = {"Accept": "*/*",
                   "Accept-Encoding": "gzip, deflate",
                   "Accept-Language": "zh-CN,zh;q=0.9",
                   "Connection": "keep-alive",
                   "Host": "push2his.eastmoney.com",
                   "Referer": "http://quote.eastmoney.com/basic/h5chart-iframe.html?code=510050&market=1&type=r",
                   "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36",
                   }
        print(url)
        #https://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fields=f57,f58,f106,f105,f62,f106,f108,f59,f43,f46,f60,f44,f45,f47,f48,f49,f113,f114,f115,f117,f85,f152,f164,f292,f301,f51,f52,f116,f84,f92,f55,f126,f109,f167,f123,f124,f125,f530,f119,f120,f121,f122,f135,f136,f137,f138,f139,f140,f141,f142,f143,f144,f145,f146,f147,f148,f149,f177&secid=116.00700&cb=jQuery35106789541401852681_1702195084505&_=1702195084506
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data_json = resp.json()
            data = data_json['data']
            klines = data['klines']
            k_d_list = []
            pre_close=0
            for k in klines:
                ks = str(k).split(",")
                ks.append(pre_close)
                k_d_list.append(ks)
                pre_close=ks[2]
            print("k_d_list:",len(k_d_list))
            if len(k_d_list)==0:
                return
            # print("k_d_list:",k_d_list)
            dataDF = pd.DataFrame(k_d_list,
                                  columns=["f51", "f52", "f53", "f54", "f55", "f56", "f57", "f58", "f59", "f60", "f61", "f18"])
            dataDF = dataDF.rename(columns=col_dict)
            # dataDF.to_excel("/Users/xiaxuhong/Downloads/贵州茅台.xlsx")
            # dataDF.to_excel("/Users/Robby/Downloads/%s_%s.xlsx"%(stockName,end))
            #dataDF.to_excel("D:/work/temp/%s_%s.xlsx"%(stockName,end))

            dataDF['windCode']=windCode
            dataDF['stockCode']=stockCode
            dataDF['ric']=eastCode
            dataDF['交易日期']=dataDF['交易日期'].apply(lambda x:str(x).replace("-",''))
            tradeDate= dataDF['交易日期'].min()
            if stocktype in ['stocks']:
                del_usstockmarket = "delete from spider.east_us_stockprice t where t.tradeDate>=%s and windCode=%s"
            elif stocktype in ['idx']:
                del_usstockmarket = "delete from spider.east_us_idxprice t where t.tradeDate>=%s and windCode=%s"
                insert_eaststockmarket = insert_eastidxkmarket

            con,cur = dbutils.getConnect()
            #(stockcode, stockname, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate)
            # print("dataDF:",del_usstockmarket,[tradeDate,stockCode])
            dataDF=dataDF[['windCode','stockCode','ric','收盘','涨跌额','涨跌幅','成交量','成交额','今开','最高','最低','昨收','交易日期']]
            cur.execute(del_usstockmarket,[tradeDate,windCode])
            cur.executemany(insert_eaststockmarket,dataDF.values.tolist())
            print("%s addNum:%s"%(windCode,cur.rowcount))
            con.commit()

    except:
        msg="%s 行情获取异常"%windCode
        logger.error(msg)
        logger.error(traceback.format_exc())
        # wx_send.send_wx_msg_operation(msg)
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass


EASTMONEY_QUOTE_FIELDS = {
    'f12': '代码',
    'f14': '名称',
    'f3': '涨跌幅',
    'f2': '最新价',
    'f15': '最高',
    'f16': '最低',
    'f17': '今开',
    'f4': '涨跌额',
    'f8': '换手率',
    'f10': '量比',
    'f9': '动态市盈率',
    'f5': '成交量',
    'f6': '成交额',
    'f18': '昨日收盘',
    'f20': '总市值',
    'f21': '流通市值',
    'f13': '市场编号',
    'f124': '更新时间戳',
    'f297': '最新交易日',
}
class MagicConfig:
    EXTRA_FIELDS = 'extra_fields'
    QUOTE_ID_MODE = 'quote_id_mode'


EASTMONEY_QUOTE_FIELDS_HK = {
    'f12': '代码',
    'f14': '名称',
    'f3': '涨跌幅',
    'f2': '最新价',
    'f4': '涨跌额',
    'f18': '昨日收盘',
    'f13': '市场编号',
    #'f124': '更新时间戳',
    #'f297': '最新交易日',
}
# 股票、债券榜单表头 高频行情，只需要最新涨跌幅
#f13,f12,f14,f2,f3,f4,f18
EASTMONEY_QUOTE_FIELDS_MK = {
    'f12': '代码',
    'f3': '涨跌幅',
    'f2': '最新价',
    'f4': '涨跌额',
    'f18': '昨日收盘',
    'f13': '市场编号',
    #'f124': '更新时间戳',
    #'f297': '最新交易日',
}
# 请求头
EASTMONEY_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-us;q=0.5,en-US;q=0.3,en;q=0.2',
    # 'Referer': 'http://quote.eastmoney.com/center/gridlist.html',
}
def get_realtime_quotes_by_fs(fs: str,
                              **kwargs) -> pd.DataFrame:
    """
    获取沪深市场最新行情总体情况

    Returns
    -------
    DataFrame
        沪深市场最新行情信息（涨跌幅、换手率等信息）

    """
    # print("kwargs:",kwargs)
    columns = {
        **EASTMONEY_QUOTE_FIELDS,
        **kwargs.get(MagicConfig.EXTRA_FIELDS, {})
    }
    if 'quote' in kwargs:
        quote_key = kwargs['quote']
        if quote_key in ['HK','AM']:
            columns = {
            **EASTMONEY_QUOTE_FIELDS_HK,
            **kwargs.get(MagicConfig.EXTRA_FIELDS, {})}
        else:
            columns = {
                **EASTMONEY_QUOTE_FIELDS_MK,
                **kwargs.get(MagicConfig.EXTRA_FIELDS, {})
            }
    fields = ",".join(columns.keys())
    params = (
        ('pn', '1'),
        ('pz', '1000000'),
        ('po', '1'),
        ('np', '1'),
        ('fltt', '2'),
        ('invt', '2'),
        ('fid', 'f3'),
        ('fs', fs),
        ('fields', fields)
    )
    url = 'http://push2.eastmoney.com/api/qt/clist/get'
    json_response = requests.get(url,
                                headers=EASTMONEY_REQUEST_HEADERS,
                                params=params).json()
    df = pd.DataFrame(json_response['data']['diff'])
    df = df.rename(columns=columns)
    # df: pd.DataFrame = df[list(columns.values())]
    # df['行情ID'] = df['市场编号'].astype(str)+'.'+df['代码'].astype(str)
    # df['市场类型'] = df['市场编号'].astype(str).apply(
    #     lambda x: MARKET_NUMBER_DICT.get(x))
    # df['更新时间'] = df['更新时间戳'].apply(lambda x: str(datetime.fromtimestamp(x)))
    # df['最新交易日'] = pd.to_datetime(df['最新交易日'], format='%Y%m%d').astype(str)
    # tmp = df['最新交易日']
    # del df['最新交易日']
    # df['最新交易日'] = tmp
    # del df['更新时间戳']
    return df


FS_DICT = {
    # 可转债
    'bond': 'b:MK0354',
    '可转债': 'b:MK0354',
    'stock': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048',
    # 沪深A股
    # 'stock': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',
    '沪深A股': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',
    '沪深京A股': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048',
    '北证A股': 'm:0 t:81 s:2048',
    '北A': 'm:0 t:81 s:2048',
    # 期货
    'futures': 'm:113,m:114,m:115,m:8,m:142',
    '期货': 'm:113,m:114,m:115,m:8,m:142',

    '上证A股': 'm:1 t:2,m:1 t:23',
    '沪A': 'm:1 t:2,m:1 t:23',

    '深证A股': 'm:0 t:6,m:0 t:80',
    '深A': 'm:0 t:6,m:0 t:80',

    # 沪深新股
    '新股': 'm:0 f:8,m:1 f:8',

    '创业板': 'm:0 t:80',
    '科创板': 'm:1 t:23',
    '沪股通': 'b:BK0707',
    '深股通': 'b:BK0804',
    '风险警示板': 'm:0 f:4,m:1 f:4',
    '两网及退市': 'm:0 s:3',

    # 板块
    '地域板块': 'm:90 t:1 f:!50',
    '行业板块': 'm:90 t:2 f:!50',
    '概念板块': 'm:90 t:3 f:!50',

    # 指数
    '上证系列指数': 'm:1 s:2',
    '深证系列指数': 'm:0 t:5',
    '沪深系列指数': 'm:1 s:2,m:0 t:5',
    # ETF 基金
    'ETF': 'b:MK0021,b:MK0022,b:MK0023,b:MK0024',
    # LOF 基金
    'LOF': 'b:MK0404,b:MK0405,b:MK0406,b:MK0407',

    '美股': 'm:105,m:106,m:107',
    '港股': 'm:128 t:3,m:128 t:4,m:128 t:1,m:128 t:2',
    '英股': 'm:155 t:1,m:155 t:2,m:155 t:3,m:156 t:1,m:156 t:2,m:156 t:5,m:156 t:6,m:156 t:7,m:156 t:8',
    '中概股': 'b:MK0201',
    '中国概念股': 'b:MK0201'


}
from typing import Dict, List, Union
def get_realtime_quotes(fs: Union[str, List[str]] = None,
                        **kwargs) -> pd.DataFrame:
    fs_list: List[str] = []
    if fs is None:
        fs_list.append(FS_DICT['stock'])

    if isinstance(fs, str):
        fs = [fs]

    if isinstance(fs, list):

        for f in fs:
            if not FS_DICT.get(f):
                raise KeyError(f'指定的行情参数 `{fs}` 不正确')
            fs_list.append(FS_DICT[f])
        # 给空列表时 试用沪深A股行情
        if not fs_list:
            fs_list.append(FS_DICT['stock'])
    fs_str = ','.join(fs_list)
    df = get_realtime_quotes_by_fs(fs_str, **kwargs)
    df.rename(columns={'代码': '股票代码',
                       '名称': '股票名称'
                       }, inplace=True)
    # print("df:",df.columns)
    # df.to_excel('d:/work/temp/df.xlsx')
    return df
#stockcode, stockname, s_dq_close, s_dq_change, s_dq_pctchange, s_dq_volume, s_dq_amount, s_dq_open, s_dq_high, s_dq_low, s_dq_preclose, tradedate
def first_hisdata(stocktype='stocks',beg="20170101"):
    lcon,lcur=dbutils.getConnect()
    # quscodedf = pd.read_sql("select windcode,stockcode,ric from newdata.a_reuters_basic arb"
    #                         " where exchangecountry ='Hong Kong' and windcode not in (select windcode from   spider.east_us_stockprice group by windcode  )",lcon)
    if stocktype in ['stocks']:
        quscodedf = pd.read_sql("select  windcode,area  from spider.usa_stockcode  where  windcode is not null ",lcon)
    elif stocktype in ['idx']:
        quscodedf=pd.read_sql("select * from config.stockinfo c WHERE stocktype='idx' and area='AM' " ,lcon)#AND  C.WINDCODE NOT IN (select windcode from spider.east_us_idxprice group by windcode)
    else:
        return
    # quscodedf = pd.read_sql("select  *  from spider.usa_stockcode  where windcode not in (select windcode from   spider.east_us_stockprice group by windcode  )",lcon)
    bt=datetime.datetime.now()


    allsize=len(quscodedf)
    print("allsize:",allsize)
    areaDict = {"105": 'O', "106": 'N', "107": 'A', "100": 'I', '116': 'HK', '153': 'PS'}
    rtareaDict = {v:k for k,v in areaDict.items()}
    for qc in quscodedf.itertuples():
        # stockcode=qc.stockcode
        # ric=qc.ric
        windcode = qc.windcode
        if stocktype in ['idx']:
            stockcode=qc.stockcode
            eastcode=qc.eastcode
            getStockHisMarket(windCode=windcode, stockCode=stockcode,
                              eastCode=eastcode, beg=beg, end="",stocktype=stocktype)
        else:
            stockCode = str(windcode).split(".")[0]
            area = qc.area
            if area in rtareaDict:
                secid = rtareaDict[area] + "." + stockCode
            else:
                print("xxxxxxxxxx", windcode)
                continue
            getStockHisMarket(windCode=windcode,stockCode=str(windcode).zfill(5),eastCode='116.%s'%(str(windcode).split(".")[0]).zfill(5), beg=beg, end="",stocktype=stocktype)
        # getStockHisMarket(windCode="NDX.GI",stockCode=stockCode,eastCode=secid, beg="20231213", end="")
        # time.sleep(random.randint(3,8))

        allsize-=1
        print("allsize:%s"%allsize)
    et = datetime.datetime.now()
    print(bt)
    print(et)
    pass
if __name__ == '__main__':
    # synusMarket()
    first_hisdata(stocktype="idx")

    # getStockHisMarket(windCode="NDX.GI", stockCode="NDX", eastCode="100.NDX", beg="20170101", end="")
    # getStockHisMarket(windCode="SPX.GI", stockCode="SPX", eastCode="100.SPX", beg="20170101", end="")