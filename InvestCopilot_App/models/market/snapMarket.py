#行情快照
import os
import pickle
import pandas as pd
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'InvestCopilot.settings')

from InvestCopilot_App.models.market.snapRedis import cacheTools
from InvestCopilot_App.models.toolsutils import ToolsUtils as toolsUtils
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
# 缓存有效期24*10小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24 * 10

snap_redis = cacheTools(decode_responses=False)

class snapUtils:

    def getRealStockMarketDF(self,reload=False):
        """
        获取实时行情数据的缓存
        """
        qKey = "east_all_realMarketDF"
        df_bytes_from_redis = snap_redis.get(qKey)
        isLock = False
        if df_bytes_from_redis is None:
            # _lock=threading.RLock()
            # _lock.acquire()
            isLock = True
            reload = True
            df_bytes_from_redis = snap_redis.get(qKey)
            if df_bytes_from_redis is not None:
                reload = False
        if not reload:
            stockMarketDF = pickle.loads(df_bytes_from_redis)
            stockMarketDF=toolsUtils.dfColumUpper(stockMarketDF)

            # f2:最新价    f3:涨跌幅    f4:涨跌额  f12：股票代码  f14：股票名称 f18：昨收盘价，停牌股票
            if 'F3' in stockMarketDF.columns.values.tolist():
                stockMarketDF['EASTCODE'] = stockMarketDF['STOCKCODE']
                stockMarketDF['F3'] = stockMarketDF['F3'].astype(float) * 100
                stockMarketDF['F3'] = stockMarketDF['F3'].round(2)
                # 缓存中已经处理 etf 是3位小数，股票2位。
                stockMarketDF['F2'] = stockMarketDF['F2']  # .astype(float).round(2)
                stockMarketDF['F2'] = stockMarketDF['F2'].astype(str)
                stockMarketDF['F3'] = stockMarketDF['F3'].astype(str)
                # 昨收盘
                # stockMarketDF['F18'] = stockMarketDF['F18'].astype(float).round(2)
                stockMarketDF['F18'] = stockMarketDF['F18'].astype(str)
                swtEastToWind_dt=cache_dict.swtEastToWind(reload)
                stockMarketDF['WINDCODE']=stockMarketDF['STOCKCODE'].apply(lambda x :swtEastToWind_dt[x] if x in swtEastToWind_dt else x)

                stockMarketDF['INDEXCODE'] = stockMarketDF['WINDCODE']
                stockMarketDF = stockMarketDF.rename(
                    columns={'F2': 'NOWPRICE', 'F3': 'PCTCHANGE', 'F18': 'PRECLOSEPRICE', })
                stockMarketDF = stockMarketDF.set_index('INDEXCODE')
                stockMarketDF = stockMarketDF[
                    ['WINDCODE', 'EASTCODE','STOCKCODE', 'NOWPRICE', 'PRECLOSEPRICE', 'PCTCHANGE', 'STATUS']]
                #EASTCODE to windCode

            # todo 停牌情况处理
            # tp_list=stockMarketDF[stockMarketDF['NOWPRICE'].astype(float) == 0]
            # print(tp_list['STOCKWINDCODE'])
            # stockMarketDF.loc[stockMarketDF['NOWPRICE'].astype(float) == 0,'PCTCHANGE']='停牌'
            # print(stockMarketDF)
        else:
            stockMarketDF = pd.DataFrame([],columns=['WINDCODE', 'EASTCODE', 'STOCKCODE', 'NOWPRICE', 'PRECLOSEPRICE', 'PCTCHANGE', 'STATUS'])
            return stockMarketDF
            raise Exception("请更新缓存行情数据！")
            if True:
                try:
                    con, cur = dbutils.getConnect()
                    # 东财高频行情
                    # STOCKCODE STOCKWINDCODE NOWPRICE  PCTCHANGE
                    # STOCKCODE STOCKWINDCODE  NOWPRICE PCTCHANGE
                    # sqlStr = '''
                    # SELECT a.stockcode,
                    # a.stockcode as stockwindcode,
                    # a.stockcode as indexCode,
                    # to_char(a.nowprice, 'FM99990.00') AS nowprice,
                    # to_char(a.pctchange * 100, 'FM99990.00') AS pctchange
                    # FROM spider.emminhq2 a
                    # '''
                    q_max_date = "select max(tradedate) from spider.emminhq_time"
                    cur.execute(q_max_date)
                    maxData = cur.fetchone()
                    if len(maxData) > 0:
                        maxData = str(maxData[0])
                    # maxData='20200506'
                    # print("maxData:",maxData)
                    sqlStr = """
                     SELECT a.stockcode,a.stockcode as stockwindcode,a.stockcode as indexCode,to_char(a.nowprice, 'FM99990.00') AS nowprice,
                    to_char(a.pctchange * 100, 'FM99990.00') AS pctchange,status FROM spider.emminhq_time a
                     where tradetime =(select max(tradetime) from spider.emminhq_time where tradedate='%s' )
                     and a.tradedate='%s'
                    """ % (maxData, maxData)
                    stockMarketDF = pd.read_sql(sqlStr, con=con, index_col="INDEXCODE")
                    #
                    # # 股价和涨跌幅为零表示停牌，需要填充股价
                    # suspensionStockDF = stockMarketDF[
                    #     (stockMarketDF['NOWPRICE'] == '0.00') & (stockMarketDF['PCTCHANGE'] == '0.00')]
                    # suspensionStockDF = suspensionStockDF[
                    #     (suspensionStockDF['STOCKWINDCODE'].str.contains('SZ')) | suspensionStockDF[
                    #         'STOCKWINDCODE'].str.contains(
                    #         'SH')]
                    # if not suspensionStockDF.empty:
                    #     sStockList = suspensionStockDF['STOCKWINDCODE'].values.tolist()
                    #     sStockList = getQueryInParam(sStockList)
                    #     q_prc_mk = "select T.S_INFO_WINDCODE AS stockwindcode,T.S_DQ_CLOSE from newdata.ashareeodprices T WHERE T.S_INFO_WINDCODE IN (%s)" % (
                    #         sStockList)
                    #     preMkDF = pd.read_sql(q_prc_mk, con)
                    #     preMkDict = {}
                    #     for row in preMkDF.itertuples():
                    #         preMkDict[row.STOCKWINDCODE] = row.S_DQ_CLOSE
                    #     for idx, row in stockMarketDF.iterrows():
                    #         if row.STOCKWINDCODE in preMkDict:
                    #             row.NOWPRICE = preMkDict[row.STOCKWINDCODE]
                    # row.PCTCHANGE="停牌"
                    # def fillPrePrice(row):
                    #     if row.STOCKWINDCODE in preMkDict:
                    #         return [preMkDict[row.STOCKWINDCODE],'停牌']
                    #     else:
                    #         return [row.NOWPRICE,row.PCTCHANGE]
                    # stockMarketDF[['NOWPRICE','PCTCHANGE']] = stockMarketDF[['STOCKWINDCODE', 'NOWPRICE', 'PCTCHANGE']].apply(fillPrePrice, axis=1)
                    cur.close()
                    con.close()
                    snap_redis.set(qKey, pickle.dumps(stockMarketDF), CACHEUTILS_UPDATE_SECENDS)
                except:
                    raise
            if isLock:
                pass  # _lock.release()
        return stockMarketDF

    def getRealStockMarketByEastCode(self,eastCodeList):
        """
        获取A股+H股实时行情数据 ,取最后时间更新的行情
        stockCodeList: 股票代码要加后缀。600519.SH        """
        emminhqDF = self.getRealStockMarketDF()
        emminhqDF = emminhqDF.loc[emminhqDF['EASTCODE'].isin(eastCodeList)]
        return emminhqDF

    def getRealStockMarketByWindCode(self,eastCodeList):
        """
        获取A股+H股实时行情数据 ,取最后时间更新的行情
        stockCodeList: 股票代码要加后缀。600519.SH        """
        emminhqDF = self.getRealStockMarketDF()
        emminhqDF = emminhqDF.loc[emminhqDF['WINDCODE'].isin(eastCodeList)]
        return emminhqDF

if __name__ == '__main__':
    sn  = snapUtils()
    emminhqDF = sn.getRealStockMarketDF()
    # # # #
    stockCodeList = ['833454.BJ','COHR.N','JPRE','FRSH.O','DASH.O','LKNCY.OO','00700.HK','600519.SH']
    # stockCodeList = ['833454.BJ','COHR','JPRE','FRSH','DASH','LKNCY','00700.HK','600519.SH']
    # emminhqDF = emminhqDF.loc[emminhqDF['EASTCODE'].isin(stockCodeList)]
    emminhqDF = emminhqDF.loc[emminhqDF['WINDCODE'].isin(stockCodeList)]
    print(emminhqDF.columns.values.tolist())
    print(emminhqDF.values.tolist())
