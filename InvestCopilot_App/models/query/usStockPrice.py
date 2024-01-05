# -*- coding: utf-8 -*-

import sys
import time

import pandas as pd
sys.path.append("../..")

import InvestCopilot_App.models.cache.dict.dictCache as cache_dict

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.user.userMode import cuserMode
from  InvestCopilot_App.models.market.snapMarket import snapUtils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
import datetime
import logging

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

class usStockPriceMode():
    def getusStockPriceCol(self,windCode):
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        msg = ""
        rtdata = {'columns_count': [], 'columns_name': []}
        try:
            # query_for_columns = """select column_name from information_schema.columns
            #                 where table_schema='newdata' and table_name='reuters_us_stockprice';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            # print("','".join(columns))
            columns = ['windcode', 'stockcode', 'ric', 'stockname', 's_dq_close', 's_dq_change', 's_dq_pctchange', 's_dq_volume', 's_dq_amount', 's_dq_open', 's_dq_high', 's_dq_low', 's_dq_preclose', 'tradedate']

            columns = list(set(columns) - set(['windcode', 'stockcode', 'ric', 'stockname', 'tradedate']))
            len_col = len(columns)
            rtdata['columns_count']=len_col
            rtdata['columns_name']=columns
            rst.errorMsg = msg
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getusStockPriceColError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
    def getusStockPricewithTimeChoice(self,windCode,starttime='',col_name=''):
        # 如果不输入列名，或者列名均不存在于列名列表中，返回全部列
        # 's_dq_open|s_dq_close|s_dq_low|s_dq_high|s_dq_volume'
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        msg=""
        rtdata = {'stock_info':{},'columns_count': [],'columns_name':[], 'stockpricedata': []}
        try:
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #                 where table_schema='newdata' and table_name='reuters_us_stockprice';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            # print(columns)
            columns = ['windcode', 'stockcode', 'ric', 'stockname', 's_dq_close', 's_dq_change', 's_dq_pctchange', 's_dq_volume', 's_dq_amount', 's_dq_open', 's_dq_high', 's_dq_low', 's_dq_preclose', 'tradedate']
            if(col_name !='' and col_name !=None):
                columns_input = str(col_name).split('|')
                columns_input = list(set(columns).intersection(set(columns_input)))
                if(len(columns_input)!=0):
                    columns = ['windcode', 'stockcode','ric', 'stockname','tradedate']+columns_input
                    columns = list(set(columns))
            # columns = ['windcode', 'stockcode','ric', 'stockname', 's_dq_close', 's_dq_change', 's_dq_pctchange',
            #            's_dq_volume', 's_dq_amount', 's_dq_open', 's_dq_high', 's_dq_low', 's_dq_preclose', 'tradedate']
            columns_query = ','.join(columns)

            if starttime!="" and starttime !=None:
                starttime = str(starttime)
                try:
                    starttime = time.strptime(starttime,'%Y%m%d')
                    starttime = time.strftime("%Y%m%d",starttime)
                except:
                    msg = "Sorry, Start time format error, please re-enter."
                    Logger.errLineNo(msg=msg)
                    rst.errorData(translateCode="getusStockPricewithtimeError", errorMsg=msg)
                    return rst
                query_usStockPrice = f"""select {columns_query}
                                from newdata.reuters_us_stockprice where windcode=%s AND
                                TRADEDATE >= '{starttime}' order by tradedate;"""
            else:
                #  2. 查询改股票所有年度报告
                query_usStockPrice = f"""select {columns_query}
                            from newdata.reuters_us_stockprice where windcode=%s order by tradedate;"""
            qDF =pd.read_sql(query_usStockPrice,con,params=[windCode])
            qDF= tools_utils.dfColumUpper(qDF)
            qDF.fillna('-',inplace=True)
            # print(qDF)
            data_list = []
            count_df = len(qDF)
            dict_info_more = dict()
            if count_df >= 1:
                dict_info_more = {'windcode': qDF.iloc[0]['WINDCODE'], 'stockcode': qDF.iloc[0]['STOCKCODE'],
                                  'ric':qDF.iloc[0]['RIC'],'stockname_1':qDF.iloc[0]['STOCKNAME']}
            columns = list(set(columns) - set(['windcode', 'stockcode','ric','stockname','tradedate']))
            # for row in qDF.itertuples():
            #     row_dict = dict()
            #     tradedate_col = row['TRADEDATE']
            #     for column in columns:
            #         str_temp = column.upper()
            #         row_dict[column] = str(getattr(row, str_temp, '-'))
            #     data_list.append(row_dict)
            columns_name = 'TRADEDATE'
            # columns=['s_dq_open','s_dq_close','s_dq_low','s_dq_high','s_dq_volume']
            for column in columns:
                str_temp = column.upper()
                columns_name +=(","+str_temp)
            # print(columns_name)
            dataframe_q = qDF[list(columns_name.split(','))]
            # dataframe_q = qDF.loc[:,[columns]]
            rtdata['stockpricedata'] = dataframe_q.values.tolist()
            rtdata['columns_count'] = count_df
            rtdata['columns_name'] = ['tradedate']+columns
            # print(dataframe_q)
            # 3. 查询该股票个股信息
            #  3.1 从缓存中获得股票信息
            stockInfo_dt = cache_dict.getStockInfoDT()
            if windCode in stockInfo_dt:
                stock_info_dict = {'stockname': stockInfo_dt[windCode]['Stockname'], 'stocktype': stockInfo_dt[windCode]['Stocktype'],
                                   'area': stockInfo_dt[windCode]['Area']}
            else:
                # 3.2 负责从stockinfo中获取
                msg += "Cachedict does not have information about this stock! "
                query_stock_info = f"select stockname,stocktype,area from config.stockinfo where windcode ='{windCode}';"
                cur.execute(query_stock_info)
                stock_info = cur.fetchall()
                if(len(stock_info)==0):
                    stock_info_dict = {'stockname': '', 'stocktype': '',
                                       'area': ''}
                    msg+=" The configstockinfo table does not have information about this stock! "
                else:
                    stock_info_dict = {'stockname':stock_info[0][0],'stocktype':stock_info[0][1],'area':stock_info[0][2]}
            dict_info_more.update(stock_info_dict)
            rtdata['stock_info'] = dict_info_more
            rst.errorMsg = msg
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getusStockPriceError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getusStockPricewithTime(self,windCode,starttime=''):
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        msg=""
        rtdata = {'stock_info':{},'columns_count': [],'columns_name':[], 'stockpricedata': []}
        try:
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #                 where table_schema='newdata' and table_name='reuters_us_stockprice';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            columns = ['windcode', 'stockcode','ric', 'stockname', 's_dq_close', 's_dq_change', 's_dq_pctchange',
                       's_dq_volume', 's_dq_amount', 's_dq_open', 's_dq_high', 's_dq_low', 's_dq_preclose', 'tradedate']
            columns_query = ','.join(columns)

            if starttime!="" and starttime !=None:
                starttime = str(starttime)
                try:
                    starttime = time.strptime(starttime,'%Y%m%d')
                    starttime = time.strftime("%Y%m%d",starttime)
                except:
                    msg = "Sorry, Start time format error, please re-enter."
                    Logger.errLineNo(msg=msg)
                    rst.errorData(translateCode="getusStockPricewithtimeError", errorMsg=msg)
                    return rst
                query_usStockPrice = f"""select {columns_query}
                                from newdata.reuters_us_stockprice where windcode=%s AND
                                TRADEDATE >= '{starttime}' order by tradedate;"""
            else:
                starttime = datetime.datetime.now()-datetime.timedelta(days=5*365)
                starttime = starttime.strftime('%Y%m%d')
                #  2. 查询改股票所有年度报告
                query_usStockPrice = f"""select {columns_query}
                            from newdata.reuters_us_stockprice where windcode=%s AND
                                TRADEDATE >= '{starttime}' order by tradedate;"""
            qDF =pd.read_sql(query_usStockPrice,con,params=[windCode])
            qDF= tools_utils.dfColumUpper(qDF)
            qDF.fillna('-',inplace=True)
            # print(qDF)
            data_list = []
            count_df = len(qDF)
            dict_info_more = dict()
            if count_df >= 1:
                dict_info_more = {'windcode': qDF.iloc[0]['WINDCODE'], 'stockcode': qDF.iloc[0]['STOCKCODE'],
                                  'ric':qDF.iloc[0]['RIC'],'stockname_1':qDF.iloc[0]['STOCKNAME']}
            columns = list(set(columns) - set(['windcode', 'stockcode','ric','stockname','tradedate']))
            # for row in qDF.itertuples():
            #     row_dict = dict()
            #     tradedate_col = row['TRADEDATE']
            #     for column in columns:
            #         str_temp = column.upper()
            #         row_dict[column] = str(getattr(row, str_temp, '-'))
            #     data_list.append(row_dict)
            columns_name = 'TRADEDATE'
            columns=['s_dq_open','s_dq_close','s_dq_low','s_dq_high','s_dq_volume']
            for column in columns:
                str_temp = column.upper()
                columns_name +=(","+str_temp)
            # print(columns_name)
            dataframe_q = qDF[list(columns_name.split(','))]
            # dataframe_q = qDF.loc[:,[columns]]
            rtdata['stockpricedata'] = dataframe_q.values.tolist()
            rtdata['columns_count'] = count_df
            rtdata['columns_name'] = ['tradedate']+columns
            # print(dataframe_q)
            # 3. 查询该股票个股信息
            #  3.1 从缓存中获得股票信息
            stockInfo_dt = cache_dict.getStockInfoDT()
            if windCode in stockInfo_dt:
                stock_info_dict = {'stockname': stockInfo_dt[windCode]['Stockname'], 'stocktype': stockInfo_dt[windCode]['Stocktype'],
                                   'area': stockInfo_dt[windCode]['Area']}
            else:
                # 3.2 负责从stockinfo中获取
                msg += "Cachedict does not have information about this stock! "
                query_stock_info = f"select stockname,stocktype,area from config.stockinfo where windcode ='{windCode}';"
                cur.execute(query_stock_info)
                stock_info = cur.fetchall()
                if(len(stock_info)==0):
                    stock_info_dict = {'stockname': '', 'stocktype': '',
                                       'area': ''}
                    msg+=" The configstockinfo table does not have information about this stock! "
                else:
                    stock_info_dict = {'stockname':stock_info[0][0],'stocktype':stock_info[0][1],'area':stock_info[0][2]}
            dict_info_more.update(stock_info_dict)
            rtdata['stock_info'] = dict_info_more
            rst.errorMsg = msg
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getusStockPriceError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getUsStockPrice(self,windCode,starttime=''):
        #美股行情
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        try:
            if starttime=="" or starttime is None:
                starttime=(datetime.datetime.now()+datetime.timedelta(days=-365*5)).strftime("%Y%m%d")
            else:
                starttime=str(starttime).replace("-","").replace("/","")
            q="select tradedate,s_dq_open,s_dq_close,s_dq_low,s_dq_high,s_dq_volume from spider.east_us_stockprice where windcode=%s and tradedate>=%s order by tradedate "
            dfdata = pd.read_sql(q,con,params=[windCode,starttime])
            dfdata=dfdata.rename(columns=lambda x:str(x).lower())
            rtdata={"columns_count":len(dfdata),"columns_name":dfdata.columns.tolist(),"stockpricedata":dfdata.values.tolist()}
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getUsStockPriceError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass
    def gethkStockPricewithTime(self,windCode,starttime=''):
        #港股行情
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        try:
            if starttime=="" or starttime is None:
                starttime=(datetime.datetime.now()+datetime.timedelta(days=-365*5)).strftime("%Y%m%d")
            else:
                starttime=str(starttime).replace("-","").replace("/","")
            q="select tradedate,s_dq_open,s_dq_close,s_dq_low,s_dq_high,s_dq_volume from spider.east_hk_stockprice where windcode=%s and tradedate>=%s order by tradedate "
            dfdata = pd.read_sql(q,con,params=[windCode,starttime])
            dfdata=dfdata.rename(columns=lambda x:str(x).lower())
            rtdata={"columns_count":len(dfdata),"columns_name":dfdata.columns.tolist(),"stockpricedata":dfdata.values.tolist()}
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="gethkStockPricewithTimeError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getchStockPricewithTime(self,windCode,starttime=''):
        #A股行情
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        try:
            if starttime=="" or starttime is None:
                starttime=(datetime.datetime.now()+datetime.timedelta(days=-365*5)).strftime("%Y%m%d")
            else:
                starttime=str(starttime).replace("-","").replace("/","")
            q="select tradedate,s_dq_open,s_dq_close,s_dq_low,s_dq_high,s_dq_volume from spider.east_ch_stockprice where windcode=%s and tradedate>=%s order by tradedate "
            dfdata = pd.read_sql(q,con,params=[windCode,starttime])
            dfdata=dfdata.rename(columns=lambda x:str(x).lower())
            rtdata={"columns_count":len(dfdata),"columns_name":dfdata.columns.tolist(),"stockpricedata":dfdata.values.tolist()}
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getchStockPricewithTimeError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getinsiderwithTime(self,windCode, starttime='',page=1 ,pageSize=10):
        #港股行情
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        page = int(page)
        pageSize = int(pageSize)
        rtdata = {"page": page,"pageTotal":0,"pageSize": pageSize, "totalNum":0,"columns_count": 0, "columns_name": [],"data": []}
        try:
            if starttime=="" or starttime is None or len(starttime)<8:
                # 获取近5年的数据
                starttime=(datetime.datetime.now()+datetime.timedelta(days=-365*5)).strftime("%Y%m%d")
            else:
                starttime=str(starttime).replace("-","").replace("/","")
            totalcount = f"select count(1) from newdata.reuters_insider where windcode='{windCode}' and tradedate>=%s;"
            cur.execute(totalcount, [starttime])
            totalcount = cur.fetchone()[0]
            rtdata["totalNum"] = totalcount
            pagetatol_1 = (int)(totalcount / pageSize) if (totalcount % pageSize == 0) else (int) (totalcount / pageSize + 1)
            pagetatol_1 = max(1, pagetatol_1)
            if page > pagetatol_1: page = pagetatol_1
            if page < 1: page = 1
            rtdata["pageTotal"] = pagetatol_1
            rtdata["page"] = page
            start_offset = (page - 1) * pageSize
            q=f"""select insiderfullname Insider,insidertitle Relation,tradedate lastDate ,
                    insidertransactiontypeshort transaction ,InsiderSharesTradedAdjusted sharestraded,
                    insidertransactionprice price from newdata.reuters_insider where windcode=%s and tradedate>=%s 
                    order by tradedate desc  limit {pageSize} offset {start_offset};"""
            dfdata = pd.read_sql(q,con,params=[windCode,starttime])
            dfdata.fillna('', inplace=True)
            dfdata=dfdata.rename(columns=lambda x:str(x).lower())
            rtdata["columns_count"]=len(dfdata)
            rtdata["columns_name"]=dfdata.round(2).columns.tolist()
            rtdata["data"]=dfdata.values.tolist()
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getinsiderwithTimeError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def get_only_windcodes_by_portfolioid(self,user_id,portfolioId):
        windCodes=[]
        con, cur = dbutils.getConnect()
        userId = str(user_id)
        try:
            if portfolioId == "" or portfolioId is None:
                # 检查该用户是否有该指标组合
                q_count = "select distinct windCode from portfolio.user_portfolio_list where userid=%s order by windCode;"
                cur.execute(q_count, [userId])
            else :
                portfolioId = str(portfolioId)
                # 检查该用户是否有该指标组合
                q_count = "select windCode from portfolio.user_portfolio_list where userid=%s and portfolioid=%s order by seqno desc;"
                cur.execute(q_count, [userId, portfolioId])
            windCodes = cur.fetchall()
            if len(windCodes) > 0:
                windCodes = [x[0] for x in windCodes]
        except Exception as ex:
            Logger.errLineNo(traceback.format_exc())
        finally:
            cur.close()
            con.close()
        return windCodes

    def getinsiderCombinationwithTime(self,user_id,portfolioId, starttime='',page=1 ,pageSize=10):
        #港股行情
        con, cur = dbutils.getConnect()
        rst = ResultData()
        user_id = str(user_id)
        portfolioId = str(portfolioId)
        page = int(page)
        pageSize = int(pageSize)
        rsDF0 = pd.DataFrame()
        rtdata = {"page": page,"pageTotal":0, "pageSize": pageSize, "totalNum":0,"columns_count": 0, "columns_name": [],"data": []}
        try:
            if starttime=="" or starttime is None or len(starttime)<8:
                # 获取近5年的数据
                starttime=(datetime.datetime.now()+datetime.timedelta(days=-365*5)).strftime("%Y%m%d")
            else:
                starttime=str(starttime).replace("-","").replace("/","")
            stockcodes = self.get_only_windcodes_by_portfolioid(user_id, portfolioId=portfolioId)
            count_stockcode = len(stockcodes)
            if (count_stockcode == 1):
                totalcount = f"select count(1) from newdata.reuters_insider where windcode='{stockcodes[0]}' and tradedate>=%s;"
            else:
                totalcount = f"select count(1) from newdata.reuters_insider where windcode in {tuple(stockcodes)} and tradedate>=%s;"
            cur.execute(totalcount, [starttime])
            totalcount = cur.fetchone()[0]
            rtdata["totalNum"] = totalcount
            pagetatol_1 = (int)(totalcount / pageSize) if (totalcount % pageSize == 0) else (int) (totalcount / pageSize + 1)
            pagetatol_1 = max(1, pagetatol_1)
            if page > pagetatol_1: page = pagetatol_1
            if page < 1: page = 1
            rtdata["pageTotal"] = pagetatol_1
            rtdata["page"] = page
            start_offset = (page - 1) * pageSize

            if count_stockcode == 1:
                q = f"""select stockcode,insiderfullname Insider,insidertitle Relation,tradedate lastDate ,
                    insidertransactiontypeshort transaction ,InsiderSharesTradedAdjusted sharestraded,
                    insidertransactionprice price from newdata.reuters_insider where windcode='{stockcodes[0]}' and tradedate>=%s 
                    order by tradedate desc  limit {pageSize} offset {start_offset};"""
            else:
                q = f"""select stockcode,insiderfullname Insider,insidertitle Relation,tradedate lastDate ,
                        insidertransactiontypeshort transaction ,InsiderSharesTradedAdjusted sharestraded,
                        insidertransactionprice price from newdata.reuters_insider where windcode in {tuple(stockcodes)} and tradedate>=%s 
                        order by tradedate desc  limit {pageSize} offset {start_offset};"""
            dfdata = pd.read_sql(q,con,params=[starttime])
            dfdata.fillna('', inplace=True)
            dfdata=dfdata.rename(columns=lambda x:str(x).lower())
            rtdata["columns_count"]=len(dfdata)
            rtdata["columns_name"]=dfdata.round(2).columns.tolist()
            rtdata["data"]=dfdata.values.tolist()
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getinsiderCombinationwithTime",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getusStockPriceIndexPricewithTime(self, windCode, indexCode, starttime=''):
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        indexCode = str(indexCode)
        if indexCode =="41620":
            indexCode = "SPX.GI"
        elif indexCode == "40823":
            indexCode = "NDX.GI"
        msg=""
        rtdata = {"columns_count": 0, "columns_name":[], "data":[]}
        try:
            if starttime=="" or starttime is None or len(starttime)<8:
                # 获取近5年的数据
                starttime=(datetime.datetime.now()+datetime.timedelta(days=-365*5)).strftime("%Y%m%d")
            else:
                starttime=str(starttime).replace("-","").replace("/","")

            stockInfo = cache_dict.getCacheStockInfo(windCode)
            if isinstance(stockInfo, dict):
                codeArea = stockInfo['Area']
            else:
                codeArea = "AM"
            print(codeArea)
            totalcount = 0
            qDF = pd.DataFrame()
            if codeArea in ['HK']:
                totalcount = (f"""select count(*) from spider.east_hk_idxprice rui join spider.east_hk_stockprice eus 
                                on rui.tradedate=eus.tradedate  where rui.windcode=%s and eus.windcode=%s and eus.tradedate>=%s;""")
                cur.execute(totalcount, [indexCode, windCode, starttime])
                totalcount = cur.fetchone()[0]
                rtdata["totalNum"] = totalcount
                query_Price = f"""select rui.tradedate as tradedate, rui.s_dq_close as indexprice, eus.s_dq_close as stockprice, eus.s_dq_volume as volume from spider.east_hk_idxprice rui join
                                                    spider.east_hk_stockprice eus on rui.tradedate=eus.tradedate  where rui.windcode=%s and eus.windcode=%s and rui.tradedate>=%s 
                                                    order by tradedate desc; """
                qDF = pd.read_sql(query_Price, con, params=[indexCode, windCode, starttime])
            elif codeArea in ['CH']:
                totalcount = (f"""select count(*) from spider.east_ch_idxprice rui join spider.east_ch_stockprice eus 
                                                on rui.tradedate=eus.tradedate  where rui.windcode=%s and eus.windcode=%s and eus.tradedate>=%s;""")
                cur.execute(totalcount, [indexCode, windCode, starttime])
                totalcount = cur.fetchone()[0]
                rtdata["totalNum"] = totalcount
                query_Price = f"""select rui.tradedate as tradedate, rui.s_dq_close as indexprice, eus.s_dq_close as stockprice , eus.s_dq_volume as volume from spider.east_ch_idxprice rui join
                                                                    spider.east_ch_stockprice eus on rui.tradedate=eus.tradedate  where rui.windcode=%s and eus.windcode=%s and rui.tradedate>=%s 
                                                                    order by tradedate desc; """
                qDF = pd.read_sql(query_Price, con, params=[indexCode, windCode, starttime])
            elif codeArea in ['AM']:
                totalcount = (f"""select count(*) from spider.east_us_idxprice rui join spider.east_us_stockprice eus
                                                on rui.tradedate=eus.tradedate  where rui.windcode=%s and eus.windcode=%s and eus.tradedate>=%s;""" )
                cur.execute(totalcount, [indexCode,windCode,starttime])
                totalcount = cur.fetchone()[0]
                rtdata["totalNum"] = totalcount
                query_Price = f"""select rui.tradedate as tradedate, rui.s_dq_close as indexprice, eus.s_dq_close as stockprice , eus.s_dq_volume as volume from spider.east_us_idxprice rui join
                                        spider.east_us_stockprice eus on rui.tradedate=eus.tradedate  where rui.windcode=%s and eus.windcode=%s and rui.tradedate>=%s
                                        order by tradedate desc; """
                qDF =pd.read_sql(query_Price, con, params=[indexCode, windCode, starttime])
            qDF = qDF.rename(columns=lambda x:str(x).lower())
            qDF.fillna('-',inplace=True)
            count_df = len(qDF)
            rtdata['data'] = qDF.values.tolist()
            rtdata['columns_count'] = count_df
            rtdata['columns_name'] = qDF.columns.tolist()
            rst.errorMsg = msg
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getusStockPriceIndexPricewithTimeError", errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getusStockPrice(self,windCode):
        # 查询年度报告
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        msg=""
        rtdata = {'stock_info':{},'columns_name': [], 'stockpricedata': []}
        try:
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #                 where table_schema='newdata' and table_name='reuters_us_stockprice';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            columns = ['windcode', 'stockcode', 'stockname', 's_dq_close', 's_dq_change', 's_dq_pctchange', 's_dq_volume', 's_dq_amount', 's_dq_open', 's_dq_high', 's_dq_low', 's_dq_preclose', 'tradedate']
            #  2. 查询改股票所有年度报告
            columns_query = ','.join(columns)
            query_usStockPrice = f"""select {columns_query}
                        from newdata.reuters_us_stockprice where windcode=%s order by tradedate;"""
            qDF =pd.read_sql(query_usStockPrice,con,params=[windCode])
            qDF= tools_utils.dfColumUpper(qDF)
            qDF.fillna('-',inplace=True)
            rtdata['stockpricedata'] = qDF.values.tolist()
            rtdata['columns_name'] = columns
            # 3. 查询该股票个股信息
            #  3.1 从缓存中获得股票信息
            stockInfo_dt = cache_dict.getStockInfoDT()
            if windCode in stockInfo_dt:
                stock_info_dict = {'stockname': stockInfo_dt[windCode]['Stockname'], 'stocktype': stockInfo_dt[windCode]['Stocktype'],
                                   'area': stockInfo_dt[windCode]['Area']}
            else:
                # 3.2 负责从stockinfo中获取
                msg += "Cachedict does not have information about this stock! "
                query_stock_info = f"select stockname,stocktype,area from config.stockinfo where windcode ='{windCode}';"
                cur.execute(query_stock_info)
                stock_info = cur.fetchall()
                if(len(stock_info)==0):
                    stock_info_dict = {'stockname': '', 'stocktype': '',
                                       'area': ''}
                    msg+=" The configstockinfo table does not have information about this stock! "
                else:
                    stock_info_dict = {'stockname':stock_info[0][0],'stocktype':stock_info[0][1],'area':stock_info[0][2]}
            rtdata['stock_info'] = stock_info_dict
            rst.errorMsg = msg
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getusStockPriceError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def gettitlenavewithTime(self, titleId , starttime=''):
        con, cur = dbutils.getConnect()
        rst = ResultData()
        titleId = str(titleId)
        msg=""
        rtdata = {"columns_count": 0, "columns_name":[], "data":[]}
        try:
            if starttime=="" or starttime is None or len(starttime)<8:
                # 获取近5年的数据
                starttime=(datetime.datetime.now()+datetime.timedelta(days=-365*5)).strftime("%Y-%m-%d")
            else:
                starttime = str(starttime)
                starttime = time.strptime(starttime, '%Y-%m-%d')
                starttime = time.strftime("%Y-%m-%d", starttime)

            totalcount = (f"""select count(*) from  business.news_title_nav where title_id= %s and tradedate>= %s;""" )
            cur.execute(totalcount, [titleId, starttime])
            totalcount = cur.fetchone()[0]
            rtdata["totalNum"] = totalcount

            query_title = f"""select * from business.news_title_nav where title_id = %s and tradedate>= %s order by tradedate desc; """
            qDF =pd.read_sql(query_title, con, params = [titleId, starttime])
            qDF=qDF.rename(columns=lambda x:str(x).lower())
            qDF.fillna('-',inplace=True)
            count_df = len(qDF)
            rtdata['data'] = qDF.values.tolist()
            rtdata['columns_count'] = count_df
            rtdata['columns_name'] = qDF.columns.tolist()
            rst.errorMsg = msg
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="gettitlenavewithTimeError", errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass


if __name__ == '__main__':

    cu =usStockPriceMode()
    # rst=cu.getusStockPriceCol('AAPL.O')
    # rst = cu.getusStockPricewithTime('AAPL.O',20180101)
    # print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['stock_info'])
    # print(rst.toDict()['data']['columns_count'])
    # print(rst.toDict())
    print("           ")
    # rst = cu.gethkStockPricewithTime('0700.HK')
    rst = cu.getusStockPriceIndexPricewithTime(indexCode='41620', windCode="AAPL.O")
    # rst = cu.getusStockPriceIndexPricewithTime(indexCode='000300.SH',windCode="603300.SH")
    print(datetime.datetime.now()-datetime.timedelta(days=5*365))
    print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['stock_info'])
    # print(rst.toDict()['data']['columns_count'])
    print(rst.toDict())
    #
    # rst = cu.getusStockPricewithTimeChoice(windCode='AAPL.O')
    # rst = cu.getusStockPricewithTimeChoice(windCode='AAPL.O',col_name='s_dq_close|s_dq_change|s_dq_pctchange')
    # rst = cu.getusStockPricewithTimeChoice(windCode='AAPL.O',col_name='s_ddsfq_close|s_dq_cfsfhange|s_dqfs_pctchange')
    # print(rst.toDict())
    # rst = cu.getusStockPriceIndexPricewithTime('NVDA.O','40823','20230101')
    # rst = cu.getinsiderCombinationwithTime('73',portfolioId="self_102", page=4, pageSize=20)
    # print(datetime.datetime.now()-datetime.timedelta(days=5*365))
    # print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['columns_count'])
    # print(rst.toDict())
    # rst = cu.getinsiderCombinationwithTime('73',"self_103", page=1, pageSize=30)
    # print(datetime.datetime.now() - datetime.timedelta(days=5 * 365))
    # print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['columns_count'])
    # print(rst.toDict())
    # rst = cu.getinsiderCombinationwithTime('73',"self_104", page=1, pageSize=20)
    # print(datetime.datetime.now() - datetime.timedelta(days=5 * 365))
    # print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['columns_count'])
    # print(rst.toDict())
    # rst = cu.gettitlenavewithTime('ai_software', starttime='2023-01-01')
    # print(datetime.datetime.now()-datetime.timedelta(days=5*365))
    # print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['columns_count'])
    # print(rst.toDict())
    # rst = cu.gettitlenavewithTime('ai_software',)
    # print(datetime.datetime.now() - datetime.timedelta(days=5 * 365))
    # print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['columns_count'])
    # print(rst.toDict())
    # rst = cu.gettitlenavewithTime('bitcoin',"2023-12-11")
    # print(datetime.datetime.now() - datetime.timedelta(days=5 * 365))
    # print(rst.toDict()['data'].keys())
    # print(rst.toDict()['data']['columns_count'])
    # print(rst.toDict())
    stockInfo = cache_dict.getCacheStockInfo('HSI.HI')
    if isinstance(stockInfo, dict):
        codeArea = stockInfo['Area']
    else:
        codeArea = "AM"
    print(codeArea)

