# -*- coding: utf-8 -*-

import sys
from pymongo import UpdateOne
import requests
import math
import threading
from django.http import HttpResponseRedirect
import pandas as pd
sys.path.append("../..")

import InvestCopilot_App.models.cache.dict.dictCache as cache_dict

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.user.userMode import cuserMode
from  InvestCopilot_App.models.market.snapMarket import snapUtils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils

import logging
from InvestCopilot_App.models.toolsutils import dbmongo
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

class FinancialReportMode():
    def getYearFinancialReport(self,windCode):
        # 查询年度报告
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        msg=""
        rtdata={'stock_info':{},'YeardataCount':0,'showColumns':{},'YearFinancialReportdata':{}}
        try:
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #                 where table_schema='newdata' and table_name='reuters_financial_report';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            #  2. 查询改股票所有年度报告
            columns = ['windcode', 'stockcode', 'revenuemm', 'revenuegrowth', 'grossprofitmm', 'grossmargin', 'ebitdamm', 'ebitdamargin', 'netincomemm',
                        'netmargin', 'eps', 'epsgrowth', 'cashfromoperationsmm', 'capitalexpendituresmm', 'freecashflowmm','perioddate','tradedate']
            columns_query = ','.join(columns)
            columns_query +=",split_part(perioddate,'-',1) as year"
            query_Financial_Report = f"""select {columns_query} from newdata.reuters_financial_report  where windcode=%s and periodid='year' and position('-' in perioddate)>0 order by perioddate;"""
            q_windCode=windCode
            if windCode in ['GOOG.O']:
                q_windCode = "GOOGL.O"
            qDF =pd.read_sql(query_Financial_Report,con,params=[q_windCode])
            qDF= tools_utils.dfColumUpper(qDF)
            qDF.fillna('-',inplace=True)
            data_list=[]
            count_df = len(qDF)
            dict_info_more = dict()
            if count_df>=1 :
                dict_info_more={'windcode':qDF.iloc[0]['WINDCODE'],'stockcode':qDF.iloc[0]['STOCKCODE'],'latestedTradedate':qDF.iloc[0]['TRADEDATE']}
            columns = list(set(columns)-set(['windcode', 'stockcode']))
            columns = columns + ['year']
            dbcolumns = ['REVENUEMM', 'REVENUEGROWTH', 'GROSSPROFITMM', 'GROSSMARGIN', 'EBITDAMM', 'EBITDAMARGIN',
                         'NETINCOMEMM', 'NETMARGIN', 'EPS', 'EPSGROWTH', 'CASHFROMOPERATIONSMM',
                         'CAPITALEXPENDITURESMM', 'FREECASHFLOWMM']
            vnames = ["Revenue", "Revenue Growth (%)", "Gross Profit", "Gross Margin (%)", "EBITDA","EBITDA Margin (%)",
                      "Net Income", "Net Margin (%)", "EPS", "EPS Growth (%)","Cash from Operations",
                      "Capital Expenditures", "Free Cash Flow"]
            map_1={}
            for index in range(len(dbcolumns)):
                map_1[dbcolumns[index].lower()]=vnames[index]
            rtdata['showColumns']=map_1

            for row in qDF.itertuples():
                row_dict=dict()
                # year_row = str(row.YEAR)
                for column in columns:
                    str_temp = column.upper()
                    row_dict[column] = str(getattr(row,str_temp,'-'))
                    # row_dict['revenuemm'] = str(row.REVENUEMM)
                # dict_temp = {year_row:row_dict}
                data_list.append(row_dict)
            rtdata['YearFinancialReportdata'] = data_list
            rtdata['YeardataCount'] = count_df

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
            rtdata['stock_info'] =dict_info_more
            rst.errorMsg = msg
            rst.data  = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getYearFinancialReportError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getQuarterFinancialReport(self,windCode):
        # 查询季度报告
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        rtdata = {'stock_info':{},'quarterdataCount':0,'showColumns':{},'quarterFinancialReportdata':{}}
        msg = ""
        try:
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #                 where table_schema='newdata' and table_name='reuters_financial_report';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            #  2. 查询改股票所有季度报告
            columns = ['windcode', 'stockcode', 'revenuemm', 'revenuegrowth', 'grossprofitmm', 'grossmargin',
                       'ebitdamm', 'ebitdamargin', 'netincomemm',
                       'netmargin', 'eps', 'epsgrowth', 'cashfromoperationsmm', 'capitalexpendituresmm',
                       'freecashflowmm','perioddate','tradedate']
            columns_query = ','.join(columns)
            columns_query += ", split_part(perioddate,'-',1) as year,extract (quarter from to_date(perioddate,'YYYY-MM-DD')) as quarter "
            query_Financial_Report = f"""select {columns_query}
                               from newdata.reuters_financial_report  where windcode=%s and periodid='quarter' and position('-' in perioddate)>0  order by perioddate;"""
            q_windCode = windCode
            if windCode in ['GOOG.O']:
                q_windCode = "GOOGL.O"
            qDF = pd.read_sql(query_Financial_Report, con, params=[q_windCode])
            qDF = tools_utils.dfColumUpper(qDF)
            qDF.fillna('-', inplace=True)
            data_list = []
            count_df = len(qDF)
            dict_info_more = dict()
            if count_df >= 1:
                dict_info_more = {'windcode': qDF.iloc[0]['WINDCODE'], 'stockcode': qDF.iloc[0]['STOCKCODE'],
                                  'latestedTradedate': qDF.iloc[0]['TRADEDATE']}
            print(dict_info_more)
            columns = list(set(columns) - set(['windcode', 'stockcode']))
            columns = columns + ['year','quarter']
            for row in qDF.itertuples():
                row_dict=dict()
                for column in columns:
                    str_temp = column.upper()
                    row_dict[column] = str(getattr(row,str_temp,'-'))
                data_list.append(row_dict)
            rtdata['quarterFinancialReportdata'] = data_list
            rtdata['quarterdataCount'] = count_df
            dbcolumns = ['REVENUEMM', 'REVENUEGROWTH', 'GROSSPROFITMM', 'GROSSMARGIN', 'EBITDAMM', 'EBITDAMARGIN',
                         'NETINCOMEMM', 'NETMARGIN', 'EPS', 'EPSGROWTH', 'CASHFROMOPERATIONSMM',
                         'CAPITALEXPENDITURESMM', 'FREECASHFLOWMM']
            vnames = ["Revenue", "Revenue Growth (%)", "Gross Profit", "Gross Margin (%)", "EBITDA",
                      "EBITDA Margin (%)","Net Income", "Net Margin (%)", "EPS", "EPS Growth (%)", "Cash from Operations",
                      "Capital Expenditures", "Free Cash Flow"]
            map_1 = {}
            for index in range(len(dbcolumns)):
                map_1[dbcolumns[index].lower()] = vnames[index]
            rtdata['showColumns'] = map_1

            # 3. 查询该股票个股信息
            #  3.1 从缓存中获得股票信息
            stockInfo_dt = cache_dict.getStockInfoDT()
            if windCode in stockInfo_dt:
                stock_info_dict = {'stockname': stockInfo_dt[windCode]['Stockname'],
                                   'stocktype': stockInfo_dt[windCode]['Stocktype'],
                                   'area': stockInfo_dt[windCode]['Area']}
            else:
                # 3.2 负责从stockinfo中获取
                msg += "Cachedict does not have information about this stock! "
                query_stock_info = f"select stockname,stocktype,area from config.stockinfo where windcode ='{windCode}';"
                cur.execute(query_stock_info)
                stock_info = cur.fetchall()
                if (len(stock_info) == 0):
                    stock_info_dict = {'stockname': '', 'stocktype': '',
                                       'area': ''}
                    msg += " The configstockinfo table does not have information about this stock! "
                else:
                    stock_info_dict = {'stockname': stock_info[0][0], 'stocktype': stock_info[0][1],
                                       'area': stock_info[0][2]}
            dict_info_more.update(stock_info_dict)
            rtdata['stock_info'] = dict_info_more
            rst.errorMsg = msg
            rst.data =  rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getQuarterFinancialReportError", errorMsg=msg)
            return rst
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass

    def getQuarterandYearFinancialReport(self,windCode):
        # 查询季度报告
        con, cur = dbutils.getConnect()
        rst = ResultData()
        windCode = str(windCode)
        msg = ""
        rtdata = {'stock_info':{},'quarterdataCount':[],'yeardataCount':[],'showColumns':{},'quarterFinancialReportdata':[],'yearFinancialReportdata':[]}
        try:
            # 1. 若更改列名后.取消前5行注释. 注释columns两行
            # query_for_columns = """select column_name from information_schema.columns
            #                 where table_schema='newdata' and table_name='reuters_financial_report';"""
            # cur.execute(query_for_columns)
            # columns = cur.fetchall()
            # columns = [ii[0] for ii in columns]
            #  2. 查询改股票所有季度报告
            columns = ['windcode', 'stockcode', 'revenuemm', 'revenuegrowth', 'grossprofitmm', 'grossmargin',
                       'ebitdamm', 'ebitdamargin', 'netincomemm',
                       'netmargin', 'eps', 'epsgrowth', 'cashfromoperationsmm', 'capitalexpendituresmm',
                       'freecashflowmm','perioddate','tradedate']
            columns_query = ','.join(columns)
            query_Financial_Report = f"""select {columns_query}
                               from newdata.reuters_financial_report  where windcode=%s and periodid='quarter' and position('-' in perioddate)>0 order by perioddate;"""
            qDF = pd.read_sql(query_Financial_Report, con, params=[windCode])
            qDF = tools_utils.dfColumUpper(qDF)
            qDF.fillna('-', inplace=True)
            data_list = []
            count_df_1 = len(qDF)
            dict_info_more = dict()
            if count_df_1 >= 1:
                dict_info_more = {'windcode': qDF.iloc[0]['WINDCODE'], 'stockcode': qDF.iloc[0]['STOCKCODE']}
            columns = list(set(columns) - set(['windcode', 'stockcode']))
            for row in qDF.itertuples():
                row_dict = dict()
                for column in columns:
                    str_temp = column.upper()
                    row_dict[column] = str(getattr(row, str_temp, '-'))
                data_list.append(row_dict)
            rtdata['quarterFinancialReportdata'] = data_list
            rtdata['quarterdataCount'] = count_df_1

            dbcolumns = ['REVENUEMM', 'REVENUEGROWTH', 'GROSSPROFITMM', 'GROSSMARGIN', 'EBITDAMM', 'EBITDAMARGIN',
                         'NETINCOMEMM', 'NETMARGIN', 'EPS', 'EPSGROWTH', 'CASHFROMOPERATIONSMM',
                         'CAPITALEXPENDITURESMM', 'FREECASHFLOWMM']
            vnames = ["Revenue", "Revenue Growth (%)", "Gross Profit", "Gross Margin (%)", "EBITDA",
                      "EBITDA Margin (%)", "Net Income", "Net Margin (%)", "EPS", "EPS Growth (%)",
                      "Cash from Operations",
                      "Capital Expenditures", "Free Cash Flow"]
            map_1 = {}
            for index in range(len(dbcolumns)):
                map_1[dbcolumns[index].lower()] = vnames[index]
            rtdata['showColumns'] = map_1

            columns_query = ','.join(columns)
            columns_query += ",split_part(perioddate,'-',1) as year"
            query_Financial_Report = f"""select {columns_query}
                                    from newdata.reuters_financial_report  where windcode=%s and periodid='year' and position('-' in perioddate)>0 order by perioddate;"""
            qDF = pd.read_sql(query_Financial_Report, con, params=[windCode])
            qDF = tools_utils.dfColumUpper(qDF)
            qDF.fillna('-', inplace=True)
            columns =columns+['year']
            data_list = []
            count_df = len(qDF)
            for row in qDF.itertuples():
                row_dict = dict()
                for column in columns:
                    str_temp = column.upper()
                    row_dict[column] = str(getattr(row, str_temp, '-'))
                data_list.append(row_dict)
            rtdata['yearFinancialReportdata'] = data_list
            rtdata['yeardataCount'] = count_df

            # 3. 查询该股票个股信息
            #  3.1 从缓存中获得股票信息
            stockInfo_dt = cache_dict.getStockInfoDT()
            if windCode in stockInfo_dt:
                stock_info_dict = {'stockname': stockInfo_dt[windCode]['Stockname'],
                                   'stocktype': stockInfo_dt[windCode]['Stocktype'],
                                   'area': stockInfo_dt[windCode]['Area']}
            else:
                # 3.2 负责从stockinfo中获取
                msg += "Cachedict does not have information about this stock! "
                query_stock_info = f"select stockname,stocktype,area from config.stockinfo where windcode ='{windCode}';"
                cur.execute(query_stock_info)
                stock_info = cur.fetchall()
                if (len(stock_info) == 0):
                    stock_info_dict = {'stockname': '', 'stocktype': '',
                                       'area': ''}
                    msg += " The configstockinfo table does not have information about this stock! "
                else:
                    stock_info_dict = {'stockname': stock_info[0][0], 'stocktype': stock_info[0][1],
                                       'area': stock_info[0][2]}
            dict_info_more.update(stock_info_dict)
            rtdata['stock_info'] = dict_info_more
            rst.errorMsg = msg
            rst.data = rtdata
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getQuarterFinancialReportError", errorMsg=msg)
            return rst
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass


if __name__ == '__main__':
    # stockInfo_dt = cache_dict.getStockInfoDT()


    # print(stockInfo_dt)
    cu =FinancialReportMode()

    rst =  cu.getYearFinancialReport('AAPL.O')
    print(rst.toDict()['data'].keys())
    print(rst.toDict())
    rst = cu.getQuarterFinancialReport('AAPL.O')
    print(rst.toDict()['data'].keys())
    print(rst.toDict())
    rst = cu.getQuarterandYearFinancialReport('AAPL.O')
    print(rst.toDict()['data'].keys())
    print(rst.toDict())