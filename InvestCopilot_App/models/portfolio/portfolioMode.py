# -*- coding: utf-8 -*-

import json
import math
import time
import traceback
import datetime
import re
import socket
import os
import sys
import requests
import math
import threading
import pandas as pd
from pymongo import UpdateOne

from django.http import HttpResponseRedirect
sys.path.append("../..")

import InvestCopilot_App.models.cache.dict.dictCache as cache_dict

from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.user.userMode import cuserMode
from  InvestCopilot_App.models.market.snapMarket import snapUtils
from InvestCopilot_App.models.toolsutils import dbutils as dbutils
from  InvestCopilot_App.models.stocks.stockUtils import stockUtils

import logging
from InvestCopilot_App.models.toolsutils import dbmongo

import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
Logger = logger_utils.LoggerUtils()

class cportfolioMode():

    def addPortfolio(self,portfolioName,userId,portfolioType="self"):
        #创建组合
        userId=str(userId)
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            #检查是是否存在
            q_one = "select count(1) from portfolio.user_portfolio where userid=%s and portfolioname=%s"
            cur.execute(q_one,[userId,portfolioName])
            fcount = cur.fetchall()[0][0]
            if fcount>0:
                msg = "This name is already in use."
                rst.errorData(translateCode="PortfolioAdd",errorMsg=msg)
                return rst
            selectSql = "SELECT nextval('portfolio.seq_portfolioid') AS portfolioid"
            cur.execute(selectSql)
            portfolioId = cur.fetchall()[0][0]
            sportfolioId = portfolioType +"_"+ str(portfolioId)
            # 获取组合最大排序编号
            q_maxSeqNo = "select coalesce(max(seqno),1) from  portfolio.user_portfolio where userId=%s"
            cur.execute(q_maxSeqNo, [userId])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo) + 1
            insertSql = "INSERT INTO  portfolio.user_portfolio(userid,portfolioid,portfolioType,portfolioname,seqno,createtime) VALUES (%s,%s,%s,%s,%s,%s)"
            createDate = datetime.datetime.now() 
            cur.execute(insertSql, [userId, sportfolioId, portfolioType, portfolioName,ordNo, createDate])
            con.commit()
            data={'portfolioId':sportfolioId}
            rst.data=data
            return rst
        except:
            msg = "Sorry, Create Portfolio failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="PortfolioAddError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def delPortfolio(self,portfolioId,userId):
        # 组合删除
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            q_count = "select count(1) from  portfolio.user_portfolio  where userid=%s"
            cur.execute(q_count,[userId])
            fnum=cur.fetchone()[0]
            if fnum==1:
                msg = "Sorry, need to reserve 1 Portfolio."
                rst.errorData(translateCode="PortfolioDel",errorMsg=msg)
                return rst
            d_portfolio = "delete from  portfolio.user_portfolio  where userid=%s and portfolioid=%s"
            d_portfoliolist = "delete from  portfolio.user_portfolio_list  where userid=%s and portfolioid=%s"
            cur.execute(d_portfolio,[userId,portfolioId])
            cur.execute(d_portfoliolist,[userId,portfolioId])
            rowcount=cur.rowcount
            con.commit()
            data={'rowcount':rowcount}
            rst.data=data
            return rst
        except:
            msg = "Sorry, delete Portfolio failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="PortfolioDelError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def editPortfolio(self,portfolioId,portfolioName,userId):
        # 组合修改
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            q_count = "select * from  portfolio.user_portfolio  where userid=%s and portfolioId=%s "
            oneDF=pd.read_sql(q_count,con,params=[userId,portfolioId])
            oneDF=tools_utils.dfColumUpper(oneDF)
            if oneDF.empty:
                msg = "Sorry, PortfolioName not found."
                rst.errorData(translateCode="PortfolioEdit",errorMsg=msg)
                return rst
            #修改名字
            if str(oneDF.iloc[0].PORTFOLIONAME).strip()==portfolioName:
                return rst
            else:
                usql="update  portfolio.user_portfolio set  portfolioName=%s  where userid=%s and portfolioId=%s "
                cur.execute(usql,[portfolioName,userId,portfolioId])
            #修改中股票池 或排序
            # stockInfo_dt = cache_dict.getStockInfoDT()
            # portfolio_lists=[]
            # createDate=datetime.datetime.now()
            # for _ord,windCode in enumerate(windCodes):
            #     if windCode in stockInfo_dt:
            #         portfolio_lists.append([userId, portfolioId, windCode, _ord, createDate])
            #
            # insertSql = "INSERT INTO  portfolio.user_portfolio_list(userid,portfolioid,windCode,seqno,createtime) VALUES (%s,%s,%s,%s,%s)"
            # createDate = datetime.datetime.now()
            # d_his="delete from portfolio.user_portfolio_list where userid =%s and portfolioid=%s"
            # cur.execute(d_his, [userId, portfolioId])
            # cur.executemany(insertSql, portfolio_lists)
            rowcount=cur.rowcount
            con.commit()
            rst.data={"rowcount":rowcount}
            return rst
        except:
            msg = "Sorry, modify PortfolioName failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="PortfolioEditError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def portfolioStocksSort(self,portfolioId,windCodes,userId):
        # 组合股票排序
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            q_count = "select * from  portfolio.user_portfolio  where userid=%s and portfolioId=%s "
            oneDF=pd.read_sql(q_count,con,params=[userId,portfolioId])
            oneDF=tools_utils.dfColumUpper(oneDF)
            if oneDF.empty:
                msg = "Sorry, Portfolio not found."
                rst.errorData(translateCode="portfolioStocksSort",errorMsg=msg)
                return rst
            #修改中股票池 或排序
            stockInfo_dt = cache_dict.getStockInfoDT()
            portfolio_lists=[]
            windCodes.reverse()
            for _ord,windCode in enumerate(windCodes):
                if windCode in stockInfo_dt:
                    portfolio_lists.append([_ord, portfolioId, windCode])
            up_ord = "update  portfolio.user_portfolio_list set seqno=%s where portfolioid=%s and windCode =%s"
            cur.executemany(up_ord, portfolio_lists)
            rowcount=cur.rowcount
            con.commit()
            rst.data={"rowcount":rowcount}
            return rst
        except:
            msg = "Sorry, modify Portfolio Symbol order failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="portfolioStocksSortError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def createPortfolioAndAddoSymbol(self,portfolioName,windCodes,userId,portfolioType="self"):
        #创建组合
        userId=str(userId)
        windCodes = str(windCodes)
        portfolioName = str(portfolioName)
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            # 1.检查股票名是否存在
            q_one = "select count(1) from portfolio.user_portfolio where userid=%s and portfolioname=%s and portfoliotype=%s"
            cur.execute(q_one,[userId,portfolioName,portfolioType])
            fcount = cur.fetchall()[0][0]
            if fcount>0:
                msg = "This name is already in use."
                rst.errorData(translateCode="createPortfolioAndAddoSymbol",errorMsg=msg)
                return rst
            # 2.获取要插入的股票组合id
            selectSql = "SELECT nextval('portfolio.seq_portfolioid') AS portfolioid"
            cur.execute(selectSql)
            portfolioId = cur.fetchall()[0][0]
            sportfolioId = portfolioType +"_"+ str(portfolioId)
            portfolioId = sportfolioId
            # 3.获取该用户组合最大排序编号
            q_maxSeqNo = "select coalesce(max(seqno),1) from  portfolio.user_portfolio where userId=%s"
            cur.execute(q_maxSeqNo, [userId])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo) + 1
            insertSql = "INSERT INTO  portfolio.user_portfolio(userid,portfolioid,portfolioType,portfolioname,seqno,createtime) VALUES (%s,%s,%s,%s,%s,%s)"
            createDate = datetime.datetime.now()
            cur.execute(insertSql, [userId, sportfolioId, portfolioType, portfolioName,ordNo, createDate])
            con.commit()
            # 4.插入股票列表
            # 4.1 检查股票列表是否为空,为空返回
            portfolioIds = windCodes.split('|')
            windCodes = []
            msg = ''
            StockInfoDT = cache_dict.getStockInfoDT()
            for portfolid_i in portfolioIds:
                portstr = str(portfolid_i).strip()
                if len(portstr) == 0:
                    continue
                # 4.2 检查股票是否在缓存中
                if portstr not in StockInfoDT:
                    msg += f"{portstr} not found. "
                    continue
                windCodes.append(portstr)
            if len(windCodes) <= 0:
                msg = f"股票组合已创建,为{sportfolioId},但是股票数量必须大于零,请检查输入"
                rst.errorData(translateCode="createPortfolioAndAddoSymbol", errorMsg=msg)
                return rst
            for windCode in windCodes:
                # 检查该股票是否存在
                q_one = "select count(1) from  portfolio.user_portfolio_list where userid=%s and portfolioid=%s and windcode=%s"
                cur.execute(q_one, [userId, portfolioId, windCode])
                fcount = cur.fetchall()[0][0]
                if fcount > 0:
                    msg += f"{windCode} is exists."
                    continue
                # 获取组合最大排序编号
                q_maxSeqNo = "select coalesce(max(seqno),0) from  portfolio.user_portfolio_list where userid=%s and portfolioid=%s"
                cur.execute(q_maxSeqNo, [userId, portfolioId])
                ordNo = cur.fetchone()[0]
                ordNo = int(ordNo) + 1
                insertSql = "INSERT INTO  portfolio.user_portfolio_list(userid,portfolioid,windCode,seqno,createtime) VALUES (%s,%s,%s,%s,%s)"
                createDate = datetime.datetime.now()
                cur.execute(insertSql, [userId, portfolioId, windCode, ordNo, createDate])
                con.commit()
            data={'portfolioId':sportfolioId}
            rst.errorMsg=msg
            rst.data=data
            return rst
        except:
            msg = "Sorry, Create Portfolio failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="PortfolioAddError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass



    def addPortfolioSymbol(self,windCode,portfolioId,userId):
        # 组合添加股票代码
        con, cur = dbutils.getConnect()
        rst = ResultData()
        try:
            # 检查是是否存在
            q_one = "select count(1) from  portfolio.user_portfolio_list where userid=%s and portfolioid=%s and windcode=%s"
            cur.execute(q_one, [userId, portfolioId,windCode])
            fcount = cur.fetchall()[0][0]
            if fcount > 0:
                # msg = "windCode is exists."
                # rst.errorData(errorMsg=msg)
                return rst
            # 获取组合最大排序编号
            q_maxSeqNo = "select coalesce(max(seqno),0) from  portfolio.user_portfolio_list where userid=%s and portfolioid=%s"
            cur.execute(q_maxSeqNo,[userId, portfolioId])
            ordNo = cur.fetchone()[0]
            ordNo = int(ordNo) + 1
            insertSql = "INSERT INTO  portfolio.user_portfolio_list(userid,portfolioid,windCode,seqno,createtime) VALUES (%s,%s,%s,%s,%s)"
            createDate = datetime.datetime.now()
            cur.execute(insertSql, [userId, portfolioId, windCode, ordNo, createDate])
            con.commit()
            rst.errorMsg="组合添加股票成功"
            return rst
        except:
            msg = "Sorry, add symbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="addPortfolioSymbolError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def delPortfolioSymbol(self,windCode,portfolioId,userId):
        # 组合删除股票代码
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            q_one = "select count(1) from  portfolio.user_portfolio_list where userid=%s and portfolioid=%s and windcode=%s"
            cur.execute(q_one, [userId, portfolioId,windCode])
            fcount = cur.fetchall()[0][0]
            if fcount == 0:
                # msg = "windCode is exists."
                # rst.errorData(errorMsg=msg)
                return rst
            insertSql = "delete from portfolio.user_portfolio_list  where userid=%s and portfolioid=%s and windcode=%s"
            cur.execute(insertSql, [userId, portfolioId,windCode])
            rowcount=cur.rowcount
            con.commit()
            rtdata={'rowcount':rowcount}
            rst.data=rtdata
            return rst
        except:
            msg = "Sorry, delete symbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="delPortfolioSymbolError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass


    def getPortfolioInfo(self,portfolioId,userId,portfolioType='self'):
        #查询用户组合信息
        q_portfolio = "select portfolioId,portfolioname,averageyield from  portfolio.user_portfolio where userid=%s and portfolioId=%s and portfolioType=%s"
        qDF = dbutils.getPDQueryByParams(q_portfolio,params=[userId,portfolioId,portfolioType])
        qDF=tools_utils.dfColumUpper(qDF)
        return qDF

    def getPortfolios(self,userId,portfolioType="self"):
        #查询用户下的所有组合信息
        rst = ResultData()
        try:
            con, cur = dbutils.getConnect()
            q_portfolio = "select * from  portfolio.user_portfolio where userid=%s and portfolioType=%s order by seqno desc "
            qDF = pd.read_sql(q_portfolio,con,params=[userId,portfolioType])
            qDF=tools_utils.dfColumUpper(qDF)
            rst.portfolioDF=qDF
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getPortfoliosError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass


    def getPortfolioStocks(self,portfolioId,userId):
        #获取组合股票代码  组合代码修改
        rst = ResultData()
        rst.windCodes=[]
        try:
            con, cur = dbutils.getConnect()
            if portfolioId is None:
                q_one = "select distinct windCode  from  portfolio.user_portfolio_list where userid=%s order by windCode "
                cur.execute(q_one, [userId])
            else:
                q_one = "select windCode  from  portfolio.user_portfolio_list where userid=%s and portfolioid=%s order by seqno  desc  "
                cur.execute(q_one, [userId, portfolioId])
            windCodes = cur.fetchall()
            if len(windCodes)>0:
                windCodes=[x[0] for x in windCodes]
            rst.windCodes=windCodes
            return rst
        except:
            msg = "Sorry, get symbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getPortfolioStocksError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass


    def getPortfolioStocksInfo(self,portfolioId,userId):
        #获取组合股票代码 数据展示
        rst = ResultData()
        rst.portfolioStocksInfo=[]
        try:
            con, cur = dbutils.getConnect()
            q_one = "select windCode,seqno from  portfolio.user_portfolio_list where userid=%s and portfolioid=%s order by seqno  desc  "
            cur.execute(q_one, [userId, portfolioId])
            windCodes = cur.fetchall()
            if len(windCodes)>0:
                windCodes=[x for x in windCodes]
            rst.portfolioStocksInfo=windCodes
            return rst
        except:
            msg = "Sorry, get symbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="getPortfolioStocksInfoError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def countUserPortfoliosAverageYield(self,userid):
        #计算用户所有组合的平均收益率
        q_portfolio = "select portfolioid from  portfolio.user_portfolio where userid=%s"
        prDF=dbutils.getPDQueryByParams(q_portfolio,params=[str(userid)])
        if not prDF.empty:
            for portfolioId in prDF['PORTFOLIOID'].values.tolist():
                self.countPortfoliosAverageYield(portfolioId)

    def countAllPortfoliosAverageYield(self):
        #计算用户所有组合的平均收益率
        q_portfolio = "select portfolioid from  portfolio.user_portfolio"
        prDF=dbutils.getPDQuery(q_portfolio)
        if not prDF.empty:
            for portfolioId in prDF['PORTFOLIOID'].values.tolist():
                self.countPortfoliosAverageYield(portfolioId)

    def countPortfoliosAverageYield(self,portfolioId):
        """
        平均收益率 = (Σ股票收益率) / 股票数量
        例如，如果您有一个包含五支股票的股票池，每支股票在一个月内的收益率分别是10％、5％、-2％、8％和 12％，则计算平均收益率如下：
        平均收益率 = (10% + 5% - 2% + 8% + 12%) / 5 = 6.6%
        因此，该股票池的平均收益率为6.6%。
        :param portfolioId:
        :return:
        """
        con, cur = dbutils.getConnect()
        rst = ResultData()
        try:
            q_pl = "select WINDCODE from portfolio.user_portfolio_list where    portfolioid=%s"
            plDF =  pd.read_sql(q_pl,con,params=[portfolioId])
            plDF=tools_utils.dfColumUpper(plDF)
            selfwindCodes = plDF['WINDCODE'].tolist()
            if len(selfwindCodes)==0:
                return rst
            emminhqDF = snapUtils().getRealStockMarketByWindCode(selfwindCodes)
            s_pctchange=emminhqDF["PCTCHANGE"].astype(float).sum()
            AverageYield=round(s_pctchange/len(emminhqDF),2)
            u_avp="update  portfolio.user_portfolio  set AverageYield = %s,stocknum=%s where portfolioid=%s "
            cur.execute(u_avp,[AverageYield/100,len(selfwindCodes),portfolioId])
            con.commit()
            return rst
        except:
            msg = "Sorry, obtaining data failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(errorMsg=msg)
            rst.errorData(translateCode="countPortfoliosAvgYieldError",errorMsg=msg)
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

    def addComments(self,userId,sid,label=None,labelText=None,parentid=None):
        #添加评论
        try:
            rst=ResultData()
            con, cur = dbutils.getConnect()
            publishtime = datetime.datetime.now()
            qsql="select count(1) from portfolio.summarys_Comments where sid=%s and cuserid=%s"
            cur.execute(qsql, [str(sid),str(userId)])
            qnum = cur.fetchone()[0]
            if qnum>0:
                usql = "update portfolio.summarys_Comments set slable=%s, scomments=%s, publishtime=%s where sid=%s and cuserid=%s"
                cur.execute(usql, [label, labelText, publishtime, str(sid), userId])
                rowcount = cur.rowcount
                con.commit()
                data = {"rowcount": rowcount}
                rst.data = data
                return rst
            selectSql = "SELECT nextval('business.seq_comment') AS commentid"
            cur.execute(selectSql)
            commentid = str(cur.fetchall()[0][0])
            insertSql = "INSERT INTO  portfolio.summarys_Comments(rid,sid,cuserid,slable,scomments,publishtime,parentid)" \
                        " VALUES (%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(insertSql, [commentid,sid, userId, label, labelText,publishtime,parentid])
            rowcount=cur.rowcount
            con.commit()
            data={'commentid':commentid,"rowcount":rowcount}
            rst.data=data
            return rst
        except:
            msg = "Sorry, Create Comments failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(translateCode="addCommentsError",errorMsg=msg)
            return rst
        finally:
            try:cur.close()
            except:pass
            try:con.close()
            except:pass

    def getComments(self,sid,userId=None,parentid=None):
        #获取评论
        usql="select slable,scomments,publishtime,rid from portfolio.summarys_Comments  where sid=%s order by publishtime desc limit 1 "
        rtdf  = dbutils.getPDQueryByParams(usql,params=[sid],fillNaN="")
        return rtdf

    def getSummaryRelationStocks(self,summaryId):
        #获取组合股票代码
        con, cur = dbutils.getConnect()
        rst = ResultData()
        rst.portfolioStocksInfo=[]
        try:
            q_one = "select windCode,seqno from  portfolio.user_portfolio_list_summary where summaryId=%s order by seqno  desc  "
            cur.execute(q_one, [summaryId])
            windCodes = cur.fetchall()
            if len(windCodes)>0:
                windCodes=[x for x in windCodes]
            rst.portfolioStocksInfo=windCodes
            return rst
        except:
            msg = "Sorry, get symbols failed. Please try again later."
            Logger.errLineNo(msg=msg)
            rst.errorData(errorMsg=msg)
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

    def addPortfolioFromSummary(self,summaryId):
        #电话会议关联的公司代码
        con, cur = dbutils.getConnect()
        rst = ResultData()
        rst.tickers=[]
        try:
            #检查是是否存在
            #select * from  copilot_portfoliolist_summary;
            q_one = "select windcode from  portfolio.user_portfolio_list_summary where summaryId=%s"
            codeDF=pd.read_sql(q_one,con,params=[summaryId])
            windowsCode = codeDF["WINDCODE"].values.tolist()
            windowsCode = set(windowsCode)
            from login.news.InformationMode import informationMode
            #筛选匹配的股票代码
            rest = informationMode().getNewsContents_v1([summaryId])
            qdata = rest.data
            cpwindowdt={}
            if len(qdata) > 0:
                qdt = qdata[0]
                if "relationCompanies" in qdt:
                    cpwindowdt = {}
                    cpTickers = qdt['relationCompanies']
                    if "company" in cpTickers:
                        cpcompanys = cpTickers["company"]
                        for company in cpcompanys:
                            if "symbol" in company and "english_name" in company and "chinese_name" in company:
                                symbol = company['symbol']
                                if str(symbol).strip() == "":
                                    continue
                                english_name = company['english_name']
                                chinese_name = company['chinese_name']
                                symbolls = str(symbol).split(".")
                                if len(symbolls) == 1:
                                    ticker = symbolls[0]
                                    tickerInfo = informationMode().getWindCodeInfo(ticker)
                                    if isinstance(tickerInfo, dict):
                                        ticker = tickerInfo['windCode']
                                        codeName = ticker  #
                                        # codeName = tickerInfo['bbgName']
                                    else:
                                        continue
                                    unit = "US"
                                else:
                                    ticker = symbol
                                    codeName = ticker
                                    if str(ticker).endswith(".SS"):
                                        ticker = str(ticker).replace(".SS", '.SH')
                                    elif str(ticker).endswith(".HK"):
                                        ticker = str(ticker).replace(".HK", '').zfill(4) + ".HK"
                                    codeRst = newCache().getStockInfoByWindCodes(ticker)
                                    if codeRst.errorFlag:
                                        codes = codeRst.data
                                        if len(codes) >= 1:
                                            ticker = codes[0]['windCode']
                                            unit = codes[0]['unit']
                                            codeName = codes[0]['zhName']
                                        else:
                                            continue
                                    else:
                                        continue
                                cpwindowdt[symbol] = {'chinese_name': chinese_name, 'english_name': english_name,
                                                      'ticker': ticker, "tickerName": codeName, 'unit': unit}
            if len(cpwindowdt)==0:
                msg="未查找到匹配公司"
                rst.errorData(errorMsg=msg)
                return rst
            adddata=[]
            tickers=[]
            if len(cpwindowdt)>0:
                seqno=1
                for symbol,wdt in cpwindowdt.items():
                    ticker = wdt['ticker']
                    tickers.append(ticker)
                    adddata.append([summaryId,ticker,seqno])
                    seqno+=1
            tickers=set(tickers)
            if len(windowsCode)==0:
                #add
                i_sql="insert into  portfolio.user_portfolio_list_summary (summaryid,windcode,seqno,createtime) values(%s,%s,%s,current_timestamp )"
                cur.executemany(i_sql,adddata)
                con.commit()
            else:
                #update
                if set(windowsCode)-tickers==0:
                    rst.tickers=tickers
                else:
                    d_sql = "delete from   portfolio.user_portfolio_list_summary where summaryid=%s"
                    i_sql = "insert into  portfolio.user_portfolio_list_summary (summaryid,windcode,seqno,createtime) values(%s,%s,%s,current_timestamp )"
                    cur.execute(d_sql, [summaryId])
                    cur.executemany(i_sql, adddata)
                    con.commit()
            return rst
        except:
            msg = "抱歉，解析公司数据异常，请稍后重试"
            Logger.errLineNo(msg=msg)
            rst.errorData(errorMsg=msg)
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
    # cu =cportfolioMode()
    # tuserid="1732"
    # testchoice="createportfolioandsymbols"
    # if testchoice=="createportfolioandsymbols":
    #     cu.createPortfolioAndAddoSymbol("测试1203","QRHC.O|002147.SZ|VECO.O|601500.SH|NUE.N|LRMR.O","2000")

    # rst =cu.addPortfolio("测试组合5",tuserid,portfolioType="self")
    # print(rst.toDict())
    # portfolioId=rst.portfolioId
    #
    # s=cu.addPortfolioSymbol("AMD.O",portfolioId,tuserid)
    # print(s.toDict())
    # s=cu.addPortfolioSymbol("NVDA.O",portfolioId,tuserid)
    # print(s.toDict())

    # s=cu.delPortfolioSymbol("AMD.O","self_2","1732",)
    # print(s.toDict())
    # s=cu.delPortfolio("self_3","1732")
    # s=cu.getPortfolioStocks("self_22801","1732")
    # print(s.toDict())


    # s=cu.getPortfolioStocksInfo("self_22801","1732")
    # s=cu.addPortfolioFromSummary("Audio_8600416cd7fcf64da8c4e40b0132debf")
    # s=cu.getSummaryRelationStocks("Audio_8600416cd7fcf64da8c4e40b0132debf")
    # s=cu.countUserPortfoliosAverageYield("1732")

    # s=cu.countPortfoliosAverageYield("self_34")
    # s=cu.addComments('1968',"cnbc_1c62b42490857e10b8f37cbddd1bd070",'P','testxsdfssssdfdsfsdfxx')
    # s=cu.getComments('1967',"Audio_8600416cd7fcf64da8c4e40b0132debf")
    # print(s.toDict())
    # s=cu.delPortfolioSymbol("NVDA.O","self_22801","1732")
    # print(s.toDict())
    pass