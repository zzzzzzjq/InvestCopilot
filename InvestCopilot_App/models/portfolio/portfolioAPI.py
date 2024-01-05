# encoding: utf-8
import re
import collections
import threading

from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.portfolio import portfolioMode
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from  InvestCopilot_App.models.market.snapMarket import snapUtils
from  InvestCopilot_App.models.cache.dict import dictCache as cache_dict
Logger = logger_utils.LoggerUtils()


from django.http import JsonResponse
import time
import datetime
import pandas as pd
import math
import socket
import os
import traceback
import json
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dsid_mfactors.settings")
from django.core.cache import cache
# 缓存有效期24小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24

@userLoginCheckDeco
def portfolioAPIHandler(request):
    #组合管理
    rest = ResultData()
    try:
        crest=tools_utils.requestDataFmt(request,fefault=None)
        if not crest.errorFlag:
            return JsonResponse(crest.toDict())
        reqData = crest.data
        doMethod=reqData.get("doMethod")
        user_id = request.session.get("user_id")
        if doMethod=="createPortfolio":
            #添加组合
            portfolioName=reqData.get("portfolioName")
            user_id = request.session.get("user_id")
            if portfolioName=="" or portfolioName is None:
                rest.errorData(errorMsg="Please enter a portfolioName.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            portfolioName=str(portfolioName).strip()
            ckrts = tools_utils.charLength(portfolioName,20)
            if not ckrts.errorFlag:
                rest.errorData(errorMsg="portfolioName too long")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            rest = cm.addPortfolio(portfolioName,user_id)

        elif doMethod=="getPortfolios":
            #获得组合列表
            user_id = request.session.get("user_id")
            portfolioType=reqData.get("portfolioType")
            if portfolioType is None:
                portfolioType="self"
            if portfolioType=="" or portfolioType is None:
                rest.errorData(errorMsg="Please enter a portfolioType.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            threading.Thread(target=cm.countUserPortfoliosAverageYield,args=(user_id,)).start()
            # asyncio.run(cm.countUserPortfoliosAverageYield(user_id))
            qrest = cm.getPortfolios(user_id,portfolioType)
            if not qrest.errorFlag:
                return JsonResponse(qrest.toDict())
            portfolioDF=qrest.portfolioDF
            portfolioList=[]
            portfolioFirst= {}
            for r in portfolioDF.itertuples():
                portfolioId=str(r.PORTFOLIOID)
                portfolioName=str(r.PORTFOLIONAME)
                if pd.isnull(r.STOCKNUM):
                    stocknum=0
                else:
                    stocknum=int(r.STOCKNUM)
                if pd.isnull(r.AVERAGEYIELD):
                    AverageYield="N/A"
                else:
                    AverageYield=str(tools_utils.formatDigit(float(r.AVERAGEYIELD)*100))
                portfolioList.append({'portfolioId':portfolioId,"portfolioName":portfolioName,"averageYield":AverageYield,"stocknum":stocknum})
            if len(portfolioList)>0:
                portfolioFirst=portfolioList[0]
            rtdata={"portfolioList":portfolioList,"portfolioFirst":portfolioFirst,}
            rest.data=rtdata
        elif doMethod=="addPortfolioSymbol":
            #自选股添加股票
            symbols=reqData.get("symbols")
            portfolioId=reqData.get("portfolioId")
            user_id = request.session.get("user_id")
            if symbols=="" or symbols is None:
                rest.errorData(errorMsg="Please enter a symbols.")
                return JsonResponse(rest.toDict())
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolio.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            #ticker to windCode
            windCodes = re.split("[|,]", str(symbols))
            #windCode检查
            StockInfoDT = cache_dict.getStockInfoDT()
            for windCode in windCodes:
                if windCode not in StockInfoDT:
                    rest.errorData(errorMsg="not find symbol[%s]."%windCode)
                    return JsonResponse(rest.toDict())
                cm = portfolioMode.cportfolioMode()
                qrest = cm.addPortfolioSymbol(windCode,portfolioId,user_id)
                if not qrest.errorFlag:
                    return JsonResponse(qrest.toDict())
                cm.countPortfoliosAverageYield(portfolioId)
            return JsonResponse(rest.toDict())
        elif doMethod=="createPortfolioAndAddoSymbol":
            # 创建组合并为组合分配股票
            portfolioName=reqData.get("portfolioName")
            user_id = request.session.get("user_id")
            if portfolioName=="" or portfolioName is None:
                rest.errorData(errorMsg="Please enter a portfolioName.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            portfolioName=str(portfolioName).strip()
            ckrts = tools_utils.charLength(portfolioName,20)
            if not ckrts.errorFlag:
                rest.errorData(errorMsg="portfolioName too long")
                return JsonResponse(rest.toDict())
            # 自选股添加股票
            symbols = reqData.get("symbols")
            if symbols == "" or symbols is None:
                rest.errorData(errorMsg="Please enter a symbols.")
                return JsonResponse(rest.toDict())
            # windCode检查,在函数中
            cm = portfolioMode.cportfolioMode()
            qrest = cm.createPortfolioAndAddoSymbol(portfolioName=portfolioName,windCodes=symbols,userId=user_id)
            if qrest.errorFlag:
                rtdatas = qrest.data
                if hasattr(rtdatas,"portfolioId"):
                    cm.countPortfoliosAverageYield(rtdatas.portfolioId)
            return JsonResponse(qrest.toDict())
        elif doMethod == "createAudioPortfolio":
            audioId = reqData.get( "audioId")
            if audioId == "" or audioId is None:
                rest.errorData(errorMsg="Please enter a audioId.")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            rest = cm.addPortfolioFromSummary(audioId)

        elif doMethod=="delPortfolioSymbol":
            #删除股添加股票
            windCode=reqData.get("symbol")
            portfolioId=reqData.get("portfolioId")
            user_id = request.session.get("user_id")
            if windCode=="" or windCode is None:
                rest.errorData(errorMsg="Please enter a symbols.")
                return JsonResponse(rest.toDict())
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolio.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            qrest = cm.delPortfolioSymbol(windCode,portfolioId,user_id)
            if not qrest.errorFlag:
                return JsonResponse(qrest.toDict())
        elif doMethod=="delPortfolio":
            #删除组合
            portfolioId=reqData.get("portfolioId")
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolio.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            qrest = cm.delPortfolio(portfolioId,user_id)
            if not qrest.errorFlag:
                return JsonResponse(qrest.toDict())

        elif doMethod=="portfolioStocksSort":
            #组合股票排序
            portfolioId=reqData.get("portfolioId")
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolio.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            symbols=reqData.get("symbols")
            if symbols=="" or symbols is None:
                rest.errorData(errorMsg="Please enter a symbols.")
                return JsonResponse(rest.toDict())
            windCodes = re.split("[|,]", str(symbols))
            cm = portfolioMode.cportfolioMode()
            rest = cm.portfolioStocksSort(portfolioId,windCodes,user_id)
            return JsonResponse(rest.toDict())

        elif doMethod=="getPortfolioStocks":
            #获取股票列表
            portfolioId=reqData.get("portfolioId")
            # if portfolioId=="" or portfolioId is None:
            #     rest.errorData(errorMsg="Please enter a portfolio.")
            #     return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            qrest = cm.getPortfolioStocks(portfolioId,user_id)
            if not qrest.errorFlag:
                return JsonResponse(qrest.toDict())

            windCodes = qrest.windCodes
            StockInfoDT = cache_dict.getStockInfoDT()
            selfwindCodes=[]
            for windCode in windCodes:
                if windCode  in StockInfoDT:
                    sdt = StockInfoDT[windCode]
                    Stockname=sdt['Stockname']
                    selfwindCodes.append({"Windcode":windCode,"Stockname":Stockname})
            rtdata={"windCodes":windCodes,"stockList":selfwindCodes}
            rest.data=rtdata
            return JsonResponse(rest.toDict())
        elif doMethod=="getSummaryModifyState":
            #获取当前会议修改状态
            dataId=reqData.get("dataId")
            user_id = request.session.get("user_id")
            copilot_company_id = request.session.get("copilot_company_id")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="Please enter a report.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            seu=summaryEditMode()
            rest = seu.getEditSummarysStatusForUpdate(dataId,user_id,copilot_company_id)
            return JsonResponse(rest.toDict())
        elif doMethod=="releaseSummaryModifyState":
            #释放会议修改状态
            dataId=reqData.get("dataId")
            user_id = request.session.get("user_id")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="Please enter a report.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            seu=summaryEditMode()
            rest = seu.releaseSummarysStatusForUpdate(dataId,user_id)
            return JsonResponse(rest.toDict())
        #获取最新变修正后的摘要
        elif doMethod=="getSummaryModifyLastVersionData":
            #获取当前会议修改状态
            dataId=reqData.get("dataId")
            languages=reqData.get("languages")
            user_id = request.session.get("user_id")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="Please enter a report.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if languages=="" or languages is None:
                rest.errorData(errorMsg="Please enter a languages.")
                return JsonResponse(rest.toDict())
            seu=summaryEditMode()
            oneDF = seu.getSummaryModifyLastVersionData(dataId,languages)
            rest.data= {"count":0}
            if not oneDF.empty:
                rtdt=dict(oneDF.iloc[0])
                rtdt.pop("CUSERID")
                rtdt["count"]=1
                rest.data=rtdt
            return JsonResponse(rest.toDict())
        #修改后的会议摘要保存
        elif doMethod=="saveModifySummaryData":
            #获取当前会议修改状态
            dataId=reqData.get("dataId")
            newData=reqData.get("newData")
            languages=reqData.get("languages")
            user_id = request.session.get("user_id")
            if dataId=="" or dataId is None:
                rest.errorData(errorMsg="Please chooes a report.")
            if languages=="" or languages is None:
                rest.errorData(errorMsg="Please chooes a languages.")
                return JsonResponse(rest.toDict())
            if newData=="" or newData is None:
                rest.errorData(errorMsg="Please enter a report data.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            seu=summaryEditMode()
            newData=str(newData).strip()
            dataL = newData.encode('gbk', 'ignore')
            maxLength=500
            if len(dataL) < maxLength:
                rest.errorData(errorMsg='报告内容不能不能低于%d个汉字%s个字符' % (maxLength / 2, maxLength))
                return JsonResponse(rest.toDict())
            rest = tools_utils.charLength(newData, 20000, '报告内容')
            if not rest.errorFlag:
                return JsonResponse(rest.toDict())
            rest = seu.editSummaryDataSave(dataId,user_id,newData,languages)
            return JsonResponse(rest.toDict())
        elif doMethod=="editPortfolio":
            #修改组合
            portfolioId=reqData.get("portfolioId")
            portfolioName=reqData.get("portfolioName")
            user_id = request.session.get("user_id")
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolio.")
                return JsonResponse(rest.toDict())
            if portfolioName=="" or portfolioName is None:
                rest.errorData(errorMsg="Please enter a portfolioName.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            portfolioName=str(portfolioName).strip()
            ckrts = tools_utils.charLength(portfolioName,20)
            if not ckrts.errorFlag:
                rest.errorData(errorMsg="portfolioName too long")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            qrest = cm.editPortfolio(portfolioId,portfolioName,user_id)
            if not qrest.errorFlag:
                return JsonResponse(qrest.toDict())
        elif doMethod=="getPortfolioSymbols":
            #查询自选股列表
            portfolioId=reqData.get("portfolioId")
            templateId=reqData.get("vtemplateNo")
            user_id = request.session.get("user_id")
            if portfolioId=="" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolio.")
                return JsonResponse(rest.toDict())
            if user_id=="" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            cm = portfolioMode.cportfolioMode()
            if str(portfolioId).startswith("self_"):
                prts = cm.getPortfolioStocksInfo(portfolioId,user_id)
            else:
                prts=cm.getSummaryRelationStocks(portfolioId)
            sportfolioStocksInfo=prts.portfolioStocksInfo
            selfwindCodes=[]
            #wind代码转 east
            StockInfoDT = cache_dict.getStockInfoDT()
            for pcinfo in sportfolioStocksInfo:
                windCode = pcinfo[0]
                if windCode  in StockInfoDT:
                    sdt = StockInfoDT[windCode]
                    Stockname=sdt['Stockname']
                    Stockcode=sdt['Stockcode']
                    selfwindCodes.append({"WINDCODE":windCode,"Symbol":Stockcode,"Stockname":Stockname,"SEQNO":pcinfo[1]})
            if len(selfwindCodes)==0:
                rest.data = {"columns": ["Symbol","SockName"], "data": []}
                return JsonResponse(rest.toDict())

            #数据格式处理
            tDF = pd.DataFrame(selfwindCodes)
            tDF=tDF.fillna("")
            #行情
            windCodes=tDF['WINDCODE'].values.tolist()
            emminhqDF = snapUtils().getRealStockMarketByWindCode(windCodes)
            outDF=pd.merge(tDF,emminhqDF,on="WINDCODE",how='left')
            outDF=outDF.fillna("-")
            #排序
            outDF=outDF.sort_values(['SEQNO'],ascending=False)
            outDF=outDF.drop(['SEQNO',"WINDCODE","STOCKCODE",'EASTCODE',"PRECLOSEPRICE",'STATUS'],axis=1)
            outDF= outDF.rename(columns=lambda x: x.capitalize())
            #返回
            rest.data={"columns":outDF.columns.tolist(),"data":outDF.values.tolist()}
            return JsonResponse(rest.toDict())

            if templateId is None or templateId=="":
                # templateType = sys_enums.TemplateType.T_PORTFOLIO_V.value
                # templateNo = portfolio_mode.getDefaultTemplateByType(userid, templateType)
                templateId=tools_utils.globa_default_template_no
            stockInfoData, countValue = stock_utils.getDataTableColumns2(qstockList, templateNo=templateId,  portfolioType='0',removeSuffix=False)
            stockInfoData = stockInfoData.rename(columns={'股票代码': 'Symbol', '股票名称': 'SymbolName'})
            columnsList = stockInfoData.columns.values.tolist()
            # 添加股价,联查，取展示列
            stockCodeListFix = stockInfoData['Symbol'].values.tolist()
            emminhqDF = stock_utils.getRealStockMarket(stockCodeListFix)
            # chgMax = emminhqDF['PCTCHANGE'].astype(float).max()
            # chgMin = emminhqDF['PCTCHANGE'].astype(float).min()
            eTwDict = cache_dict.swtEastToWind()
            emminhqDF['STOCKCODE'] = emminhqDF['STOCKCODE'].apply(
                lambda x: eTwDict[x])  # 港股代码换行 5位转4位
            emminhqDF = emminhqDF.rename(columns={'STOCKCODE': 'Symbol', 'NOWPRICE': 'Price', 'PCTCHANGE': 'Change %'})
            stockInfoData = pd.merge(stockInfoData, emminhqDF, on="Symbol", how="left")
            stockInfoData = stockInfoData.fillna(value=stock_utils.DEFAULT_VALUE)
            stockInfoData = stockInfoData.replace('nan', value=stock_utils.DEFAULT_VALUE)
            columnsList = columnsList[0:2] + ['Price', 'Change %'] + columnsList[2:]
            stockInfoData = stockInfoData[columnsList]
            stockInfoData = pd.merge(stockInfoData, portfolioStockT, on='Symbol' , how="left")
            stockInfoData = stockInfoData.sort_values('SEQNO', ascending=False)
            stockInfoData = stockInfoData.fillna(value=stock_utils.DEFAULT_VALUE)
            stockInfoData["SymbolName"]=stockInfoData["SymbolName"].apply(lambda x: codeName_dt[x] if x in codeName_dt else x)
            rest.windCodeList = stockInfoData[['windCode','Symbol','SymbolName']].values.tolist()
            stockInfoData = stockInfoData.drop(['SEQNO','windCode'], axis=1)
            tableData=stockInfoData.values.tolist()
            tableColumns = stockInfoData.columns.tolist()
            tableColumns[1]="Symbol"
            rest.tableColumns = tableColumns
            rest.tableData = tableData
            # else:
            #     ndf = pd.DataFrame(ndata)
            #     tableData=ndf.values.tolist()
            #     rest.tableColumns = ndf.columns.tolist()
            #     rest.tableData = tableData

        elif doMethod == "addNewReport":
            symbol = reqData.get("windCode")
            title = reqData.get("researchTitle")
            editReportId = reqData.get("editReportId")
            content = reqData.get("researchSummary")

            companyId = request.session.get('copilot_company_id')
            userId = request.session.get("user_id")
            if title is None:
                rest = rest.errorData(errorMsg="主题不能为空")
                return JsonResponse(rest.toDict())
            if content is None:
                rest = rest.errorData(errorMsg="内容不能为空")
                return JsonResponse(rest.toDict())

            # 检查行业名称(100)、主题(200)、核心观点(2048)的字数是否超过最大限制
            if tools_utils.charLength(title, 2048).errorFlag is False:
                rest = rest.errorData(errorMsg='研究报告内容超过字数限制')
                return JsonResponse(rest.toDict())
            if tools_utils.charLength(title, 200).errorFlag is False:
                rest = rest.errorData(errorMsg='主题超过字数限制')
                return JsonResponse(rest.toDict())
            if tools_utils.isNull(editReportId):
                if symbol is None or symbol =="":
                    rest=rest.errorData(errorMsg="证券代码不能为空")
                    return JsonResponse(rest.toDict())
                windName = cache_dict.translateStockWindCode(symbol)
                symbolls = str(symbol).split(".")
                if len(symbolls) == 1:
                    # 美股
                    ticker = symbolls[0]
                    tickerInfo = informationMode().getWindCodeInfo(ticker)
                    if isinstance(tickerInfo, dict):
                        windCode = tickerInfo['windCode']
                    else:
                        rest.errorData(errorMsg="Symbol[%s] does not exist." % symbol)
                        return JsonResponse(rest.toDict())
                else:
                    ticker = symbol
                    codeRst = newCache().getStockInfoByWindCodes(ticker)
                    if not codeRst.errorFlag:
                        return JsonResponse(codeRst.toDict())
                    codes = codeRst.data
                    if len(codes) == 0:
                        rest.errorData(errorMsg="Symbol[%s] does not exist." % symbol)
                        return JsonResponse(rest.toDict())
                    else:
                        windCode = codes[0]['windCode']
                rest = informationMode().addNewReport(windCode,windName,title,userId,companyId, content)
            else:
                rest = informationMode().editNewReport(editReportId,title, userId, content)
        elif doMethod == "delInnerReport":
            rowId = reqData.get("rowId")
            if rowId is None or rowId =="":
                rest=rest.errorData(errorMsg="请选择报告主题")
                return JsonResponse(rest.toDict())
            userId = request.session.get("user_id")
            rest = informationMode().delInnerReport(userId,rowId)
        elif doMethod=="getMyResearch":
            user_id = request.session.get("user_id")
            summaryDatas = informationMode().getSummaryDataByPage("website", "company",windCode=None,titleTranslation="zh", userId=user_id,page=1, pageSize=500)
            #提交日期  证券公司  主题 评级 提交人 编辑 删除
            tableData=[]
            for sd in summaryDatas:
                sid =  sd['id']
                title =  sd['title']
                windCode =  sd['windCode']
                publishOn =  sd['publishOn'][0:10]
                tableData.append([sid,publishOn,windCode,title,])#,windCode,windCode
            rest.tableColumns = ["ID","提交日期",'证券公司','主题']#,'评级','目标价'
            rest.tableData = tableData

        elif doMethod=="getFinancialReport":
            windCode = reqData.get("windCode")
            cm = portfolioMode.cportfolioMode()
            financialReportDt = cm.getCompanyFinancialReport(windCode)
            if not financialReportDt.errorFlag:
                return JsonResponse(rest.toDict())
            rest.tableColumns = financialReportDt.tableColumns
            rest.tableData =  financialReportDt.tableData
        return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        #UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())
