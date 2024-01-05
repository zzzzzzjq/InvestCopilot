import datetime
import json
import re

from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils
from InvestCopilot_App.models.stocks.stockUtils import stockUtils
import  InvestCopilot_App.models.market.marketUtils as marketUtils
from  InvestCopilot_App.models.market.snapMarket import snapUtils
from  InvestCopilot_App.models.comm.mongdbConfig import  mgdbdata_utils
import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
from django.http import JsonResponse

Logger = logger_utils.LoggerUtils()

@userLoginCheckDeco
def stockAPIHandler(request):
    rest = ResultData()
    try:
        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        if doMethod == "search":
            search=reqData.get("search")
            vName=reqData.get("vName")
            flagAll=reqData.get("flagAll")
            rest=stockUtils().stockSearch(search,flagAll,vName)
        elif doMethod == "getStockInfo":
            symbol=reqData.get("symbol")
            if symbol == "" or symbol is None:
                rest.errorData(errorMsg="Please enter a symbols.")
                return JsonResponse(rest.toDict())
            stockInfo_dt=cache_dict.getCacheStockInfo(symbol)
            if isinstance(stockInfo_dt,dict):
                rest.data=stockInfo_dt
            return JsonResponse(rest.toDict())
        elif doMethod == "getIndexInfo":
            idxInfo_dt=cache_dict.getIndxInfoDT()
            Stocktypes=set()
            for idxcode,idx_dt in idxInfo_dt.items():
                Stocktypes.add(idx_dt['Area'])
            Stocktype_dt={}
            for Stocktype in list(Stocktypes):
                stocktypeCodes=[]
                for idxcode, idx_dt in idxInfo_dt.items():
                    if idx_dt['Area'] in [Stocktype]:
                        stocktypeCodes.append(idx_dt)
                Stocktype_dt[Stocktype]=stocktypeCodes
            rest.data=Stocktype_dt
            return JsonResponse(rest.toDict())
        elif doMethod == "getCompanyReportingPeriod":
            preDays=reqData.get("preDays")
            nextDays=reqData.get("nextDays")
            if preDays == "" or preDays is None :
                preDays = -7
            else:
                preDays = int(preDays)
            if nextDays == "" or nextDays is None:
                nextDays = 14
            else:
                nextDays=int(nextDays)
            rest = mgdbdata_utils().getEarningsForecastData(preDays=preDays,nextDays=nextDays)
            return JsonResponse(rest.toDict())
        elif doMethod=="getSymbolNewMarket":
            #获取最新股价和涨跌幅
            try:
                rest = ResultData()
                rest.data=[]
                symbols = reqData.get("symbols")
                if symbols == "" or symbols is None:
                    rest.errorData(errorMsg="Please enter a symbols.")
                    return JsonResponse(rest.toDict())
                windCodes = re.split("[|,]", str(symbols))
                # 按时间及区域拆分。
                workTime = datetime.datetime.now().strftime("%H%M")
                if (workTime >= '0929' and workTime <= '1135') or (workTime > '1300' and workTime <= '1505'):
                    # 剔除美股
                    if not type(windCodes) is list:
                        windCodes = [windCodes]
                    windCodes = [x for x in windCodes
                                     if str(x)[-3:] in marketUtils.MARKET_LIST]
                elif (workTime > '1135' and workTime <= '1205') or (workTime > '1505' and workTime <= '1615'):
                    # 剔除美股
                    if not type(windCodes) is list:
                        windCodes = [windCodes]
                    windCodes = [x for x in windCodes
                                     if str(x)[-3:] in [".HK",'.HI']]
                elif (workTime >= '2130' and workTime <= '2400') or (workTime > '0000' and workTime <= '0415'):
                    # 只刷新美股
                    if not type(windCodes) is list:
                        windCodes = [windCodes]
                    windCodes = [x for x in windCodes
                                     if not (str(x)[-3:] in marketUtils.MARKET_LIST)]
                else:
                    return JsonResponse(rest.toDict())
                if len(windCodes)==0:
                    return JsonResponse(rest.toDict())
                emminhqDf = snapUtils().getRealStockMarketByWindCode(windCodes)
                if emminhqDf.empty:
                    return JsonResponse(rest.toDict())
                emminhqDf['NOWPRICE'] = emminhqDf['NOWPRICE'].apply(lambda x: tools_utils.formatDigit(x, mode='{:.2f}'))
                emminhqDf['PCTCHANGE'] = emminhqDf['PCTCHANGE'].apply(
                    lambda x: tools_utils.formatDigit(x, mode='{:.2f}'))
                rtdata = emminhqDf[["WINDCODE","NOWPRICE","PCTCHANGE"]].rename(columns=lambda x: x.capitalize()).to_json(orient='records')
                # chgMax = emminhqDf['PCTCHANGE'].astype(float).max()
                # chgMin = emminhqDf['PCTCHANGE'].astype(float).min()
                # if not pd.isnull(chgMax) and not pd.isnull(chgMin):
                #     resultData.chgCenter = max(float(chgMax), abs(float(chgMin)))
                # else:
                #     resultData.chgCenter = 10
                #
                # orderedDict = collections.OrderedDict()
                # eTwDict = cache_dict.swtEastToWind()
                # for idx, row in emminhqDf.iterrows():
                #     stockCodeT = row.STOCKCODE
                #     # 东财代码转万德代码： 00700.HK >0700.HK
                #     stockCodeT = eTwDict[stockCodeT]
                #     if stockCodeT in combWeightDict:
                #         _weightList = combWeightDict[stockCodeT]
                #     else:
                #         _weightList = [0, 0]
                #     orderedDict[stockCodeT] = [row.NOWPRICE, row.PCTCHANGE] + _weightList
                rest.data = json.loads(rtdata)
                return JsonResponse(rest.toDict())
            except Exception as ex:
                errorMsg = '获取最新股价和涨跌幅的接口出错！'
                Logger.errLineNo(msg=errorMsg)
                rest.errorData(errorMsg=errorMsg)
                return JsonResponse(rest.toDict())
        else:
            rest.errorData(errorMsg="doMethod参数错误")
            return rest.toDict()
        return JsonResponse(rest.toDict())
    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='抱歉，数据处理失败，请稍后重试。')
        # dh_user_utils.UserInfoUtil().saveUserLogNew(request, "error", state='0', content='doMethod:%s'%str(doMethod),logMode='copilot')
        return JsonResponse(rest.toDict())