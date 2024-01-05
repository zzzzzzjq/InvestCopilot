# encoding: utf-8

from InvestCopilot_App.models.user.UserInfoUtil import userLoginCheckDeco, userMenuPrivCheckDeco
from InvestCopilot_App.models.toolsutils.ResultData import ResultData
import InvestCopilot_App.models.toolsutils.LoggerUtils as logger_utils
from InvestCopilot_App.models.query import usStockPrice
import  InvestCopilot_App.models.toolsutils.ToolsUtils as tools_utils

Logger = logger_utils.LoggerUtils()


from django.http import JsonResponse

import InvestCopilot_App.models.cache.dict.dictCache as cache_dict
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dsid_mfactors.settings")
from django.core.cache import cache
# 缓存有效期24小时
CACHEUTILS_UPDATE_SECENDS = 60 * 60 * 24

@userLoginCheckDeco
def usStockPriceHandler(request):
    #组合管理
    rest = ResultData()
    try:
        rest=tools_utils.requestDataFmt(request,fefault=None)
        if not rest.errorFlag:
            return JsonResponse(rest.toDict())
        reqData = rest.data
        doMethod=reqData.get("doMethod")
        Logger.debug("usStockPriceHandler request:%s"%reqData)
        if doMethod=="getusStockPrice":
            # user_id = request.session.get("user_id")
            windcode=reqData.get("windCode")
            starttime = reqData.get("startTime")
            # if user_id=="" or user_id is None:
            #     rest.errorData(errorMsg="Please Login.")
            #     return JsonResponse(rest.toDict())
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            stockInfo=cache_dict.getCacheStockInfo(windcode)
            if isinstance(stockInfo,dict):
                codeArea=stockInfo['Area']
            else:
                codeArea="AM"

            if codeArea in ['HK']:
                rest = cm.gethkStockPricewithTime(windcode,starttime)
                if not rest.errorFlag:
                    return JsonResponse(rest.toDict())
                rest.data['stock_info']={'area':codeArea,"ric":windcode,"stockcode":stockInfo['Stockcode'],"stockname":stockInfo['Stockname'],
                                         "stockname_1":stockInfo['Stockname'],"stocktype":stockInfo['Stocktype'],"windcode":windcode}
            elif codeArea in ['CH']:
                rest = cm.getchStockPricewithTime(windcode,starttime)
                if not rest.errorFlag:
                    return JsonResponse(rest.toDict())
                rest.data['stock_info']={'area':codeArea,"ric":windcode,"stockcode":stockInfo['Stockcode'],"stockname":stockInfo['Stockname'],
                                         "stockname_1":stockInfo['Stockname'],"stocktype":stockInfo['Stocktype'],"windcode":windcode}
            elif codeArea in ['AM']:
                rest = cm.getUsStockPrice(windcode,starttime)
                if not rest.errorFlag:
                    return JsonResponse(rest.toDict())
                rest.data['stock_info']={'area':codeArea,"ric":windcode,"stockcode":stockInfo['Stockcode'],"stockname":stockInfo['Stockname'],
                                         "stockname_1":stockInfo['Stockname'],"stocktype":stockInfo['Stocktype'],"windcode":windcode}
            else:
                rest = cm.getusStockPricewithTime(windcode,starttime)
            return JsonResponse(rest.toDict())
        elif doMethod == "getinsiderCombinationwithTime":
            starttime = reqData.get("startTime")
            pageSize = reqData.get("pageSize")
            if pageSize == "" or pageSize is None:
                pageSize = 10
            else:
                pageSize = int(pageSize)
            page = reqData.get("page")
            if page == "" or page is None:
                page = 1
            else:
                page = int(page)
            user_id = request.session.get("user_id")
            portfolioId = reqData.get("portfolioId")
            if user_id == "" or user_id is None:
                rest.errorData(errorMsg="Please Login.")
                return JsonResponse(rest.toDict())
            if portfolioId == "" or portfolioId is None:
                rest.errorData(errorMsg="Please enter a portfolioId.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            rest = cm.getinsiderCombinationwithTime(user_id=user_id, portfolioId=portfolioId, starttime=starttime, pageSize=pageSize, page=page)
            return JsonResponse(rest.toDict())
        elif doMethod=="getInsiderData":
            windcode=reqData.get("windCode")
            starttime = reqData.get("startTime")
            pageSize = reqData.get("pageSize")
            if pageSize == "" or pageSize is None:
                pageSize = 10
            else:
                pageSize = int(pageSize)
            page = reqData.get("page")
            if page == "" or page is None:
                page = 1
            else:
                page = int(page)
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            rest = cm.getinsiderwithTime(windCode=windcode,starttime=starttime,pageSize=pageSize,page=page)
            return JsonResponse(rest.toDict())
        elif doMethod=="getusStockPriceIndexPricewithTime":
            windcode=reqData.get("windCode")
            starttime = reqData.get("startTime")
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            INDEXCODE = reqData.get("indexCode")
            if INDEXCODE == "" or INDEXCODE is None:
                rest.errorData(errorMsg="Please enter a INDEXCODE.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            rest = cm.getusStockPriceIndexPricewithTime(indexCode=INDEXCODE, windCode=windcode, starttime=starttime)
            return JsonResponse(rest.toDict())
        elif doMethod=="gettitlenavewithTime":
            titleId =reqData.get("titleId")
            starttime = reqData.get("startTime")
            if titleId =="" or titleId is None:
                rest.errorData(errorMsg="Please enter a titleId.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            rest = cm.gettitlenavewithTime(titleId=titleId, starttime=starttime)
            return JsonResponse(rest.toDict())
        elif doMethod=="getusStockPriceChoice":
            # user_id = request.session.get("user_id")
            windcode=reqData.get("windCode")
            starttime = reqData.get("startTime")
            # if user_id=="" or user_id is None:
            #     rest.errorData(errorMsg="Please Login.")
            #     return JsonResponse(rest.toDict())
            col_name = reqData.get("col_name")
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            rest = cm.getusStockPricewithTimeChoice(windCode=windcode,starttime=starttime,col_name=col_name)
            return JsonResponse(rest.toDict())
        elif doMethod=="getusStockPriceCol":
            # user_id = request.session.get("user_id")
            windcode=reqData.get("windCode")
            # if user_id=="" or user_id is None:
            #     rest.errorData(errorMsg="Please Login.")
            #     return JsonResponse(rest.toDict())
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            rest = cm.getusStockPriceCol(windCode=windcode)
            return JsonResponse(rest.toDict())
        elif doMethod=="getusStockPrice_2":
            # user_id = request.session.get("user_id")
            windcode=reqData.get("windCode")
            # if user_id=="" or user_id is None:
            #     rest.errorData(errorMsg="Please Login.")
            #     return JsonResponse(rest.toDict())
            if windcode=="" or windcode is None:
                rest.errorData(errorMsg="Please enter a windcode.")
                return JsonResponse(rest.toDict())
            cm = usStockPrice.usStockPriceMode()
            rest = cm.getusStockPrice(windcode)
            return JsonResponse(rest.toDict())

    except Exception as ex:
        Logger.errLineNo()
        rest.errorData(errorMsg='Sorry, obtaining data failed. Please try again later.')
        return JsonResponse(rest.toDict())
